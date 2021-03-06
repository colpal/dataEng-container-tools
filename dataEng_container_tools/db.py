import datetime
import logging
import os
from operator import itemgetter
from google.cloud import datastore
import json
import traceback
from dataEng_container_tools.exceptions import InvalidJobConfig


def get_secrets(path_):
    """
    get secrets from vault mounted json file
    :param path_: path to credentials file
    :return:dict_
    """
    default_key = ['key']
    default_key.sort()
    try:
        if not os.path.exists(path_):
            raise logging.exception("No credential file "
                                    "found in path {}".format(path_))

        with open(path_) as fp:
            dict_ = json.load(fp)
        obj = {
            "key": json.dumps(dict_)
        }
        gcs_keys = list(obj.keys())
        gcs_keys.sort()
        if gcs_keys != default_key:
            raise logging.exception("Needed credentials keys: {}"
                                    " but found keys: {}".format(default_key,
                                                                 gcs_keys))
        return obj

    except json.JSONDecodeError:
        raise logging.exception("Invalid json format"
                                " for credential")
    except Exception as e:
        raise e


class Db:
    """
    This class handle all the datastore operation
    """

    def __init__(self, task_kind):
        self.current_task_kind = task_kind

    def get_data_store_client(self, PATH):
        """
        this function creates and return datastore client
        :return: datastore client
        """
        try:
            cred = get_secrets(PATH)
            key = cred["key"]
        except KeyError as ke:
            raise logging.exception("Storage credentials not"
                                    " mounted for gcs ")
        gcs_sa = json.loads(key)
        with open('gcs-sa.json', 'w') as json_file:
            json.dump(gcs_sa, json_file)
        return datastore.Client.from_service_account_json('gcs-sa.json')

    def get_task_entry(self, client, filter_map, kind, order_task_entries_params=None):
        """
        get_task_entry is used to query the entry for task
        :param kind: kind to query on
        :param filter_map: filter map (dictionary)
        :param client: data store client
        :param order_task_entries_params: json object containing two keys
                                           'order_by_key_list'-list of parameters to order the task entries
                                           'descending_order'- True/False
        :return: list of the entry
        """
        try:
            query = client.query(kind=kind)
            for key in filter_map.keys():
                query.add_filter(key, '=', filter_map[key])
            entries = list(query.fetch())
            if len(entries) > 1 and order_task_entries_params is not None:
                for key_ in order_task_entries_params['order_by_key_list']:
                    # Check if the keys provided in order_by_key_list for ordering of entries
                    # are present as a key in the entry
                    for entry in entries:
                        if key_ not in entry.keys():
                            ex = InvalidJobConfig("No element for key: {} present in fetched entry: {}. Hence entries "
                                                  "cannot be ordered by key: {}".format(key_, entry, key_))
                            raise ex
                entries = sorted(entries, key=itemgetter(*order_task_entries_params['order_by_key_list']),
                                 reverse=order_task_entries_params['descending_order'])
            return entries
        except Exception as ex_:
            logging.exception("Exception while getting task entry")
            raise ex_

    def put_snapshot_task_entry(self, client, task_entry, params):
        """
        put_snapshot_task_entry is used to store the entry for the task
        :param client: datastore client
        :param task_entry: Entity which store actual instance of data
        :param params: dictionary containing all the parameters(key-value pairs) to be stored
        :return:
        """
        for key in params.keys():
            task_entry[key] = params[key]

        task_entry['modified_at'] = datetime.datetime.utcnow()
        logging.info(task_entry)
        client.put(task_entry)

    def handle_task(self, client, params, order_task_entries_params=None):
        """
        it's used to check if the task instance for the given param is available or not.
        If task instance is already present then it will update the existing instance else
        create a new instance and store it to given Entity.
        :param client: datastore client
        :param params: dictionary containing all the parameters(key-value pairs) to be stored
        :param order_task_entries_params: parameters to order the task entries if required
        :return:
        """

        commit_id = ''
        if 'GITHUB_SHA' in os.environ:
            commit_id = os.environ['GITHUB_SHA']

        filter_entries = {
            "dag_id": params['dag_id'],
            "run_id": params['run_id'],
            'airflow_task_id': params['airflow_task_id']
        }
        existing_entries = self.get_task_entry(client, filter_entries, self.current_task_kind,
                                               order_task_entries_params)

        if len(existing_entries) > 0:
            task_entry = existing_entries[0]
        else:
            task_key = client.key(self.current_task_kind)
            task_entry = datastore.Entity(key=task_key, exclude_from_indexes=('exception_details',))
            task_entry['commit_id'] = commit_id
            task_entry['created_at'] = datetime.datetime.utcnow()
        self.put_snapshot_task_entry(client, task_entry, params)
