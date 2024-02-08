"""
this file contains all exceptions
to use decorator annotate function in
following way
ex - @handle_exception
	def some_function():
"""

# from collections import Iterable
from functools import wraps


class Container_Tools_Error(Exception):
    def __init__(self,
                 message=('An error occurred in the dataEng container tools package. ' +
                          'There should probably be a better error message for this. ' +
                          'See above for a stacktrace to see what exactly went wrong.')):
        self.message = message
        super().__init__(self.message)


class Bad_JSON(Container_Tools_Error):
    def __init__(self, filepath):
        self.message = f'No secret can be read from {filepath} because it is not a properly formatted JSON.'
        super().__init__(errors=self.errors, message=self.message)


class No_Secrets_Found(Container_Tools_Error):
    def __init__(self, filepath):
        self.message = f'No secrets found in {filepath}.'
        super().__init__(errors=self.errors, message=self.message)


class Secret_Not_Found(Container_Tools_Error):
    def __init__(self, filepath):
        self.message = f''
        super().__init__(errors=self.errors, message=self.message)


class Argument_Mismatch(Container_Tools_Error):
    def __init__(self, given_arg, available_args):
        self.message = (f'The given parameter {given_arg} does not match any of the available ' +
                        f'parameters provided during init: {available_args}')
        super().__init__(errors=self.errors, message=self.message)


class GCs_Secret_Rejected(Container_Tools_Error):
    def __init__(self, secret, project):
        self.message = f'Rejected'
        super().__init__(errors=self.errors, message=self.message)


class GCS_Secret_Mismatch(Container_Tools_Error):
    def __init__(self, secret, URI):
        self.message = f'Rejected'
        super().__init__(errors=self.errors, message=self.message)


class ReportSourceNotReachable(Container_Tools_Error):
    """
	Site might be temporarily unreachable, mark pod
	as failure and retry
	"""

    def __init__(self, message='Report Source Not Reachable'):
        self.message = message
        super().__init__(message=self.message)


class ReportSourceInvalidCredentials(Container_Tools_Error):
    """
	Send mail, assuming all alternative credentials
	already been used, mark as success
	"""

    def __init__(self, message='Report Source Invalid Credentials'):
        self.message = message
        super().__init__(message=self.message)


class ReportSourceWebsiteStructureChange(Container_Tools_Error):
    """
	Mark pod as  success ,reports won't be downloaded due
	to structural change in web site , hence no need
	to retry
	"""

    def __init__(self, message='Report Source Website Structure Changed'):
        self.message = message
        super().__init__(message=self.message)


class ReportNotPresentInSource(Container_Tools_Error):
    """
	Report needs to be manually configured,
	will proceed with other reports, mark  pod
	as success
	"""

    def __init__(self, message='Report Not Present In Source'):
        self.message = message
        super().__init__(message=self.message)


class ReportNotReady(Container_Tools_Error):
    """
	Wait for report to be ready ,
	hence mark pod as failure for retry
	"""

    def __init__(self, message='Report Not Ready'):
        self.message = message
        super().__init__(message=self.message)


class ReportGenerationException(Container_Tools_Error):
    """
	Report needs to be retriggered,
	will proceed with other reports,
	mark it as success no need to retry
	"""

    def __init__(self, message='Report Generation Exception'):
        self.message = message
        super().__init__(message=self.message)


class InvalidJobConfig(Container_Tools_Error):
    """
	job can't run because of invalid configuration
	"""

    def __init__(self, message='Invalid Job Config'):
        self.message = message
        super().__init__(message=self.message)


class StorageNotReachable(Container_Tools_Error):
    """
	Might be down temporarily, retry,
	hence mark pod as failure
	"""

    def __init__(self, message='Storage Not Reachable'):
        self.message = message
        super().__init__(message=self.message)


class StorageFileNotFound(Container_Tools_Error):
    """
	we will proceed with other reports
	hence mark it as success
	"""

    def __init__(self, message='Storage File Not Found'):
        self.message = message
        super().__init__(message=self.message)


class ReportInvalidSchema(Container_Tools_Error):
    """
	Won't be able to create parquet,
	hence should fail
	"""

    def __init__(self, message='Report Invalid Schema'):
        self.message = message
        super().__init__(message=self.message)


class DataCountMismatch(Container_Tools_Error):
    """
	Upload file, send mail for debugging,
	row count won't change with retry
	hence mark it as success
	"""

    def __init__(self, message='Data Count Mismatch'):
        self.message = message
        super().__init__(message=self.message)


class StorageInvalidCredential(Container_Tools_Error):
    """
	since files won't be in storage, no need
	to retry mark it as success
	"""

    def __init__(self, message='Storage Invalid Credential'):
        self.message = message
        super().__init__(message=self.message)


class StorageError(Container_Tools_Error):
    """
	generic exception for gcs errors, no need
	to retry mark it as success
	"""

    def __init__(self, message='Storage Error'):
        self.message = message
        super().__init__(message=self.message)


class InvalidDagParameters(Container_Tools_Error):
    """
	Dag parameters not provided as expected
	"""

    def __init__(self, message='Invalid Dag Parameters'):
        self.message = message
        super().__init__(message=self.message)


class BQInvalidCredential(Container_Tools_Error):
    """
    since can't write to bq, no need
    to retry mark it as success
    """
    def __init__(self, message='Invalid BQ Credential'):
        self.message = message
        super().__init__(message=self.message)


class BQNoSuchTable(Container_Tools_Error):
    """
    No such table exists in big query
    """
    def __init__(self, message='NO table found'):
        self.message = message
        super().__init__(message=self.message)


class BQInvalidSchema(Container_Tools_Error):
    """
    throw exception if data to be load into BQ table
    has different schema
    """
    def __init__(self, message='Invalid BQ Schema'):
        self.message = message
        super().__init__(message=self.message)


class StorageCredentialNotFound(Container_Tools_Error):
    """
    Storage credentials not found for gcs
    """
    def __init__(self, message='storage credential not found'):
        self.message = message
        super().__init__(message=self.message)


exception_dict = {
    ReportSourceNotReachable.__name__: "failure",
    ReportSourceInvalidCredentials.__name__: "success",
    ReportSourceWebsiteStructureChange.__name__: "success",
    ReportNotPresentInSource.__name__: "success",
    ReportNotReady.__name__: "failure",
    ReportGenerationException.__name__: "success",
    StorageNotReachable.__name__: "failure",
    StorageFileNotFound.__name__: "success",
    ReportInvalidSchema.__name__: "success",
    DataCountMismatch.__name__: "success",
    StorageInvalidCredential.__name__: "success",
    InvalidJobConfig.__name__: "failure",
    StorageError.__name__: "success",
    InvalidDagParameters.__name__: "failure",
    BQInvalidCredential.__name__: "success",
    BQNoSuchTable.__name__: "success",
    BQInvalidSchema.__name__: "success",
    StorageCredentialNotFound.__name__: "success",
    Exception.__name__: "failure"
}


def handle_exception(original_function):
    """
	handle exceptions is a decorator which takes
	care of exception flow of decorated function
	:param original_function: decorated function
	:return: None
	"""

    @wraps(original_function)
    def wrapper_function(*args, **kwargs):
        """
		this functions wrap the original functions
		:param args: *args passed in original_function
		:param kwargs: **kwargs passed in original_function
		:return: None
		"""

        try:
            val = original_function(*args, **kwargs)
            return val

        except Exception as e:
            exception_class = e.__class__.__name__
            if exception_class not in exception_dict.keys():
                exception_class = Exception.__name__
            action = exception_dict[exception_class]
            if action == "failure":
                raise e

    return wrapper_function
