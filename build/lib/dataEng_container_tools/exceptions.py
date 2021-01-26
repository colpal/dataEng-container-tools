from collections import Iterable

class Container_Tools_Error(Exception):
	def __init__(self,
				message = ('An error occurred in the dataEng container tools package. ' +
					  	   'There should probably be a better error message for this. ' +
						   'See above for a stacktrace to see what exactly went wrong.')):
		self.message = message
		super().__init__(self.message)

class Bad_JSON(Container_Tools_Error):
	def __init__(self, filepath):
		self.message = f'No secret can be read from {filepath} because it is not a properly formatted JSON.'
		super().__init__(errors = self.errors, message = self.message)

class No_Secrets_Found(Container_Tools_Error):
	def __init__(self, filepath):
		self.message = f'No secrets found in {filepath}.'
		super().__init__(errors = self.errors, message = self.message)

class Secret_Not_Found(Container_Tools_Error):
	def __init__(self, filepath):
		self.message = f''
		super().__init__(errors = self.errors, message = self.message)

class Argument_Mismatch(Container_Tools_Error):
	def __init__(self, given_arg, available_args):
		self.message = (f'The given parameter {given_arg} does not match any of the available ' +
						f'parameters provided during init: {available_args}')
		super().__init__(errors = self.errors, message = self.message)

class GCs_Secret_Rejected(Container_Tools_Error):
	def __init__(self, secret, project):
		self.message = f'Rejected'
		super().__init__(errors = self.errors, message = self.message)

class GCS_Secret_Mismatch(Container_Tools_Error):
	def __init__(self, secret, URI):
		self.message = f'Rejected'
		super().__init__(errors = self.errors, message = self.message)