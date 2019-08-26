from abc import ABC, abstractmethod
from .dataEngine import DataEngine

class Query(ABC):
	"""
	Abstract class. Base class for the different query building classes like SelectQuery
	"""
	def __init__(self):
		pass

	@abstractmethod
	def build_query(self):
		pass

	@abstractmethod
	def execute_query(self):
		pass


class QueryBuilder():
	"""
	This class is directly used by DLATK classes for building queries.

	Parameters
	------------
	data_engine: str
		Name of the data engine eg. mysql, sqlite
	"""

	def __init__(self, data_engine):
		self.data_engine = data_engine

	def create_select_query(self,from_table):
		"""
		Parameters
		------------
		from_table: str
			Name of the table from which records are to be fetched.
		
		Returns
		------------
		Object of SelectQuery class	
		"""
		return SelectQuery(from_table, self.data_engine)


class SelectQuery(Query):
	"""
	Class for building a SELECT query.

	Parameters
	------------
	from_table: str
		Name of the table from which records are to be fetched.
	data_engine: str
		Name of the database engine being used.
	"""

	def __init__(self, from_table, data_engine):
		super().__init__()
		self.sql = None
		self.fields = None
		self.group_by_fields = None
		self.from_table = from_table
		self.data_engine = data_engine

	def set_fields(self, fields):
		"""
		Parameters
		------------
		fields: list
			List containing name of the columns
	
		Returns	
		------------
		SelectQuery object		
		"""
		self.fields = fields
		return self

	def group_by(self, group_by_fields):
		"""
		Parameters
		------------
		group_by_fields: list
			List containing name of the fields which should be used for grouping.
	
		Returns	
		------------
		SelectQuery object		
		"""
		self.group_by_fields = group_by_fields
		return self

	def execute_query(self):
		"""
		Executes sql query
	
		Returns
		------------
		List of lists
		"""
		self.sql = self.build_query()
		return self.data_engine.execute_get_list(self.sql)

	def build_query(self):
		"""
		Builds a sql query based on the type of db
	
		Returns
		------------
		str: a built select query
		"""
		if self.data_engine.db_type == "mysql":
			return """SELECT %s FROM %s GROUP BY %s""" %(','.join(self.fields), self.from_table, ','.join(self.group_by_fields))

		if self.data_engine.db_type == "sqlite":
			pass
