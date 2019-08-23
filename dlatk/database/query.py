from abc import ABC, abstractmethod
from .dataEngine import DataEngine
#from .. import dlaConstants as dlac

class Query(ABC):
	def __init__(self):
		pass
		#self.sqlType = dlac.DB_TYPE

	@abstractmethod
	def build_query(self):
		pass

	@abstractmethod
	def execute_query(self):
		pass


class QueryBuilder():

#should have the dataEngine name
	def __init__(self, data_engine):
		self.data_engine = data_engine

	def create_select_query(self,from_table):
		return SelectQuery(from_table, self.data_engine)


class SelectQuery(Query):

	def __init__(self, from_table, data_engine):
		super().__init__()
		self.sql = None
		self.fields = None
		self.group_by_fields = None
		self.from_table = from_table
		self.data_engine = data_engine

	def set_fields(self, fields):
		self.fields = fields
		return self

	def group_by(self, group_by_fields):
		self.group_by_fields = group_by_fields
		return self

	def execute_query(self):
		self.sql = self.build_query()
		print("********************************************")
		print(self.__dict__)
		print("********************************************")
		return self.data_engine.execute_get_list(self.sql)

	def build_query(self):
		if self.data_engine.db_type == "mysql":
			return """SELECT %s FROM %s GROUP BY %s""" %(','.join(self.fields), self.from_table, ','.join(self.group_by_fields))

		if self.data_engine.db_type == "sqlite":
			pass
