from abc import ABC, abstractmethod
from .sqlWrapper import SqlWrapper
from .. import dlaConstants as dlac

class Query(ABC):
	def __init__(self):
		self.sqlType = dlac.DB_TYPE

	@abstractmethod
	def build_query(self):
		pass

	@abstractmethod
	def execute_query(self):
		pass


class QueryBuilder():

#should have the dataEngine name
	@staticmethod
	def create_select_query(from_table):
		obj = SelectQuery(from_table)
		return obj


class SelectQuery(Query):

	def __init__(self, from_table):
		super().__init__()
		self.sql = None
		self.fields = None
		self.group_by_fields = None
		self.from_table = from_table

	def set_fields(self, fields):
		self.fields = fields
		return self

	def group_by(self, group_by_fields):
		self.group_by_fields = group_by_fields
		return self

	def execute_query(self, obj):
		self.sql = self.build_query()
		print("********************************************")
		print(self.__dict__)
		print(obj.__dict__)
		print("********************************************")
		return obj.sql_wrapper.execute_get_list(self.sql)

	def build_query(self):
		if self.sqlType == "mysql":
			return """SELECT %s FROM %s GROUP BY %s""" %(','.join(self.fields), self.from_table, ','.join(self.group_by_fields))

		if self.sqlType == "sqlite":
			pass
