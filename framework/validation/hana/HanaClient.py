from __future__ import annotations
from typing import TYPE_CHECKING

from hdbcli import dbapi

from framework.infrastructure.utils.ConnectionDataBase import ConnectionDataBase


class HanaClient:

    def __init__(self) -> None:
        self._connection = None

    @staticmethod
    def connect_to(connection_data: HanaConnectionData = None, **kwargs) -> HanaClient:
        hanaClient = HanaClient()
        if connection_data is None:
            hanaClient._connection = dbapi.connect(
                address=kwargs['address'], port=kwargs['port'], user=kwargs['user'], password=kwargs['password'])
        else:
            hanaClient._connection = dbapi.connect(
                address=connection_data.address,
                port=connection_data.port,
                user=connection_data.user,
                password=connection_data.password)
        return hanaClient

    def get_rowcount(self, schemaname: str, tablename: str) -> int:
        cursor = self._connection.cursor()
        sql_command = f'SELECT "RECORD_COUNT" FROM "SYS"."M_TABLES" WHERE SCHEMA_NAME = \'{schemaname}\' AND TABLE_NAME = \'{tablename}\''
        cursor.execute(sql_command)
        rows = cursor.fetchall()
        cursor.close()
        return rows[0][0]

    def get_rows_by_offset(self, schemaname: str, tablename: str, limit: int, offset: int, columnname: str = None, isDesc: bool = False):
        cursor = self._connection.cursor()
        order = "DESC" if isDesc else "ASC"
        if columnname is not None:
            sql_command = f'SELECT * FROM {schemaname}.{tablename} ORDER BY {columnname} {order} LIMIT {limit} OFFSET {offset}'
        else:
            sql_command = f'SELECT * FROM {schemaname}.{tablename} LIMIT {limit} OFFSET {offset}'
        cursor.execute(sql_command)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def drop_table(self, schemaname: str, tablename: str) -> None:
        cursor = self._connection.cursor()
        sql_command = f'DROP TABLE {schemaname}.{tablename}'
        cursor.execute(sql_command)
        cursor.close()

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Checks whether the table with the given name exists within the schema 
        with the provided name in the database system.

        Parameters
        ----------
        schema_name : str
            The name of the schema.
        table_name : str
            The name of the table to be checked.

        Returns
        -------
        bool
            `True` if the table exists, otherwise `False`
        """
        cursor = self._connection.cursor()
        sql_command = ('SELECT * FROM OBJECTS WHERE '
                       'OBJECT_TYPE=\'TABLE\' AND '
                       f'SCHEMA_NAME=\'{schema_name}\' AND '
                       f'OBJECT_NAME=\'{table_name}\'')
        cursor.execute(sql_command)
        rows = cursor.fetchall()
        cursor.close()
        return len(rows) > 0

    def create_schema(self, schema_name: str) -> None:
        """
        Creates a new schema with the given name in the database system.

        Parameters
        ----------
        schema_name : str
            The name of the schema to be created.        
        """
        cursor = self._connection.cursor()
        sql_command = f'CREATE SCHEMA "{schema_name}"'
        cursor.execute(sql_command)
        cursor.close()

    def delete_schema(self, schema_name: str) -> None:
        """
        Deletes the schema with the given name in the database system.

        Parameters
        ----------
        schema_name : str
            The name of the schema to be deleted.        
        """
        cursor = self._connection.cursor()
        sql_command = f'DROP SCHEMA "{schema_name}" CASCADE'
        cursor.execute(sql_command)
        cursor.close()

    def schema_exists(self, schema_name: str) -> bool:
        """
        Checks whether the schema with the given name exists in the database 
        system.

        Parameters
        ----------
        schema_name : str
            The name of the schema to be checked.

        Returns
        -------
        bool
            `True` if the schema exists, otherwise `False`
        """
        cursor = self._connection.cursor()
        sql_command = f'SELECT * FROM "SYS"."SCHEMAS" WHERE (SCHEMA_NAME = \'{schema_name}\')'
        cursor.execute(sql_command)
        rows = cursor.fetchall()
        cursor.close()
        return len(rows) > 0


class HanaConnectionData(ConnectionDataBase):

    property_names = ['address', 'port', 'user', 'password']

    def __init__(self, name: str) -> None:
        self._values = {}
        self._values['name'] = name

    @property
    def name(self) -> str:
        return self._values.get('name', None)

    @property
    def address(self) -> str:
        return self._values.get('address', None)

    @address.setter
    def address(self, address: str) -> None:
        self._values['address'] = address

    @property
    def port(self) -> str:
        return self._values.get('port', None)

    @port.setter
    def port(self, port):
        self._values['port'] = port

    @property
    def user(self) -> str:
        return self._values.get('user', None)

    @user.setter
    def user(self, user: str) -> None:
        self._values['user'] = user

    @property
    def password(self) -> str:
        return self._values.get('password', None)

    @password.setter
    def password(self, password: str) -> None:
        self._values['password'] = password

    def fill_properties(self, values_dict: dict, property_names=property_names) -> None:
        super().fill_properties(values_dict, property_names)
