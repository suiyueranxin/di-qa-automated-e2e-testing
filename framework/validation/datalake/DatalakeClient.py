from __future__ import annotations
from typing import TYPE_CHECKING
import json

from azure.storage.filedatalake import DataLakeServiceClient

from framework.infrastructure.utils.ConnectionDataBase import ConnectionDataBase


class DatalakeClient:

    def __init__(self) -> None:
        self._connection = None

    @staticmethod
    def connect_to(connection_data: DatalakeConnectionData = None, **kwargs) -> DatalakeClient:
        datalakeclient = DatalakeClient()

        if connection_data == None:
            datalakeclient.accountname = kwargs['accountname']
            datalakeclient.accountkey = kwargs['accountkey']
            datalakeclient.container = kwargs['container']
        else:
            datalakeclient.accountname = connection_data.accountname
            datalakeclient.accountkey = connection_data.accountkey
            datalakeclient.container = connection_data.container

        connect_string = "DefaultEndpointsProtocol=https;AccountName=" + \
            datalakeclient.accountname + ";AccountKey=" + \
            datalakeclient.accountkey + ";EndpointSuffix=core.windows.net"
        datalakeclient._connection = DataLakeServiceClient.from_connection_string(
            conn_str=connect_string, logging_enable=True)
        return datalakeclient

    def get_fileNames(self, foldername: str):
        file_system_client = self._connection.get_file_system_client(
            self.container)
        paths = file_system_client.get_paths(path=foldername)
        pathName = []
        for path in paths:
            pathName.append(path.name)
        return pathName

    def is_file_Exist(self, foldername: str, filename: str) -> bool:
        paths = self.get_fileNames(foldername)
        for path in paths:
            print(path + '\n')
            if path == foldername+"/"+filename:
                return True
        return False

    def get_jsonfile_content(self, foldername: str, filename: str):
        file_system_client = self._connection.get_file_system_client(
            self.container)
        directory_client = file_system_client.get_directory_client(foldername)
        file_client = directory_client.get_file_client(filename)
        download = file_client.download_file()
        downloaded_bytes = download.readall().decode('utf-8')
        content = json.loads(downloaded_bytes)
        return content


class DatalakeConnectionData(ConnectionDataBase):

    property_names = ['accountname', 'container', 'accountkey']

    def __init__(self, name: str) -> None:
        self._values = {}
        self._values['name'] = name

    @property
    def name(self) -> str:
        return self._values.get('name', None)

    @property
    def accountname(self) -> str:
        return self._values.get('accountname', None)

    @accountname.setter
    def accountname(self, accountname: str) -> None:
        self._values['accountname'] = accountname

    @property
    def accountkey(self) -> str:
        return self._values.get('accountkey', None)

    @accountkey.setter
    def accountkey(self, accountkey: str) -> None:
        self._values['accountkey'] = accountkey

    @property
    def container(self) -> str:
        return self._values.get('container', None)

    @container.setter
    def container(self, container: str) -> None:
        self._values['container'] = container

    def fill_properties(self, values_dict: dict, property_names=property_names) -> None:
        super().fill_properties(values_dict, property_names)
