from __future__ import annotations
from typing import TYPE_CHECKING

import os
import json

from framework.infrastructure.Cluster import ClusterConnectionData
from framework.validation.abap.AbapClient import AbapConnectionData
from framework.validation.hana.HanaClient import HanaConnectionData
from framework.validation.datalake.DatalakeClient import DatalakeConnectionData


class Utils:

    @staticmethod
    def read_json(path):
        if os.path.isfile(path) and os.access(path, os.R_OK):
            f = open(path)
            data = f.read()
            f.close()
            return json.loads(data)

        return None

    @staticmethod
    def write_json(path, data: dict):
        f = open(path, 'w')
        f.write(json.dumps(data))
        f.close()

    @staticmethod
    def deletefile(path):
        os.remove(path)


class ConnectionData:

    @staticmethod
    def _from_file(name: str, path) -> dict:
        """Returns a dictionary with the connection data for the given id from a file.

        This requires a .json file with the given id as its name to exist. 
        Information that is supposed to stay secret such as credentials and 
        access token should reside in a file with the same name but with extension
        .secrets.json. This way files with that extension can be excluded from
        version control and won't be accidentally committed.
        """

        filename = name + '.json'
        secrets_filename = name + '.secrets.json'

        connection_data = Utils.read_json(os.path.join(path, filename))
        secret_connection_data = Utils.read_json(
            os.path.join(path, secrets_filename))

        if connection_data:
            if secret_connection_data:
                connection_data = connection_data | secret_connection_data

            return connection_data

        return None

    @staticmethod
    def _from_envvar_json(name: str) -> dict:
        envvar_json = os.environ.get(name.upper(), None)

        if envvar_json:
            return json.loads(envvar_json)

        return None

    @staticmethod
    def _from_envvar_prefixed(name: str, keys: list[str]) -> dict:
        prefix = name.upper()

        values = {}
        for key in keys:
            envvar = prefix + '_' + key.upper()
            value = os.environ.get(envvar, None)
            values[key] = value

        return values

    @staticmethod
    def _compile_connection_data(connection_data, name: str, path_to_connection_folder=None):
        """
        This method provides the actual logic for compiling the connection data 
        by combining it from the file system and/or environment variables.

        """
        envvar_json_values = ConnectionData._from_envvar_json(name)
        if envvar_json_values:
            connection_data.fill_properties(envvar_json_values)
            return connection_data

        if path_to_connection_folder:
            file_values = ConnectionData._from_file(
                name, path_to_connection_folder)
            connection_data.fill_properties(file_values)

        envvar_values = ConnectionData._from_envvar_prefixed(
            name, connection_data.property_names)
        connection_data.fill_properties(envvar_values)

        return connection_data

    @staticmethod
    def for_cluster(name: str, path_to_connection_folder=None) -> ClusterConnectionData:
        """ Reads the connection data for the cluster with the given name.

        The connection data can be stored in the file system or in environment 
        variables. Even a combination of file and evironment variables is 
        possible.

        The file approach requires a .json file with the given name to exist in 
        the given path to the folder that contains the connection data. 
        Information that is supposed to stay secret such as credentials and 
        access token should reside in a file with the same name but with 
        extension .secrets.json. Files with that extension can be excluded from 
        version control and won't be accidentally committed. 

        The environment variables can be used in two ways: 

        1. Have the individual values spread across multiple environment 
        variables that all have name parameter in upper case as prefix:
        ConnectionData.for_cluster('abc') will look for ABC_USER, ABC_TENANT
        etc. and will use the values of these variables for the corresponding
        connection parameters

        2. Have a JSON string with all key-value-pairs for the connection 
        assigned to a single environment variable. The name of the environment 
        variable needs to match the name parameter in upper case:
        ConnectionData.for_cluster('abc') will expect environment variable ABC.
        This approach cannot be combined with the file based approach or the 
        first environment variable based approach. It will take precedence over
        the other two!


        """
        connection_data = ClusterConnectionData(name)

        ConnectionData._compile_connection_data(
            connection_data, name, path_to_connection_folder)

        return connection_data

    @staticmethod
    def for_abap(name: str, path_to_connection_folder=None) -> AbapConnectionData:
        """ Reads the connection data for the ABAP system with the given name.

        The connection data can be stored in the file system or in environment
        variables. Even a combination of file and evironment variables is
        possible.

        The file approach requires a .json file with the given name to exist in
        the given path to the folder that contains the connection data.
        Information that is supposed to stay secret such as credentials and
        access token should reside in a file with the same name but with
        extension .secrets.json. Files with that extension can be excluded from
        version control and won't be accidentally committed.

        The environment variables can be used in two ways:

        1. Have the individual values spread across multiple environment
        variables that all have name parameter in upper case as prefix:
        ConnectionData.for_abap('ABC') will look for ABC_USER, ABC_CLIENT
        etc. and will use the values of these variables for the corresponding
        connection parameters

        2. Have a JSON string with all key-value-pairs for the connection
        assigned to a single environment variable. The name of the environment
        variable needs to match the name parameter in upper case:
        ConnectionData.for_abap('ABC') will expect environment variable ABC.
        This approach cannot be combined with the file based approach or the
        first environment variable based approach. It will take precedence over
        the other two!


        """
        connection_data = AbapConnectionData(name)

        ConnectionData._compile_connection_data(
            connection_data, name, path_to_connection_folder)

        return connection_data

    @staticmethod
    def for_hana(name: str, path_to_connection_folder=None) -> HanaConnectionData:
        """ Reads the connection data for the Hana system with the given name.

        The connection data can be stored in the file system or in environment
        variables. Even a combination of file and evironment variables is
        possible.

        The file approach requires a .json file with the given name to exist in
        the given path to the folder that contains the connection data.
        Information that is supposed to stay secret such as credentials and
        access token should reside in a file with the same name but with
        extension .secrets.json. Files with that extension can be excluded from
        version control and won't be accidentally committed.

        The environment variables can be used in two ways:

        1. Have the individual values spread across multiple environment
        variables that all have name parameter in upper case as prefix:
        ConnectionData.for_hana('ABC') will look for ABC_ADDRESS, ABC_PORT
        etc. and will use the values of these variables for the corresponding
        connection parameters

        2. Have a JSON string with all key-value-pairs for the connection
        assigned to a single environment variable. The name of the environment
        variable needs to match the name parameter in upper case:
        ConnectionData.for_hana('ABC') will expect environment variable ABC.
        This approach cannot be combined with the file based approach or the
        first environment variable based approach. It will take precedence over
        the other two!


        """
        connection_data = HanaConnectionData(name)

        ConnectionData._compile_connection_data(
            connection_data, name, path_to_connection_folder)

        return connection_data

    @staticmethod
    def for_datalake(name: str, path_to_connection_folder=None) -> DatalakeConnectionData:
        """ Reads the connection data for the Azure Data Lake system with the 
        given name.

        The connection data can be stored in the file system or in environment
        variables. Even a combination of file and evironment variables is
        possible.

        The file approach requires a .json file with the given name to exist in
        the given path to the folder that contains the connection data.
        Information that is supposed to stay secret such as credentials and
        access token should reside in a file with the same name but with
        extension .secrets.json. Files with that extension can be excluded from
        version control and won't be accidentally committed.

        The environment variables can be used in two ways:

        1. Have the individual values spread across multiple environment
        variables that all have name parameter in upper case as prefix:
        ConnectionData.for_datalake('ABC') will look for ABC_ACCOUNTNAME, 
        ABC_CONTAINER etc. and will use the values of these variables for the 
        corresponding connection parameters

        2. Have a JSON string with all key-value-pairs for the connection
        assigned to a single environment variable. The name of the environment
        variable needs to match the name parameter in upper case:
        ConnectionData.for_datalake('ABC') will expect environment variable ABC.
        This approach cannot be combined with the file based approach or the
        first environment variable based approach. It will take precedence over
        the other two!


        """
        connection_data = DatalakeConnectionData(name)

        ConnectionData._compile_connection_data(
            connection_data, name, path_to_connection_folder)

        return connection_data
