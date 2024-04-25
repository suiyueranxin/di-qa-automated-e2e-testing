from __future__ import annotations
from typing import TYPE_CHECKING

from enum import Enum
import json

from framework.infrastructure.connections.Connection import Connection
from framework.validation.abap.AbapClient import AbapConnectionData
from framework.validation.datalake.DatalakeClient import DatalakeConnectionData
from framework.validation.hana.HanaClient import HanaConnectionData

if TYPE_CHECKING:
    from framework.infrastructure.Cluster import Cluster


class ConnectionManagement:

    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster

    def getconnection(self, connectionid: str) -> Connection:
        path = '/app/datahub-app-connection/connections/' + connectionid
        response = self._cluster.apiget(path)
        if 200 == response.status_code:
            return ConnectionJson.deserialize(response.text)

        return None

    def create_abap_connection(self, connection_data: AbapConnectionData) -> Connection:
        connection = Connection(connection_data.name)
        connection.type = ConnectionType.ABAP.value

        connection.contentData['protocol'] = 'RFC'
        connection.contentData['loadbalancing'] = 'Without Load Balancing'
        connection.contentData['sysid'] = connection_data.sysid
        connection.contentData['ashost'] = connection_data.ashost
        connection.contentData['sysnr'] = connection_data.sysnr
        connection.contentData['client'] = connection_data.client
        connection.contentData['authentication'] = 'Basic'
        connection.contentData['enablesnc'] = False
        connection.contentData['user'] = connection_data.user
        connection.contentData['password'] = connection_data.password

        connection.licenseRelevant = True
        connection.changedNote = ''
        connection.gatewayId = ''
        connection.cloudConnectorLocationId = ''

        return connection

    def create_hana_connection(self, connection_data: HanaConnectionData) -> Connection:
        connection = Connection(connection_data.name)
        connection.type = ConnectionType.HANA.value

        connection.contentData['host'] = connection_data.address
        connection.contentData['port'] = connection_data.port
        connection.contentData['additionalHosts'] = []
        connection.contentData['user'] = connection_data.user
        connection.contentData['password'] = connection_data.password
        connection.contentData['ignoreList'] = []
        connection.contentData['useTLS'] = True
        connection.contentData['useProxy'] = False
        connection.contentData['validateCertificate'] = False

        connection.licenseRelevant = True
        connection.changedNote = ''
        connection.gatewayId = ''
        connection.cloudConnectorLocationId = ''

        return connection

    def create_adlv2_connection(self, connection_data: DatalakeConnectionData) -> Connection:
        connection = Connection(connection_data.name)
        connection.type = ConnectionType.ADLv2.value

        connection.contentData['authorizationMethod'] = 'shared_key'
        connection.contentData['sharedKeys'] = {}
        connection.contentData['sharedKeys']['accountName'] = connection_data.accountname
        connection.contentData['sharedKeys']['accountKey'] = connection_data.accountkey
        connection.contentData['endpointSuffix'] = 'core.windows.net'
        connection.contentData['rootPath'] = connection_data.container

        connection.licenseRelevant = True
        connection.changedNote = ''
        connection.gatewayId = ''
        connection.cloudConnectorLocationId = ''

        return connection

    def save_connection(self, connection: Connection):
        path = '/app/datahub-app-connection/connections'
        payload = ConnectionJson.serialize(connection)
        response = self._cluster.apipost(
            path, payload)
        if 201 == response.status_code:
            return

        raise RuntimeError(response.text)

    def remove_connection(self, connection_id: str) -> None:
        path = f'/app/datahub-app-connection/connections/{connection_id}'
        response = self._cluster.apidelete(path)
        if 204 == response.status_code:
            return

        raise RuntimeError(response.text)


class ConnectionType(Enum):
    ABAP = "ABAP"
    HANA = "HANA_DB"
    ADLv2 = "ADL_V2"


class ConnectionJson:
    @staticmethod
    def deserialize(jsoncontent: str) -> Connection:
        jsondata = json.loads(jsoncontent)
        connection = Connection(jsondata['id'])

        connection._values = jsondata

        return connection

    @staticmethod
    def serialize(connection: Connection) -> str:
        return json.dumps(connection._values)
