import json
import unittest
import os

from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.ConnectionManagement import ConnectionJson, ConnectionManagement, ConnectionType
from framework.infrastructure.Utils import ConnectionData
from framework.unittests.doubles.SessionMock import SessionMock

test_path = os.path.join(os.path.dirname(
    __file__), 'testdata', 'connectiondata')


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


def getDummyConnection():
    return """
    {
        "id": "S4H_2021",
        "description": "S4H 2021 - via QOI-927 (GAERTNERNI) ",
        "type": "ABAP",
        "contentData": {
            "sysid": "QOI",
            "client": "927",
            "sap_language": "EN",
            "protocol": "RFC",
            "loadbalancing": "Without Load Balancing",
            "ashost": "ldciqoi.wdf.sap.corp",
            "sysnr": "00",
            "saprouter": "",
            "enablesnc": false,
            "user": "GAE*******",
            "authentication": "Basic"
        },
        "tags": [],
        "tagsFromType": [
            "application"
        ],
        "owner": "system",
        "ownerDisplayName": "system",
        "createdBy": "system",
        "createdByDisplayName": "system",
        "createdAt": "2021-10-04T09:31:51.015Z",
        "changedBy": "d060045",
        "changedByDisplayName": "d060045",
        "changedAt": "2021-10-18T11:52:01.767Z",
        "changedNote": "",
        "status": {
            "connectionId": "S4H_2021",
            "status": "UNKNOWN",
            "message": ""
        },
        "gatewayId": "SAP Cloud Connector",
        "cloudConnectorLocationId": "",
        "licenseRelevant": true,
        "readOnly": false,
        "ccmId": "663a485a-b7b6-48df-bc22-34d0527c7961",
        "ccmTypeId": "ABAP",
        "typeServices": [
            {
                "provider": "com.sap.dh.metadata",
                "type": "extractMetadata"
            },
            {
                "provider": "com.sap.dh.flowagent",
                "type": "com.sap.dh.connection::statusCheck"
            },
            {
                "provider": "com.sap.dh.flowagent",
                "type": "browse"
            },
            {
                "provider": "com.sap.dh.flowagent",
                "type": "metadata"
            },
            {
                "provider": "com.sap.dh.flowagent",
                "type": "dataPreview"
            }
        ]
    }
    """


class testConnectionManagement(unittest.TestCase):

    def test_basics(self):
        cut = ConnectionManagement(None)

    def test_getconnection(self):
        cluster = Cluster(getDummyConnectionData())

        content = getDummyConnection()
        cluster.session = SessionMock()
        cluster.session.setresponsecontent(200, content)

        cut = ConnectionManagement(cluster)
        connection = cut.getconnection('S4H_2021')
        self.assertEqual('S4H_2021', connection.id)
        self.assertEqual('S4H 2021 - via QOI-927 (GAERTNERNI) ',
                         connection.description)
        self.assertEqual('ABAP', connection.type)
        self.assertEqual('ABAP', connection.ccmTypeId)

        cluster.session.setresponse(404)
        connection = cut.getconnection('NON-EXISTING')
        self.assertIsNone(connection)

    def test_create_abap_connection(self):
        cluster = Cluster(getDummyConnectionData())
        abap_connection_data = ConnectionData.for_abap('dummyABAP', test_path)

        cut = ConnectionManagement(cluster)

        abap_connection = cut.create_abap_connection(abap_connection_data)

        self.assertIsNotNone(abap_connection)
        self.assertEqual('dummyABAP', abap_connection.id)
        self.assertEqual('', abap_connection.description)
        self.assertTrue(abap_connection.licenseRelevant)
        self.assertEqual(ConnectionType.ABAP.value, abap_connection.type)
        self.assertEqual('', abap_connection.changedNote)
        self.assertEqual('', abap_connection.gatewayId)
        self.assertEqual('', abap_connection.cloudConnectorLocationId)
        self.assertEqual([], abap_connection.tags)

        self.assertIsNotNone(abap_connection.contentData)
        self.assertEqual('RFC', abap_connection.contentData['protocol'])
        self.assertEqual('Without Load Balancing',
                         abap_connection.contentData['loadbalancing'])
        self.assertEqual('DUM', abap_connection.contentData['sysid'])
        self.assertEqual('ldciqoi.wdf.sap.corp',
                         abap_connection.contentData['ashost'])
        self.assertEqual('00', abap_connection.contentData['sysnr'])
        self.assertEqual('244', abap_connection.contentData['client'])
        self.assertEqual(
            'Basic', abap_connection.contentData['authentication'])
        self.assertFalse(abap_connection.contentData['enablesnc'])
        self.assertEqual('ANZEIGER', abap_connection.contentData['user'])
        self.assertEqual('topSecretPassword',
                         abap_connection.contentData['password'])

    def test_create_hana_connection(self):
        cluster = Cluster(getDummyConnectionData())
        hana_connection_data = ConnectionData.for_hana('dummyHana', test_path)

        cut = ConnectionManagement(cluster)

        hana_connection = cut.create_hana_connection(hana_connection_data)

        self.assertIsNotNone(hana_connection)
        self.assertEqual('dummyHana', hana_connection.id)
        self.assertEqual('', hana_connection.description)
        self.assertTrue(hana_connection.licenseRelevant)
        self.assertEqual(ConnectionType.HANA.value, hana_connection.type)
        self.assertEqual('', hana_connection.changedNote)
        self.assertEqual('', hana_connection.gatewayId)
        self.assertEqual('', hana_connection.cloudConnectorLocationId)
        self.assertEqual([], hana_connection.tags)

        self.assertIsNotNone(hana_connection.contentData)
        self.assertEqual('eu10.hana.com', hana_connection.contentData['host'])
        self.assertEqual(443, hana_connection.contentData['port'])
        self.assertEqual([], hana_connection.contentData['additionalHosts'])
        self.assertEqual('testuser', hana_connection.contentData['user'])
        self.assertEqual('******', hana_connection.contentData['password'])
        self.assertEqual([], hana_connection.contentData['ignoreList'])
        self.assertTrue(hana_connection.contentData['useTLS'])
        self.assertFalse(hana_connection.contentData['useProxy'])
        self.assertFalse(hana_connection.contentData['validateCertificate'])

    def test_create_adlv2_connection(self):
        cluster = Cluster(getDummyConnectionData())
        adlv2_connection_data = ConnectionData.for_datalake(
            'dummyDatalake', test_path)

        cut = ConnectionManagement(cluster)

        adlv2_connection = cut.create_adlv2_connection(adlv2_connection_data)

        self.assertIsNotNone(adlv2_connection)
        self.assertEqual('dummyDatalake', adlv2_connection.id)
        self.assertEqual('', adlv2_connection.description)
        self.assertTrue(adlv2_connection.licenseRelevant)
        self.assertEqual(ConnectionType.ADLv2.value, adlv2_connection.type)
        self.assertEqual('', adlv2_connection.changedNote)
        self.assertEqual('', adlv2_connection.gatewayId)
        self.assertEqual('', adlv2_connection.cloudConnectorLocationId)
        self.assertEqual([], adlv2_connection.tags)

        self.assertIsNotNone(adlv2_connection.contentData)
        self.assertEqual(
            'shared_key', adlv2_connection.contentData['authorizationMethod'])
        self.assertIsNotNone(adlv2_connection.contentData['sharedKeys'])

        self.assertEqual(
            'dummyAdl', adlv2_connection.contentData['sharedKeys']['accountName'])
        self.assertEqual(
            '******', adlv2_connection.contentData['sharedKeys']['accountKey'])

        self.assertEqual('core.windows.net',
                         adlv2_connection.contentData['endpointSuffix'])
        self.assertEqual('dataintegration2021',
                         adlv2_connection.contentData['rootPath'])

    def test_save_connection(self):
        cluster = Cluster(getDummyConnectionData())
        abap_connection_data = ConnectionData.for_abap('dummyABAP', test_path)

        cut = ConnectionManagement(cluster)

        connection = cut.create_abap_connection(abap_connection_data)

        cluster.session = SessionMock()
        cluster.session.setresponse(201)

        cut.save_connection(connection)

        self.assertTrue(cluster.session.lastcalledurl.endswith(
            'app/datahub-app-connection/connections'))
        self.assertEqual(ConnectionJson.serialize(
            connection), cluster.session.posteddata)

    def test_save_connection_with_error(self):
        cluster = Cluster(getDummyConnectionData())
        abap_connection_data = ConnectionData.for_abap('dummyABAP', test_path)

        cut = ConnectionManagement(cluster)

        connection = cut.create_abap_connection(abap_connection_data)

        cluster.session = SessionMock()
        cluster.session.setresponsecontent(400, 'This is the error message!')

        connection.contentData['sysid'] = 'not a valid sysid'

        self.assertRaises(RuntimeError, cut.save_connection, connection)

    def test_remove_connectio(self):
        cluster = Cluster(getDummyConnectionData())
        #abap_connection_data = ConnectionData.for_abap('dummyABAP', test_path)

        cut = ConnectionManagement(cluster)

        cluster.session = SessionMock()
        cluster.session.setresponse(204)

        cut.remove_connection('dummyABAP')

        self.assertTrue(cluster.session.lastcalledurl.endswith(
            'app/datahub-app-connection/connections/dummyABAP'))

    def test_remove_connectio_with_errors(self):
        cluster = Cluster(getDummyConnectionData())

        cut = ConnectionManagement(cluster)

        cluster.session = SessionMock()
        cluster.session.setresponsecontent(400, 'This is the error message!')

        self.assertRaises(RuntimeError, cut.remove_connection, 'non-existing')


class testConnectionDeSerializer(unittest.TestCase):

    def test_deserialize(self):
        connectionJson = getDummyConnection()
        connection = ConnectionJson.deserialize(connectionJson)
        self.assertEqual('S4H_2021', connection.id)
        self.assertEqual('S4H 2021 - via QOI-927 (GAERTNERNI) ',
                         connection.description)
        self.assertEqual('ABAP', connection.type)

    def test_serialize_basic_abap_connection(self):
        cluster = Cluster(getDummyConnectionData())
        abap_connection_data = ConnectionData.for_abap('dummyABAP', test_path)
        cut = ConnectionManagement(cluster)
        abap_connection = cut.create_abap_connection(abap_connection_data)

        connectionJson = ConnectionJson.serialize(abap_connection)

        expectedJson = _get_simple_abap_connection_json()

        self.assertEqual(json.loads(expectedJson), json.loads(connectionJson))


def _get_simple_abap_connection_json():
    return """
{
  "id": "dummyABAP",
  "description": "",
  "licenseRelevant": true,
  "type": "ABAP",
  "contentData": {
    "protocol": "RFC",
    "loadbalancing": "Without Load Balancing",
    "sysid": "DUM",
    "ashost": "ldciqoi.wdf.sap.corp",
    "sysnr": "00",
    "client": "244",
    "authentication": "Basic",
    "enablesnc": false,
    "user": "ANZEIGER",
    "password": "topSecretPassword"
  },
  "changedNote": "",
  "gatewayId": "",
  "cloudConnectorLocationId": "",
  "tags": []
}
"""


if __name__ == '__main__':
    unittest.main()
