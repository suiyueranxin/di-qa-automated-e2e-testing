import os
import unittest

from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Utils import ConnectionData


def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'testdata', 'connectiondata')

    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data


class testConnectionToCluster(unittest.TestCase):
    def test_basics(self):
        connection_data = get_connection_data()
        cut = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cut)

    def test_repository_basics(self):
        connection_data = get_connection_data()
        cut = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cut)
        repository = cut.repository
        status = repository.get_status(
            'user', 'files/rms/ABAP_CDS_S4H_to_HC_initFat.replication')
        self.assertIsNotNone(status)
        self.assertEqual(200, status.status_code)

        dummyobjectpath = 'files/rms/ts_dummy.replication'
        if repository.exists('user', dummyobjectpath):
            status = repository.remove('user', dummyobjectpath)
            self.assertIsNotNone(status)
            self.assertEqual(200, status.status_code)

        status = repository.get_status('user', dummyobjectpath)
        self.assertIsNotNone(status)
        self.assertEqual(404, status.status_code)

        payload = """{
    "name": "ABCDEFGH123",
    "description": "",
    "version": "ONE_SOURCE_ONE_TARGET",
    "sourceSpaces": [
        {
            "name": "",
            "connectionId": "",
            "connectionType": "",
            "technicalName": "",
            "ccmConnectionId": "",
            "ccmConnectionType": "",
            "container": ""
        }
    ],
    "targetSpaces": [
        {
            "name": "",
            "connectionId": "",
            "connectionType": "",
            "technicalName": "",
            "ccmConnectionId": "",
            "ccmConnectionType": "",
            "container": ""
        }
    ],
    "oneSourceOneTargetTasks": []
}"""

        status = repository.write('user', dummyobjectpath, payload)
        self.assertIsNotNone(status)
        self.assertEqual(200, status.status_code)

        status = repository.read('user', dummyobjectpath)
        self.assertEqual(200, status.status_code)
        self.assertEqual(payload, status.text)

    def test_connectionmanagement_basics(self):
        connectionData = get_connection_data()
        cut = Cluster.connect_to(connectionData)
        self.assertIsNotNone(cut)
        connectionmanagement = cut.connectionmanagement
        connection = connectionmanagement.getconnection('S4H_2021')
        self.assertIsNotNone(connection)
        self.assertEqual('S4H_2021', connection.id())
        self.assertEqual('ABAP', connection.type())


if __name__ == '__main__':
    unittest.main()
