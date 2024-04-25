import os
import unittest

from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Utils import ConnectionData

test_path = os.path.join(os.path.dirname(
    __file__), 'testdata', 'connectiondata')


def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data


class testConnectionManagementBasics(unittest.TestCase):

    @unittest.skip
    def test_save_abap_connection(self):
        connection_data = get_connection_data()
        cluster = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cluster)

        cut = cluster.connectionmanagement

        connection_data = ConnectionData.for_abap('dummyABAP', test_path)

        connection = cut.create_abap_connection(connection_data)
        connection.description = 'Just a dummy. Can be deleted.'

        # The following line can be used to produce an error
        # connection.contentData['sysid'] = 'not a valid sysid'

        cut.save_connection(connection)
        cut.remove_connection(connection.id)

    def test_save_hana_connection(self):
        connection_data = get_connection_data()
        cluster = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cluster)

        cut = cluster.connectionmanagement

        connection_data = ConnectionData.for_hana('dummyHana', test_path)

        connection = cut.create_hana_connection(connection_data)
        connection.description = 'Just a dummy. Can be deleted.'

        cut.save_connection(connection)
        cut.remove_connection(connection.id)

    def test_save_adlv2_connection(self):
        connection_data = get_connection_data()
        cluster = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cluster)

        cut = cluster.connectionmanagement

        connection_data = ConnectionData.for_datalake('dummyADLv2', test_path)

        connection = cut.create_adlv2_connection(connection_data)
        connection.description = 'Just a dummy. Can be deleted.'

        cut.save_connection(connection)
        cut.remove_connection(connection.id)


if __name__ == '__main__':
    unittest.main()
