import os
import unittest
#from rich import print

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


class testMonitoringBasics(unittest.TestCase):
    def test_basics(self):
        connection_data = get_connection_data()
        cluster = Cluster.connect_to(connection_data)
        self.assertIsNotNone(cluster, 'Connection to cluster failed!')

        cut = cluster.monitoring

        replicationmonitors = cut.replications.get_monitors()

        print()
        for monitor in replicationmonitors:
            error = monitor.taskmetrics.error
            print(monitor)

            #color = 'yellow'
            # if ('RUNNING' == monitor.getstatus()):
            #    color = 'bold green'
            # if(error > 0):
            #    color = 'bold red'
            # print(f'[{color}]{monitor}[/{color}]')

            self.assertEqual(0, error)
