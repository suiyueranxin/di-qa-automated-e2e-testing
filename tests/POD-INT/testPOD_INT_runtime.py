import os
import unittest
from unittest.case import skip

from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Utils import ConnectionData


def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'connectiondata')

    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data


class testPOD_INT_runtime(unittest.TestCase):
    cluster = None

    @classmethod
    def setUp(self) -> None:
        if self.cluster == None:
            connection_data = get_connection_data()
            self.cluster = Cluster.connect_to(connection_data)

        self.assertIsNotNone(self.cluster, 'Connection to cluster failed!')

    def check_status(self, replicationname):
        """Checks whether the replicationflow for the replication with the given
        name has been run without errors."""

        monitor = self.cluster.monitoring.replications.get_monitor(
            replicationname)
        self.assertIsNotNone(
            monitor, f'Monitor for replication flow {replicationname} could not be retrieved!')

        taskmetrics = monitor.taskmetrics

        taskmonitors = self.cluster.monitoring.replications.get_taskmonitors(
            replicationname)

        if taskmetrics == None and len(taskmonitors) == 0:
            self.skipTest('No information available!')

        haserror = taskmetrics != None and taskmetrics.error != 0
        if haserror:
            message = str(monitor)
            for taskmonitor in taskmonitors:
                message = message + taskmonitor.statusinfo()

            self.fail(message)

    # S4H CDS
    # @skip
    def test_ABAP_CDS_S4H_to_HC_deltaSkinny(self):
        self.check_status('ABAP_CDS_S4H_to_HC_deltaSkinny')

    # @skip
    def test_ABAP_CDS_S4H_to_HC_initFat(self):
        self.check_status('ABAP_CDS_S4H_to_HC_initFat')

    # @skip
    def test_ABAP_CDS_S4H_to_HC_initSkinny(self):
        self.check_status('ABAP_CDS_S4H_to_HC_initSkinny')

    # ECC SLT
    # @skip
    def test_ABAP_SLT_ECC_to_HC_deltaSkinny(self):
        self.check_status('ABAP_SLT_ECC_to_HC_deltaSkinny')

    # @skip
    def test_ABAP_SLT_ECC_to_HC_initSkinny(self):
        self.check_status('ABAP_SLT_ECC_to_HC_initSkinny')

    # S4H SLT
    # @skip
    def test_ABAP_SLT_S4H_to_HC_deltaSkinny(self):
        self.check_status('ABAP_SLT_S4H_to_HC_deltaSkinny')

    # @skip
    def test_ABAP_SLT_S4H_to_HC_initFat(self):
        self.check_status('ABAP_SLT_S4H_to_HC_initFat')

    # @skip
    def test_ABAP_SLT_S4H_to_HC_initSkinny(self):
        self.check_status('ABAP_SLT_S4H_to_HC_initSkinny')
