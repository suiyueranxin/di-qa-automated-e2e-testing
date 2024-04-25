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


class testCIT_RMS_runtime(unittest.TestCase):
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

        has_error = taskmetrics != None and taskmetrics.error != 0
        if has_error:
            message = str(monitor)
            for taskmonitor in taskmonitors:
                message = message + taskmonitor.statusinfo()

            self.fail(message)

    # ABAP CDS to HC
    def test_abap_cds_to_hc_small(self):
        self.check_status('abap-cds-to-hc-small')

    @skip
    def test_abap_cds_to_hc_fat(self):
        self.check_status('abap-cds-to-hc-fat')

    @skip
    def test_abap_cds_to_hc_deltaSmall(self):
        self.check_status('abap-cds-to-hc-deltaSmall')

    # ABAP SLT to HC
    @skip
    def test_abap_slt_to_hc_small(self):
        self.check_status('abap-slt-to-hc-small')

    @skip
    def test_abap_slt_to_hc_fat(self):
        self.check_status('abap-slt-to-hc-fat')

    @skip
    def test_abap_slt_to_hc_deltaSmall(self):
        self.check_status('abap-slt-to-hc-deltaSmall')

    # ABAP CDS to ADL2
    @skip
    def test_abap_cds_to_adlv2_small(self):
        self.check_status('abap-cds-to-adlv2-small')

    @skip
    def test_abap_cds_to_adlv2_fat(self):
        self.check_status('abap-cds-to-adlv2-fat')

    @skip
    def test_abap_cds_to_adlv2_deltaSmall(self):
        self.check_status('abap-cds-to-adlv2-deltaSmall')

    # ABAP SLT to ADL2
    @skip
    def test_abap_slt_to_adlv2_small(self):
        self.check_status('abap-slt-to-adlv2-small')

    @skip
    def test_abap_slt_to_adlv2_fat(self):
        self.check_status('abap-slt-to-adlv2-fat')

    @skip
    def test_abap_slt_to_adlv2_deltaSmall(self):
        self.check_status('abap-slt-to-adlv2-deltaSmall')
