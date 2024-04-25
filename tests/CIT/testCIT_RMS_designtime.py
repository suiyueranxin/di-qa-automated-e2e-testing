import os
import unittest
from unittest.case import skip

from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Rms import ChangerequeststatusStatus
from framework.infrastructure.Utils import ConnectionData
from framework.infrastructure.replications.Replication import ReplicationLoadtype, ReplicationSpaceFileCompression, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy


def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'connectiondata')

    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data


class testCIT_RMS_designtime(unittest.TestCase):
    cluster = None

    @classmethod
    def setUp(self) -> None:
        if self.cluster == None:
            connection_data = get_connection_data()
            self.cluster = Cluster.connect_to(connection_data)

        self.assertIsNotNone(self.cluster, 'Connection to cluster failed!')

    # ABAP CDS to HC
    @skip
    def test_abap_cds_to_hc_small(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-hc-small')

        replication.set_description('Replication of Small CDS View to HANA DB')

        replication.set_sourcespace('CIT_S4', '/CDS')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        replication.create_task('DHE2E_CDS_WS_LS')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip
    def test_abap_cds_to_hc_fat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-hc-fat')

        replication.set_description('Replication of Fat CDS View to HANA DB')

        replication.set_sourcespace('CIT_S4', '/CDS')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        replication.create_task('DHE2E_CDS_WF_LL')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip
    def test_abap_cds_to_hc_deltaSmall(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-hc-deltaSmall')

        replication.set_description(
            'Replication of Small CDS View to HANA DB (incl. delta)')

        replication.set_sourcespace('CIT_S4', '/CDS')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        task = replication.create_task('DHE2E_CDS_WS_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    # ABAP SLT to HC
    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_hc_small(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-hc-small')

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to HANA DB')

        replication.set_sourcespace('UK5', '/SLT')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        replication.create_task('LTE2E_WS_LS')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_hc_fat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-hc-fat')

        replication.set_description(
            'Replication of Table LTE2E_WF_LL to HANA DB')

        replication.set_sourcespace('UK5', '/SLT')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        replication.create_task('LTE2E_WF_LL')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_hc_deltaSmall(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-hc-deltaSmall')

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to HANA DB (incl. delta)')

        replication.set_sourcespace('UK5', '/SLT')
        replication.set_targetspace('CIT_HANA', '/RMS_CIT_TARGET')
        task = replication.create_task('LTE2E_WS_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    # ABAP CDS to ADL2

    @skip
    def test_abap_cds_to_adlv2_small(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-adlv2-small')

        replication.set_description('Replication of Small CDS View to ADLv2')

        replication.set_sourcespace('SAL', '/CDS')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('DHE2E_CDS_WS_LS')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip
    def test_abap_cds_to_adlv2_fat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-adlv2-fat')

        replication.set_description('Replication of fat CDS View to ADLv2')

        replication.set_sourcespace('SAL', '/CDS')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('DHE2E_CDS_WF_LL')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip
    def test_abap_cds_to_adlv2_deltaSmall(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-cds-to-adlv2-deltaSmall')

        replication.set_description(
            'Replication of small CDS View to ADLv2 (incl. delta)')

        replication.set_sourcespace('SAL', '/CDS')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)

        task = replication.create_task('DHE2E_CDS_WS_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    # ABAP SLT to ADL2

    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_adlv2_small(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-adlv2-small')

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to ADLv2')

        replication.set_sourcespace('UK5', '/SLT')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)

        replication.create_task('LTE2E_WS_LS')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_adlv2_fat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-adlv2-fat')

        replication.set_description(
            'Replication of Table LTE2E_WL_LF to ADLv2')

        replication.set_sourcespace('UK5', '/SLT')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('LTE2E_WF_LL')

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

    @skip  # Before enabling this test you need to provide a valid MTID in the sourcespace
    def test_abap_slt_to_adlv2_deltaSmall(self):
        replication = self.cluster.modeler.replications.create_replication(
            'abap-slt-to-adlv2-deltaSmall')

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to ADLv2')

        replication.set_sourcespace('UK5', '/SLT')
        targetspace = replication.set_targetspace('ADLv2', '/CIT_TEST')
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        task = replication.create_task('LTE2E_WS_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)

        replicationflow.runorresume()

        changerequeststatus = replicationflow.waitwhilebusy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.getstatus(), changerequeststatus)
