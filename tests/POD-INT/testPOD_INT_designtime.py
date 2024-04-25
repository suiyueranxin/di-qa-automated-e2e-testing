import os
import unittest
from unittest.case import skip

from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.Rms import ChangerequeststatusStatus
from framework.infrastructure.Utils import ConnectionData
from framework.infrastructure.replications.Replication import ReplicationLoadtype, ReplicationTaskFilterOperator


def get_connection_data():
    # Make sure that you provide the credentials either in CIT.secrets.json or
    # have them in environment variables:
    # CIT_USER =
    # CIT_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'connectiondata')

    connection_data = ConnectionData.for_cluster('CIT', test_path)
    return connection_data


class testPOD_INT_designtime(unittest.TestCase):
    cluster = None

    @classmethod
    def setUp(self) -> None:
        if self.cluster == None:
            connection_data = get_connection_data()
            self.cluster = Cluster.connect_to(connection_data)

        self.assertIsNotNone(self.cluster, 'Connection to cluster failed!')

    @skip
    def test_ABAP_CDS_S4H_to_HC_initSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_CDS_S4H_to_HC_initSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('ZE2E_SEPM_I_SALESORDERITEM')
        task.set_targetdataset(
            'E2E_ABAP_CDS_S4H_to_HC_initSkinny_Z_SEPM_I_SALESORDERITEM')

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
    def test_ABAP_CDS_S4H_to_HC_deltaSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_CDS_S4H_to_HC_deltaSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('ZE2E_SEPM_I_SALESORDER')
        task.set_targetdataset(
            'E2E_ABAP_CDS_S4H_to_HC_deltaSkinny_Z_SEPM_I_SALESORDER')
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

    @skip
    def test_ABAP_CDS_S4H_to_HC_initFat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_CDS_S4H_to_HC_initFat')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('ZE2E_ACDOCA')
        task.set_targetdataset('E2E_ABAP_CDS_S4H_to_HC_initFat_Z_ACDOCA')
        task.set_loadtype(ReplicationLoadtype.INITIAL)

        task.filters.add_filter(
            'Rldnr', ReplicationTaskFilterOperator.EQUALS, '0D')
        task.filters.add_filter(
            'Gjahr', ReplicationTaskFilterOperator.EQUALS, '2013')

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
    def test_ABAP_SLT_S4H_to_HC_initSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_SLT_S4H_to_HC_initSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/SLT/771')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('SNWD_SO_I')
        task.set_targetdataset('E2E_ABAP_SLT_S4H_to_HC_initSkinny_SNWD_SO_I')

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
    def test_ABAP_SLT_S4H_to_HC_deltaSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_SLT_S4H_to_HC_deltaSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/SLT/771')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('SNWD_SO')
        task.set_targetdataset('E2E_ABAP_SLT_S4H_to_HC_deltaSkinny_SNWD_SO')
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

    @skip
    def test_ABAP_SLT_S4H_to_HC_initFat(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_SLT_S4H_to_HC_initFat')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4H_2021', '/SLT/771')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('ACDOCA')
        task.set_targetdataset('E2E_ABAP_SLT_S4H_to_HC_initFat_ACDOCA')
        task.set_loadtype(ReplicationLoadtype.INITIAL)

        task.filters.add_filter(
            'RLDNR', ReplicationTaskFilterOperator.EQUALS, '0D')
        task.filters.add_filter(
            'GJAHR', ReplicationTaskFilterOperator.EQUALS, '2013')

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
    def test_ABAP_SLT_ECC_to_HC_initSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_SLT_ECC_to_HC_initSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('UK5', '/SLT/5JF')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('SNWD_SO_I')
        task.set_targetdataset('E2E_ABAP_SLT_ECC_to_HC_initSkinny_SNWD_SO_I')

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
    def test_ABAP_SLT_ECC_to_HC_deltaSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_SLT_ECC_to_HC_deltaSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('UK5', '/SLT/5JF')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('SNWD_SO')
        task.set_targetdataset('E2E_ABAP_SLT_ECC_to_HC_deltaSkinny_SNWD_SO')
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

    @skip
    def test_ABAP_CDS_S4HC_to_HC_initSkinny(self):
        replication = self.cluster.modeler.replications.create_replication(
            'E2E_CDS_S4HC_to_HC_initSkinny')

        replication.set_description('Created by POD-INT E2E testing framework')

        replication.set_sourcespace('S4HANA_CLOUD_APE', '/CDS')
        replication.set_targetspace('HANA_CLOUD', '/SYSTEM')
        task = replication.create_task('I_CITYCODE')  # BUSINESSPARTNER')
        task.set_targetdataset(
            'E2E_ABAP_CDS_S4HC_to_HC_initSkinny_I_CITYCODE')  # BUSINESSPARTNER')

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


if __name__ == '__main__':
    unittest.main()
