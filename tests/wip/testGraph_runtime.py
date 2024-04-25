import unittest
import uuid
import os
from datetime import datetime
from decimal import *
from framework.infrastructure.Cluster import Cluster
from framework.infrastructure.App import App
from framework.infrastructure.utils.netweaver.SLTConfigurationOperation import SLTConfigurationOperation as sltConfig
from framework.validation.hana.HanaClient import HanaClient
from framework.infrastructure.Utils import ConnectionData


def get_connection_data():
    # Make sure that you provide the credentials either in CET.secrets.json or
    # have them in environment variables:
    # CET_USER =
    # CET_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'connectiondata')

    connection_data = ConnectionData.for_cluster('CET', test_path)
    return connection_data

def get_hana_connection_data(name: str):
    # Make sure that you provide the credentials for relevant hana connection, either in HANA_EU10.secrets.json or
    # have them in environment variables:
    # HANA_EU10_USER =
    # HANA_EU10_PASSWORD =
    test_path = os.path.join(os.path.dirname(
        __file__), 'connectiondata')

    connection_data = ConnectionData.for_hana(name, test_path)
    return connection_data


class testPOD_Graph_Pipeline(unittest.TestCase):
    cluster = None
    hanaClient = None
    hanaSchema = "CIT_TEST"

    def check_cds_first_row_data(self, row):
        self.assertEqual("42010AEF-3F5B-1EDC-8B86-5211B4EFA0EF",row[0][0])
        self.assertEqual("0500000033",row[0][1])
        self.assertEqual("42010AEF-3F5B-1EDC-8390-FCCF9A34C0EF",row[0][2])
        self.assertEqual("EUR",row[0][10])
        self.assertEqual(Decimal('1704'),row[0][11])
        self.assertEqual(Decimal('1432'),row[0][12])
        self.assertEqual(Decimal('272'),row[0][13])

    def check_cds_last_row_data(self, row):
        self.assertEqual("42010AEF-3F5B-1EDC-A987-0C85926A60EF",row[0][0])
        self.assertEqual("0508040669",row[0][1])
        self.assertEqual("42010AEF-3F5B-1EDC-8390-FCCF9A34C0EF",row[0][2])
        self.assertEqual("EUR",row[0][10])
        self.assertEqual(Decimal('1445'),row[0][11])
        self.assertEqual(Decimal('1214'),row[0][12])
        self.assertEqual(Decimal('231'),row[0][13])
    
    def check_slt_first_row_data(self, row: list[tuple]):
        self.assertEqual("800",row[0][0])
        self.assertEqual("42010AEE-1BB6-1EDC-92ED-F8D65EC6547A",row[0][1])
        self.assertEqual("0500000000",row[0][2])
        self.assertEqual("EUR",row[0][11])
        self.assertEqual(Decimal('25867'),row[0][12])
        self.assertEqual(Decimal('21737'),row[0][13])
        self.assertEqual(Decimal('4130'),row[0][14])

    def check_slt_last_row_data(self, row: list[tuple]):
        self.assertEqual("800",row[0][0])
        self.assertEqual("42010AEE-1BB6-1EEC-92F1-E8D5E8096348",row[0][1])
        self.assertEqual("0508039989",row[0][2])
        self.assertEqual("EUR",row[0][11])
        self.assertEqual(Decimal('10591'),row[0][12])
        self.assertEqual(Decimal('8900'),row[0][13])
        self.assertEqual(Decimal('1691'),row[0][14])
    
    def verify_cds_data(self, tableName):
        firstRow = self.hanaClient.get_rows_by_offset(self.hanaSchema, tableName, 1, 0, "DI_SEQNO")
        self.check_cds_first_row_data(firstRow)
        lastRow = self.hanaClient.get_rows_by_offset(self.hanaSchema, tableName, 1, 0, "DI_SEQNO", True)
        self.check_cds_last_row_data(lastRow)
        rowCount = self.hanaClient.get_rowcount(self.hanaSchema, tableName)
        self.assertEqual(8000456, rowCount, "Please check the number of row count")
    
    def verify_slt_data(self, tableName):
        firstRow = self.hanaClient.get_rows_by_offset(self.hanaSchema, tableName, 1, 0, "DI_SEQNO")
        self.check_slt_first_row_data(firstRow)
        lastRow = self.hanaClient.get_rows_by_offset(self.hanaSchema, tableName, 1, 0, "DI_SEQNO", True)
        self.check_slt_last_row_data(lastRow)
        rowCount = self.hanaClient.get_rowcount(self.hanaSchema, tableName)
        self.assertEqual(8000100, rowCount, "Please check the number of row count")
    

    @classmethod
    def setUpClass(self):
        if self.cluster == None:
            app = App()
            connection_data = get_connection_data()
            self.cluster = Cluster.connect_to(connection_data)
        self.assertIsNotNone(self.cluster, 'Connection to cluster failed!')
        hanaConnectionData = get_hana_connection_data("HANA_EU10")
        if self.hanaClient == None:
            self.hanaClient = HanaClient.connect_to(hanaConnectionData)
        self.assertIsNotNone(self.hanaClient, 'Connection to Hana failed!')
        targetTableSuffix = app.getTableSuffix()
        now = datetime.now()
        timestampSuffix = str(now.year) + str(now.month) + str(now.day)
        self.targetTableNames = {
            "CDStoHanaInitTable": f"POD_PIPELINE_CDS_TO_HANA_INITIAL_{targetTableSuffix}_{timestampSuffix}",
            "CDStoHanaReplicationTable": f"POD_PIPELINE_CDS_TO_HANA_REPLICATION_{targetTableSuffix}_{timestampSuffix}",
            "CDStoKafkaInitTable": f"POD_PIPELINE_CDS_TO_KAFKA_INITIAL_{targetTableSuffix}_{timestampSuffix}",
            "CDStoKafkaReplicationTable": f"POD_PIPELINE_CDS_TO_KAFKA_REPLICATION_{targetTableSuffix}_{timestampSuffix}",
            "SLTtoHanaInitTable": f"POD_PIPELINE_SLT_TO_HANA_INITIAL_{targetTableSuffix}_{timestampSuffix}",
            "SLTtoHanaReplicationTable": f"POD_PIPELINE_SLT_TO_HANA_REPLICATION_{targetTableSuffix}_{timestampSuffix}",
            "SLTtoKafkaInitTable": f"POD_PIPELINE_SLT_TO_KAFKA_INITIAL_{targetTableSuffix}_{timestampSuffix}",
            "SLTtoKafkaReplicationTable": f"POD_PIPELINE_SLT_TO_KAFKA_REPLICATION_{targetTableSuffix}_{timestampSuffix}"
        }
        self.sltoperation = sltConfig()

    @classmethod
    def tearDownClass(self):
        self.hanaClient = None

    def test_start_CDStoHANA_Initial(self):
        RUN_ID = "test" + uuid.uuid4().hex
        configSubstitutions = {
            "ABAP_CDS_NAME": "Z_SEPM_I_SALESORDER",
            "ABAP_CONNECTION_ID": "S4H_2021_QOI_244",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "RUN_ID": RUN_ID,
            "TRANSFER_MODE": "Initial Load",
            "TARGET_TABLE_NAME": self.targetTableNames["CDStoHanaInitTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoHana", "CDS_ABAP_to_Hana_Initial", configSubstitutions)
    
    def test_start_CDStoHANA_Replication(self):
        RUN_ID = "test" + uuid.uuid4().hex
        configSubstitutions = {
            "ABAP_CDS_NAME": "Z_SEPM_I_SALESORDER",
            "ABAP_CONNECTION_ID": "S4H_2021_QOI_244",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "RUN_ID": RUN_ID,
            "TRANSFER_MODE": "Replication",
            "TARGET_TABLE_NAME": self.targetTableNames["CDStoHanaReplicationTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoHana", "CDS_ABAP_to_Hana_Replication", configSubstitutions)

    def test_start_CDStoKafka_Initial(self):
        RUN_ID = "test" + uuid.uuid4().hex
        configSubstitutions = {
            "ABAP_CDS_NAME": "Z_SEPM_I_SALESORDER",
            "ABAP_CONNECTION_ID": "S4H_2021_QOI_244",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "TRANSFER_MODE": "Initial Load",
            "TARGET_TABLE_NAME": self.targetTableNames["CDStoKafkaInitTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_KafkaToHana", "CDS_Kafka_to_Hana_Initial", configSubstitutions)
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoKafka", "CDS_ABAP_to_Kafka_Initial", configSubstitutions)
    
    def test_start_CDStoKafka_Replication(self):
        RUN_ID = "test" + uuid.uuid4().hex
        configSubstitutions = {
            "ABAP_CDS_NAME": "Z_SEPM_I_SALESORDER",
            "ABAP_CONNECTION_ID": "S4H_2021_QOI_244",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "TRANSFER_MODE": "Replication",
            "TARGET_TABLE_NAME": self.targetTableNames["CDStoKafkaReplicationTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_KafkaToHana", "CDS_Kafka_to_Hana_Replication", configSubstitutions)
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoKafka", "CDS_ABAP_to_Kafka_Replication", configSubstitutions)

    def test_start_SLTtoHANA_Initial(self):
        RUN_ID = "test" + uuid.uuid4().hex
        MT_ID, _ = self.sltoperation.get_mass_transfer_id()
        configSubstitutions = {
            "SLT_TABLE_NAME": "SNWD_SO",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "MT_ID": MT_ID,
            "ABAP_CONNECTION_ID": "ECC_UK5_800",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "TRANSFER_MODE": "Initial Load",
            "TARGET_TABLE_NAME": self.targetTableNames["SLTtoHanaInitTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_SLTtoHana", "SLT_ABAP_to_HANA_Initial", configSubstitutions)

    def test_start_SLTtoHANA_Replication(self):
        RUN_ID = "test" + uuid.uuid4().hex
        MT_ID, _ = self.sltoperation.get_mass_transfer_id()
        configSubstitutions = {
            "SLT_TABLE_NAME": "SNWD_SO",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "MT_ID": MT_ID,
            "ABAP_CONNECTION_ID": "ECC_UK5_800",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "TRANSFER_MODE": "Replication",
            "TARGET_TABLE_NAME": self.targetTableNames["SLTtoHanaReplicationTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_SLTtoHana", "SLT_ABAP_to_HANA_Replication", configSubstitutions)

    def test_start_SLTtoKafka_Initial(self):
        RUN_ID = "test" + uuid.uuid4().hex
        MT_ID, _ = self.sltoperation.get_mass_transfer_id()
        configSubstitutions = {
            "SLT_TABLE_NAME": "SNWD_SO",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "MT_ID": MT_ID,
            "ABAP_CONNECTION_ID": "ECC_UK5_800",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "TRANSFER_MODE": "Initial Load",
            "TARGET_TABLE_NAME": self.targetTableNames["SLTtoKafkaInitTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_KafkaToHana", "SLT_Kafka_to_HANA_Initial", configSubstitutions)
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_SLTtoKafka", "SLT_ABAP_to_Kafka_Initial", configSubstitutions)
    
    def test_start_SLTtoKafka_Replication(self):
        RUN_ID = "test" + uuid.uuid4().hex
        MT_ID, _ = self.sltoperation.get_mass_transfer_id()
        configSubstitutions = {
            "SLT_TABLE_NAME": "SNWD_SO",
            "KAFKA_CONNECTION": "KAFKA",
            "RUN_ID": RUN_ID,
            "MT_ID": MT_ID,
            "ABAP_CONNECTION_ID": "ECC_UK5_800",
            "HANA_CONNECTION_ID": "HANA_CLOUD",
            "TRANSFER_MODE": "Replication",
            "TARGET_TABLE_NAME": self.targetTableNames["SLTtoKafkaReplicationTable"]
        }
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_KafkaToHana", "SLT_Kafka_to_HANA_Replication", configSubstitutions)
        self.cluster.modeler.graph.runGraph(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_SLTtoKafka", "SLT_ABAP_to_Kafka_Replication", configSubstitutions)
    # @unittest.skip("Currently we need to clarify how to check the result for cds_to_filestore scenario, check db or via metadata explorer?")
    # def test_start_CDStoFileStore_Initial(self):
    #     RUN_ID = "test" + uuid.uuid4().hex
    #     target_folder = f"/dataintegration2021/CIT_TEST/{RUN_ID}/"
    #     CSV_PATH = target_folder + "part-<counter>.csv"
    #     JSON_PATH= target_folder + "part-<counter>.json"
    #     configSubstitutions = {
    #             "ABAP_CDS_NAME": "Z_SEPM_I_SALESORDER",
    #             "ABAP_CONNECTION_ID": "S4H_2021_QOI_244",
    #             "FILE_CONNECTION": "ADL_V2",
    #             "JSON_PATH":JSON_PATH,
    #             "CSV_PATH": CSV_PATH,
    #             "RUN_ID": RUN_ID
    #     }
    #     self.cluster.modeler.graph.runGraph("test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoFileStore","CDSViewtoFileStore",configSubstitutions)
    
    def test_validation_CDStoKafka_Initial(self):
        tableName = self.targetTableNames["CDStoKafkaInitTable"]
        status_CDSViewtoKafka = self.cluster.modeler.graph.getStatusByName(
            "CDS_ABAP_to_Kafka_Initial")
        status_KafkaToHana = self.cluster.modeler.graph.getStatusByName(
            "CDS_Kafka_to_Hana_Initial")
        self.assertEqual("completed", status_CDSViewtoKafka,
                         "The status should be completed for cds view abap to kafka scenario")
        self.assertEqual("completed", status_KafkaToHana,
                         "The status should be completed for cds view kafka to hana scenario")
        self.verify_cds_data(tableName)

    def test_validation_CDStoKafka_Replication(self):
        tableName = self.targetTableNames["CDStoKafkaReplicationTable"]
        status_CDSViewtoKafka = self.cluster.modeler.graph.getStatusByName(
            "CDS_ABAP_to_Kafka_Replication")
        self.assertEqual("running", status_CDSViewtoKafka,
                         "The status should be running for cds view abap to kafka scenario with replication load")
        self.verify_cds_data(tableName)
    
    def test_validation_CDStoHANA_Initial(self):
        tableName = self.targetTableNames["CDStoHanaInitTable"]
        status_CDSViewtoHana = self.cluster.modeler.graph.getStatusByName(
            "CDS_ABAP_to_Hana_Initial")
        self.assertEqual("completed", status_CDSViewtoHana,
                         "The status should be completed for cds view abap to hana scenario with initial load")
        self.verify_cds_data(tableName)
    
    def test_validation_CDStoHANA_Replication(self):
        tableName = self.targetTableNames["CDStoHanaReplicationTable"]
        status_CDSViewtoHana = self.cluster.modeler.graph.getStatusByName(
            "CDS_ABAP_to_Hana_Replication")
        self.assertEqual("running", status_CDSViewtoHana,
                         "The status should be running for cds view to hana scenario with replication load")
        self.verify_cds_data(tableName)
        

    def test_validation_SLTtoHANA_Initial(self):
        tableName = self.targetTableNames["SLTtoHanaInitTable"]
        status_SLTtoHana = self.cluster.modeler.graph.getStatusByName(
            "SLT_ABAP_to_HANA_Initial")
        self.assertEqual("completed", status_SLTtoHana,
                         "The status should be completed for SLT to hana scenario with initial load")
        self.verify_slt_data(tableName)
        MTID = self.cluster.modeler.graph.getMTID()
        print(MTID)
        self.sltoperation.del_mass_transfer_id(MTID)
    
    def test_validation_SLTtoHANA_Replication(self):
        tableName = self.targetTableNames["SLTtoHanaReplicationTable"]
        status_SLTtoHana = self.cluster.modeler.graph.getStatusByName(
            "SLT_ABAP_to_HANA_Replication")
        self.assertEqual("running", status_SLTtoHana,
                         "The status should be running for SLT to hana replication scenario with replication load")
        self.verify_slt_data(tableName)
        MTID = self.cluster.modeler.graph.getMTID()
        print(MTID)
        self.sltoperation.del_mass_transfer_id(MTID)
        

    def test_validation_SLTtoKafka_Initial(self):
        tableName = self.targetTableNames["SLTtoKafkaInitTable"]
        status_KafkaToHana = self.cluster.modeler.graph.getStatusByName(
            "SLT_Kafka_to_HANA_Initial")
        status_SLTtoKafka = self.cluster.modeler.graph.getStatusByName(
            "SLT_ABAP_to_Kafka_Initial")
        self.assertEqual("completed", status_SLTtoKafka,
                         "The status should be completed for SLT to kafka scenario with initial load")
        self.assertEqual("completed", status_KafkaToHana,
                         "The status should be completed for SLT kafka to hana scenario with initial load")
        self.verify_slt_data(tableName)
        MTID = self.cluster.modeler.graph.getMTID()
        print(MTID)
        #{'massTransferId': '2EL'}
        self.sltoperation.del_mass_transfer_id(MTID)

    def test_validation_SLTtoKafka_Replication(self):
        tableName = self.targetTableNames["SLTtoKafkaReplicationTable"]
        status_SLTtoKafka = self.cluster.modeler.graph.getStatusByName(
            "SLT_ABAP_to_Kafka_Replication")
        self.assertEqual("running", status_SLTtoKafka,
                         "The status should be running for SLT to kafka scenario with replication load")
        self.verify_slt_data(tableName)
        MTID = self.cluster.modeler.graph.getMTID()
        print(MTID)
        #{'massTransferId': '2EL'}
        self.sltoperation.del_mass_transfer_id(MTID)
        

    @unittest.skip("The way to validate is not finalized yet")
    def test_validation_filestore(self):
        status = self.cluster.modeler.graph.getStatusByName(
            "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoFileStore")
        print(status)

    @unittest.skip("test basic functionality for MTID creation and deletion")
    def test_start_mtid_creation(self):
        sltopt = self.sltoperation
        mt_id, json_data_del_slt_config = sltopt.get_mass_transfer_id()
        print(mt_id)
        print(json_data_del_slt_config)
        sltopt.del_mass_transfer_id(json_data_del_slt_config)


if __name__ == '__main__':
    unittest.main()
