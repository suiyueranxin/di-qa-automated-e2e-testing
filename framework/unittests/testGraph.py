import unittest
import json
from framework.infrastructure.Cluster import Cluster, ClusterConnectionData
from framework.infrastructure.graphs.Graph import Graph
from framework.unittests.doubles.SessionMock import SessionMock


def getDummyConnectionData():
    connectionData = ClusterConnectionData('POD-INT', 'https://cluster')
    connectionData.tenant = 'default'
    connectionData.user = 'tester'
    connectionData.password = '********'
    return connectionData


def getDummyResponseForRunGraph():
    return """{"user":"cit-test","tenant":"cit-tenant","src":"test.cit.slt-to-hana.slt_reader_gen2_InitialLoad","name":"gen2_test","executionType":"stream","handle":"9c2cb4a651c44a93b66b0eb637256c48","status":"pending","message":"Graph is scheduled for execution","started":0,"running":0,"updated":1642663177,"stopped":0,"submitted":1642663175,"configurationSubstitutions":{"MTID":"73H","TABLENAME":"SNWD_SO"},"processedGraphEvents":0,"tags":null,"firstGraphEvent":{"group":"","action":""},"terminationRequested":false,"terminated":false,"busService":"9c2cb4a651c44a93b66b0eb637256c48","deployer":"vsystem","snapshotConfig":{"enabled":true,"periodSeconds":30},"traceLevel":"debug","autoRestartInfo":{"id":"37a65b341ccf4296855f7640cc90d82e","runOrder":0,"runType":"normal","maxRestartCount":0,"restartDelaySeconds":2,"disableRestartAfterThreshold":false,"resetTimeThreshold":"3m0s","currentFailureCount":0}}"""


def getDummyResponseForCheckStatus():
    return """{"user":"cit-test","tenant":"cit-tenant","src":"test.cit.abap.cds-to-kafka.automationPOC.Pipeline_CDSViewtoHana_withoutValidation","name":"test.cit.abap.cds-to-kafka.automationPOC.Pipeline_CDSViewtoHana_withoutValidation","executionType":"batch","handle":"bb08871c2d174d84b86633bdec5f4efa","status":"running","message":"Graph is scheduled for execution","started":0,"running":0,"updated":1639540072,"stopped":0,"submitted":1639540069,"allocations":[{"groupName":"default","groupDescription":"","subgraph":"default","container":"vflow-graph-bb08871c2d174d84b86633bdec5f4efa-t-4grnrgzt5gs45j99","containerIp":"","host":"ip-10-0-1-19.eu-central-1.compute.internal","status":"pending","reason":"","updated":1639540070,"message":"containers with incomplete status: [vsystem-iptables]: PodInitializing","restartCount":0,"image":"837618996276.dkr.ecr.eu-central-1.amazonaws.com/dh-9w8tyh8hqzm/vora/vflow-node-f95c0a5ff0cf4d2a93f838400600f09cfdd1acc0:2113.2.0-com.sap.sles.base-20211213-002634","processes":[],"restartPolicy":"never","restartController":""}],"configurationSubstitutions":{"ABAP_CDS_NAME":"ZCDS_SNWD_SO","ABAP_CONNECTION_ID":"S4H_2020","HANA_CONNECTION_ID":"HANA_CLOUD","KAFKA_CONNECTION":"KAFKA","RUN_ID":"teste582d142a0d64eafbe6984706a932a45","TARGET_TABLE_NAME":"teste582d142a0d64eafbe6984706a932a45"},"processedGraphEvents":3,"tags":null,"firstGraphEvent":{"group":"","action":"scheduling/started"},"terminationRequested":false,"terminated":false,"busService":"bb08871c2d174d84b86633bdec5f4efa","deployer":"vsystem","snapshotConfig":{},"traceLevel":"INFO","autoRestartInfo":{"id":"d92ff915d9ec43afbcce51e93796479a","runOrder":0,"runType":"normal","maxRestartCount":0,"restartDelaySeconds":2,"disableRestartAfterThreshold":false,"resetTimeThreshold":"3m0s","currentFailureCount":0}}"""


def getDummyResponseForCheckStatusByName():
    return """[{"user":"cit-test","tenant":"cit-tenant","src":"test.cit.abap.cds-to-kafka.automationPOC.Pipeline_sub_CDSViewtoHana","name":"test","executionType":"","handle":"625ce8596c134a3487f64f45ee8a37fe","status":"running","message":"Graph is running","started":1640662021,"running":1640662032,"updated":1640662032,"stopped":0,"submitted":1640662013,"processedGraphEvents":18,"tags":null,"firstGraphEvent":{"group":"","action":"scheduling/started"},"terminationRequested":false,"terminated":false,"busService":"625ce8596c134a3487f64f45ee8a37fe","deployer":"vsystem","snapshotConfig":{},"traceLevel":"INFO","autoRestartInfo":{"id":"cab4b554085f4a6fa16ede2783692ced","runOrder":0,"runType":"normal","maxRestartCount":0,"restartDelaySeconds":2,"disableRestartAfterThreshold":false,"resetTimeThreshold":"3m0s","currentFailureCount":0}}]"""


class testGraph(unittest.TestCase):
    cluster = None

    def setUp(self) -> None:
        if self.cluster == None:
            self.cluster = Cluster(getDummyConnectionData())

    def test_basics(self):
        graph = Graph(self.cluster)

    def test_runGraph(self):
        graph = Graph(self.cluster)
        content = getDummyResponseForRunGraph()
        self.cluster.session = SessionMock()
        self.cluster.session.setresponsecontent(200, content)
        graph.runGraph("test.cit.slt-to-hana.slt_reader_gen2_InitialLoad", "gen2_test",
                       {"MTID": "73H", "TABLENAME": "SNWD_SO"}, {"enabled": True, "periodSeconds": 30})
        self.assertEqual(
            "test.cit.slt-to-hana.slt_reader_gen2_InitialLoad", graph._id)
        self.assertEqual("pending", graph._status)
        self.assertEqual("9c2cb4a651c44a93b66b0eb637256c48", graph._handleID)
        self.cluster.session.setresponse(404)
        with self.assertRaises(Exception) as context:
            graph.runGraph(
                "test.cit.abap.cds-to-kafka.automationPOC.Pipeline_CDSViewtoHana_withoutValidation", "test")
        self.assertTrue(
            "Fail to run the graph test.cit.abap.cds-to-kafka.automationPOC.Pipeline_CDSViewtoHana_withoutValidation" in str(context.exception))
        self.assertEqual("", graph._status)
        self.assertEqual("", graph._handleID)

    def test_getStatus(self):
        graph = Graph(self.cluster)
        content = getDummyResponseForCheckStatus()
        self.cluster.session = SessionMock()
        self.cluster.session.setresponsecontent(200, content)
        graph._handleID = "8c2c9565afad4a418f715344099c0a79"
        status = graph.getStatus()
        self.assertEqual("running", status)
        self.cluster.session.setresponse(404)
        try:
            graph.getStatus()
        except:
            self.assertEqual("", graph._status)

    def test_getStatusByName(self):
        graph = Graph(self.cluster)
        content = getDummyResponseForCheckStatusByName()
        self.cluster.session = SessionMock()
        self.cluster.session.setresponsecontent(200, content)
        status = graph.getStatusByName("test")
        self.assertEqual(status, "running")
        self.cluster.session.setresponse(404)
        with self.assertRaises(Exception) as context:
            graph.getStatus()
        self.assertTrue(
            f"Fail to get the graph status " in str(context.exception))
        self.assertEqual('', graph._status)

    def test_getMTID(self):
        graph = Graph(self.cluster)
        graph._handleID = ""
        self.cluster.session = SessionMock()
        content = '{"configurationSubstitutions":{"MT_ID": "5BV"}}'
        self.cluster.session.setresponsecontent(200, content)
        MTID = graph.getMTID()
        self.assertEqual(MTID,  {'massTransferId': '5BV'})
        self.cluster.session.setresponse(404)
        with self.assertRaises(Exception) as context:
            graph.getMTID()
        self.assertTrue(
            f"Graph with handle {graph._handleID} not found" in str(context.exception))


if __name__ == '__main__':
    unittest.main()
