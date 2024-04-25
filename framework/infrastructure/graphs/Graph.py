import json


class Graph:
    def __init__(self, cluster) -> None:
        self._cluster = cluster
        self._id = ''
        self._handleID = ''
        self._status = ''

    def runGraph(self, graph_id: str, name: str, configSubstitutions={}, snapshotConfig={}) -> None:
        querypath = '/app/pipeline-modeler/service/v1/runtime/graphs'
        self._id = graph_id
        jsonData = {
            "src": self._id,
            "name": name,
            "traceLevel": "DEBUG"
        }
        jsonData["snapshotConfig"] = snapshotConfig
        jsonData["configurationSubstitutions"] = configSubstitutions
        payload = json.dumps(jsonData)
        response = self._cluster.apipost(querypath, payload)
        if response.status_code == 200:
            content = json.loads(response.text)
            self._handleID = content["handle"]
            self._status = content["status"]
        else:
            self._handleID = ''
            self._status = ''
            raise Exception(f"Fail to run the graph {self._id}")

    def getStatus(self) -> str:
        querypath = f"/app/pipeline-modeler/service/v1/runtime/graphs/{self._handleID}"
        response = self._cluster.apiget(querypath)
        if response.status_code == 200:
            content = json.loads(response.text)
            self._status = content["status"]
            return self._status
        else:
            self._status = ''
            raise Exception(f"Fail to get the graph status {self._id}")

    def getStatusByName(self, name: str) -> str:
        querypath = f"/app/pipeline-modeler/service/v1/runtime/graphsquery"
        jsonData = {"filter": ["equal", "parent", ""], "detailLevel": "graph"}
        payload = json.dumps(jsonData)
        response = self._cluster.apipost(querypath, payload)
        if response.status_code == 200:
            contents = json.loads(response.text)
            for item in contents:
                if item["name"] == name:
                    self._handleID = item["handle"]
                    self._status = item["status"]
                    break
            return self._status
        else:
            self._status = ''
            raise Exception("Fail to get all graph")

    def getMTID(self) -> dict:
        querypath = f"/app/pipeline-modeler/service/v1/runtime/graphs/{self._handleID}"
        response = self._cluster.apiget(querypath)
        if response.status_code == 200:
            contents = json.loads(response.text)
            #{'massTransferId': '2EL'}
            MTIDDict = {}
            MTIDDict["massTransferId"] = contents["configurationSubstitutions"]["MT_ID"]
            return MTIDDict
        else:
            raise Exception(f"Graph with handle {self._handleID} not found")
