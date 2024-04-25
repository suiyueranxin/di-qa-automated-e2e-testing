import os
from framework.infrastructure.Cluster import Cluster, ClusterConnectionData


class App:
    def __init__(self) -> None:
        self._baseUrl = 'https://vsystem.ingress.dh-9w8tyh8hqzm.di-dev2.shoot.canary.k8s-hana.ondemand.com'
        self._tenant = 'cit-tenant'
        self._user = ''
        self._password = ''
        self._tableSuffix = 'MASTER'

    def getTestEnv(self):
        if 'VSYSTEM_ENDPOINT' in os.environ:
            self._baseUrl = str(os.environ.get('VSYSTEM_ENDPOINT')).strip()
        else:
            print("warning: Please set VSYSTEM_ENDPOINT env var")
        if 'VORA_USERNAME' in os.environ:
            self._user = str(os.environ.get('VORA_USERNAME')).strip()
        else:
            print("Warning: Please set VORA_USERNAME env var")
        if 'VORA_PASSWORD' in os.environ:
            self._password = str(os.environ.get('VORA_PASSWORD')).strip()
        else:
            print("Warning: Please set VORA_PASSWORD env var")
        if 'VORA_TENANT' in os.environ:
            self._tenant = str(os.environ.get('VORA_TENANT')).strip()
        else:
            print("Warning: Please set VORA_TENANT env var")
        if 'TABLE_SUFFIX' in os.environ:
            self._tableSuffix = str(os.environ.get('TABLE_SUFFIX')).strip()
        else:
            print("Warning: Please set TABLE_SUFFIX env var")

    def connectDI(self, name='Master') -> Cluster:
        self.getTestEnv()
        connectionData = ClusterConnectionData(
            name, self._baseUrl)
        connectionData.tenant = self._tenant
        connectionData.user = self._user
        connectionData.password = self._password
        return Cluster.connect_to(connectionData)

    def getTableSuffix(self) -> str:
        return self._tableSuffix
