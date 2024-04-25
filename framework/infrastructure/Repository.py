from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from framework.infrastructure.Cluster import Cluster

class Repositoy: 

    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster

    def get_status(self, spacetype, path):
        # http://api.datahub.only.sap/master/velocity/vsystem/open-api/vrep-public-api.yaml.bp.html#operation--repository-v2-files--spaceType---path--op-stat-get
        querypath = '/repository/v2/files/' + spacetype + '/' + path + '?op=stat'
        return self._cluster.apiget(querypath)

    def exists(self, spacetype, path) -> bool:
        return self.get_status(spacetype,path).status_code == 200

    def write(self, spacetype, path, content):
        querypath = '/repository/v2/files/' + spacetype + '/' + path + '?op=write'
        return self._cluster.apipost(querypath, content)

    def remove(self, spacetype, path):
        querypath = '/repository/v2/files/' + spacetype + '/' + path + '?op=remove'
        return self._cluster.apidelete(querypath)

    def read(self, spacetype, path):
        querypath = '/repository/v2/files/' + spacetype + '/' + path + '?op=read'
        return self._cluster.apiget(querypath)

