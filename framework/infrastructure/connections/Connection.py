class Connection:

    def __init__(self, connectionid: str) -> None:
        # TODO connectionid must match ^ [A-Za-z0-9_]+$
        # http://api.datahub.only.sap/master/bdh/datahub-app-base/src/apps/dh-app-connection/spec/swagger.yaml.bp.html#/definitions/ConnectionCreate
        self._values = {}
        self._values['id'] = connectionid
        self._values['description'] = ''
        self._values['type'] = None
        self._values['tags'] = []
        self._values['contentData'] = {}

    @property
    def id(self):
        return self._values['id']

    @property
    def description(self) -> str:
        return self._values['description']

    @description.setter
    def description(self, description: str):
        self._values['description'] = description

    @property
    def type(self) -> str:
        return self._values['type']

    @type.setter
    def type(self, connection_type: str):
        self._values['type'] = connection_type

    @property
    def ccmTypeId(self):
        return self._values['ccmTypeId']

    @ccmTypeId.setter
    def ccmTypeId(self, ccmtypeid):
        self._values['ccmTypeId'] = ccmtypeid

    @property
    def licenseRelevant(self) -> bool:
        return self._values['licenseRelevant']

    @licenseRelevant.setter
    def licenseRelevant(self, licenseRelevant: bool):
        self._values['licenseRelevant'] = licenseRelevant

    @property
    def gatewayId(self) -> str:
        return self._values['gatewayId']

    @gatewayId.setter
    def gatewayId(self, gatewayId: str) -> None:
        self._values['gatewayId'] = gatewayId

    @property
    def changedNote(self) -> str:
        return self._values['changedNote']

    @changedNote.setter
    def changedNote(self, changedNote: str) -> None:
        self._values['changedNote'] = changedNote

    @property
    def cloudConnectorLocationId(self) -> str:
        return self._values['cloudConnectorLocationId']

    @cloudConnectorLocationId.setter
    def cloudConnectorLocationId(self, cloudConnectorLocationId: str) -> None:
        self._values['cloudConnectorLocationId'] = cloudConnectorLocationId

    @property
    def owner(self) -> str:
        return self._values['owner']

    @owner.setter
    def owner(self, owner: str) -> None:
        self._values['owner'] = owner

    @property
    def readOnly(self) -> bool:
        return self._values['readOnly']

    @readOnly.setter
    def readOnly(self, readOnly: bool) -> None:
        self._values['readOnly'] = readOnly

    @property
    def tags(self):
        return self._values['tags']

    @tags.setter
    def tags(self, tags) -> None:
        self._values['tags'] = tags

    @property
    def contentData(self):
        return self._values['contentData']
