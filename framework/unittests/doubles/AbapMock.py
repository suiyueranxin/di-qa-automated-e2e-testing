class AbapConnectionMock:
    def __init__(self) -> None:
        self._rfcresult = None
        self._lastrfcall = None

    @property
    def last_rfc_call(self) -> list:
        return self._lastrfcall

    def set_rfc_result(self, rfcresult):
        self._rfcresult = rfcresult

    def call(self, functionmodule: str, **parameters):
        self._lastrfcall = [functionmodule, parameters]
        return self._rfcresult
