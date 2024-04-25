class HanaConnectionMock:
    def __init__(self) -> None:
        self._cursorMock = HanaCursorMock(self)
        self._lastsql = ''
        self._sql_result = None

    def cursor(self):
        return self._cursorMock

    def set_sql_result(self, sql_result):
        self._sql_result = sql_result

    @property
    def last_sql(self) -> str:
        return self._lastsql


class HanaCursorMock:
    def __init__(self, connection: HanaConnectionMock) -> None:
        self._connectionMock = connection

    def execute(self, sql: str):
        self._connectionMock._lastsql = sql

    def fetchall(self):
        return self._connectionMock._sql_result

    def close(self):
        pass
