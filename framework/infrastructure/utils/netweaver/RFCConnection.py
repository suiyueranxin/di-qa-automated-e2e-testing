import os
from pyrfc import Connection


class RFCConnection:

    def __init__(self):
        self.conn = self._open_rfc_connection()

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(RFCConnection, "_instance"):
            RFCConnection._instance = RFCConnection(*args, **kwargs)
        return RFCConnection._instance

    def _open_rfc_connection(self):
        """Open RFC connection with valid logon credentials."""
        
        if all([os.getenv('ABAP_CONN_USER'),
                os.getenv('ABAP_CONN_PASSWD'),
                os.getenv('ABAP_CONN_ASHOST'),
                os.getenv('ABAP_CONN_SYSNR'),
                os.getenv('ABAP_CONN_CLIENT')]):
            self.conn = Connection(user=os.getenv('ABAP_CONN_USER'),
                                   passwd=os.getenv('ABAP_CONN_PASSWD'),
                                   ashost=os.getenv('ABAP_CONN_ASHOST'),
                                   sysnr=os.getenv('ABAP_CONN_SYSNR'),
                                   client=os.getenv('ABAP_CONN_CLIENT'))
            print("create connection successful")
        else:
            raise ValueError("The ABAP connection don't allowed None value")

        return self.conn
