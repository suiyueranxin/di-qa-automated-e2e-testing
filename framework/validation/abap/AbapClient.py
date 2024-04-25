from __future__ import annotations
from typing import TYPE_CHECKING

from pyrfc import Connection

from framework.infrastructure.utils.ConnectionDataBase import ConnectionDataBase


class AbapClient:
    """A class for call RFC function modules on ABAP systems."""

    def __init__(self) -> None:
        self._connection = None

    @staticmethod
    def connect_to(connection_data: AbapConnectionData = None, **kwargs) -> AbapClient:
        abapclient = AbapClient()

        if connection_data is None:
            abapclient._connection = Connection(
                user=kwargs['user'], passwd=kwargs['passwd'], ashost=kwargs['ashost'], sysnr=kwargs['sysnr'], client=kwargs['client'])
        else:
            filled_values = {}
            for key, value in connection_data._values.items():
                if value:
                    filled_values[key] = value
            abapclient._connection = Connection(**filled_values)

        return abapclient

    def call_rfc(self, function_module, **kwargs):
        rfcresult = self._connection.call(function_module, **kwargs)

        return rfcresult


class AbapConnectionData(ConnectionDataBase):

    property_names = ['user', 'password', 'client',
                      'sysnr', 'ashost', 'sysid', 'group', 'mshost']

    def __init__(self, name: str) -> None:
        self._values = {}
        self._values['name'] = name

    @property
    def name(self) -> str:
        return self._values['name']

    @property
    def user(self) -> str:
        return self._values.get('user', None)

    @user.setter
    def user(self, user: str) -> None:
        self._values['user'] = user

    @property
    def password(self) -> str:
        return self._values.get('passwd', None)

    @password.setter
    def password(self, password: str) -> None:
        self._values['passwd'] = password

    @property
    def ashost(self) -> str:
        return self._values.get('ashost', None)

    @ashost.setter
    def ashost(self, ashost: str) -> None:
        self._values['ashost'] = ashost

    @property
    def sysnr(self) -> str:
        return self._values.get('sysnr', None)

    @sysnr.setter
    def sysnr(self, sysnr) -> None:
        self._values['sysnr'] = str(sysnr).zfill(2)

    @property
    def client(self) -> str:
        return self._values.get('client', None)

    @client.setter
    def client(self, client) -> None:
        self._values['client'] = str(client).zfill(3)

    @property
    def mshost(self) -> str:
        return self._values.get('mshost', None)

    @mshost.setter
    def mshost(self, mshost: str) -> None:
        self._values['mshost'] = mshost

    @property
    def sysid(self) -> str:
        return self._values.get('sysid', None)

    @sysid.setter
    def sysid(self, sysid) -> None:
        self._values['sysid'] = sysid

    @property
    def group(self) -> str:
        return self._values.get('group', None)

    @group.setter
    def group(self, group) -> None:
        self._values['group'] = group

    def fill_properties(self, values_dict: dict, property_names=property_names) -> None:
        super().fill_properties(values_dict, property_names)
