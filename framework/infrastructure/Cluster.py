import requests
import json
from urllib.parse import urlsplit, urlunsplit
from framework.infrastructure.ConnectionManagement import ConnectionManagement

from framework.infrastructure.Modeler import Modeler
from framework.infrastructure.Monitoring import Monitoring
from framework.infrastructure.Repository import Repositoy
from framework.infrastructure.Rms import Rms
from framework.infrastructure.utils.ConnectionDataBase import ConnectionDataBase


class Cluster:
    def __init__(self): pass

    def __init__(self, cluster_connection_data):
        self._connectiondata = cluster_connection_data
        self._urls = ClusterUrls(cluster_connection_data.baseurl)
        self._headers = ClusterHeaders()

        self._modeler = Modeler(self)
        self._repository = Repositoy(self)
        self._connectionmanagement = ConnectionManagement(self)
        self._rms = Rms(self)
        self._monitoring = Monitoring(self)

        self.session = requests.session()

        self._loggedin = False

    @property
    def modeler(self) -> Modeler:
        """A reference to the Modeler"""

        return self._modeler

    @property
    def repository(self) -> Repositoy:
        """A reference to the Repository"""

        return self._repository

    @property
    def connectionmanagement(self) -> ConnectionManagement:
        return self._connectionmanagement

    @property
    def rms(self) -> Rms:
        return self._rms

    @property
    def monitoring(self) -> Monitoring:
        return self._monitoring

    @staticmethod
    def connect_to(cluster_connection_data):
        cluster = Cluster(cluster_connection_data)
        if cluster.login():
            return cluster

        return None

    def login(self):
        authBody = {
            "username": self._connectiondata.user,
            "password": self._connectiondata.password,
            "tenant": self._connectiondata.tenant
        }

        try:
            authResp = self.session.post(self._urls.login, data=json.dumps(
                authBody), headers=self._headers.di_header)
        except Exception as e:
            print(
                "[bold red]Error connecting to the Data Intelligence cluster:[/bold red]")
            print(self._urls.base)
            print("[bold red]" + str(e) + "[/bold red]")
            return False

        if authResp.status_code != 200:
            print(
                "[bold red]Error connecting to the Data Intelligence cluster:[/bold red]")
            return False

        self._loggedin = True
        return True

    def apiget(self, path):
        return self.session.get(self._urls.base + path)

    def apipost(self, path, data):
        return self.session.post(self._urls.base + path, data=data, headers=self._headers.di_header)

    def apidelete(self, path):
        return self.session.delete(self._urls.base + path, headers=self._headers.di_header)

    def apiput(self, path, data):
        return self.session.put(self._urls.base + path, data=data, headers=self._headers.di_header)


class ClusterConnectionData(ConnectionDataBase):
    """Holds the data that is needed to establish a connection to a DI cluster"""

    property_names = ['baseurl', 'user', 'password', 'tenant']

    def __init__(self, name, base_url=None):
        """Creates a new instance of ClusterConnectionData with the given name
        and baseurl.

        The baseurl parameter is checked and all path information that maybe 
        contained will be stripped."""

        self._name = name
        self.baseurl = base_url

    def _validate_baseurl(self, base_url):
        """Validates the given base URL by performing some basic sanity checks."""

        parsedurl = urlsplit(base_url)

        scheme = parsedurl.scheme
        hostname = parsedurl.hostname
        netloc = parsedurl.netloc.strip('/')
        port = parsedurl.port

        if 'https' != scheme:
            raise ValueError(
                'Invalid scheme in baseurl! Only https is allowed.')

        if hostname is None or '' == hostname.strip():
            raise ValueError('No hostname specified in baseurl!')

        # The following check should not be necessary: according the online
        # documentation of urllib.parse.urlsplit should already raise a
        # ValueError if the port is not numeric. But for some reason it does
        # not, hence this code here.
        if port is not None and not isinstance(port, int):
            raise ValueError('Port specified in baseurl is not an integer!')

        # The empty parameters strip off the path, query and fragment data
        # that may have been given.
        return urlunsplit([scheme, netloc, '', '', ''])

    @property
    def tenant(self) -> str:
        return self._tenant

    @tenant.setter
    def tenant(self, tenant: str) -> None:
        self._tenant = tenant

    @property
    def user(self) -> str:
        return self._user

    @user.setter
    def user(self, user: str) -> None:
        self._user = user

    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, password: str) -> None:
        self._password = password

    @property
    def baseurl(self):
        return self._baseurl

    @baseurl.setter
    def baseurl(self, base_url) -> None:
        if base_url is not None:
            self._baseurl = self._validate_baseurl(base_url)
        else:
            self._baseurl = None

    def fill_properties(self, values_dict: dict, property_names=property_names) -> None:
        super().fill_properties(values_dict, property_names)


class ClusterUrls:
    def __init__(self, base_url):
        self._base = base_url
        self._login = base_url + '/api/login/v2/finalize'

    @property
    def base(self):
        return self._base

    @property
    def login(self):
        return self._login


class ClusterHeaders:
    def __init__(self):
        self._di_header = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'Fetch'
        }

    @property
    def di_header(self):
        return self._di_header
