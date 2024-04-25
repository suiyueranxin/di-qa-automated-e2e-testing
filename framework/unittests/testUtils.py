import os
import unittest
from pathlib import Path

from framework.infrastructure.Utils import ConnectionData


test_path = os.path.join(os.path.dirname(
    __file__), 'testdata', 'connectiondata')


class testConnectionData(unittest.TestCase):

    def test_from_file(self):
        connection_data = ConnectionData._from_file('DUMMY', test_path)
        self.assertIsNotNone(connection_data)
        self.assertEqual('DUMMY', connection_data['name'])

    def test_from_file_with_secrets(self):
        connection_data = ConnectionData._from_file('DUMMY', test_path)
        self.assertIsNotNone(connection_data)
        self.assertEqual('DUMMY', connection_data['name'])
        self.assertEqual('dummyUser', connection_data['user'])
        self.assertEqual('******', connection_data['password'])

    def test_for_cluster_file(self):
        cut = ConnectionData.for_cluster('dummyCluster', test_path)

        self.assertEqual('dummyCluster', cut._name)
        self.assertEqual('someDummyUser', cut.user)
        self.assertEqual('topSecretPassword', cut.password)
        self.assertEqual('default', cut.tenant)
        self.assertEqual(
            'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com', cut.baseurl)

    def test_for_cluster_environemt(self):
        os.environ['CLUSTERENVONLY_USER'] = 'username'
        os.environ['CLUSTERENVONLY_PASSWORD'] = 'secret'
        os.environ['CLUSTERENVONLY_TENANT'] = 'default'
        os.environ['CLUSTERENVONLY_BASEURL'] = 'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com'

        cut = ConnectionData.for_cluster('clusterEnvOnly')

        self.assertEqual('clusterEnvOnly', cut._name)
        self.assertEqual('username', cut.user)
        self.assertEqual('secret', cut.password)
        self.assertEqual('default', cut.tenant)
        self.assertEqual(
            'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com', cut.baseurl)

        os.environ.pop('CLUSTERENVONLY_USER')
        os.environ.pop('CLUSTERENVONLY_PASSWORD')
        os.environ.pop('CLUSTERENVONLY_TENANT')
        os.environ.pop('CLUSTERENVONLY_BASEURL')

    def test_for_cluster_jsonenvironment(self):
        os.environ['CLUSTERENVJSON'] = '{"name": "clusterEnvJson","tenant": "default","baseurl": "https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com","user": "someDummyUser","password": "topSecretPassword"}'

        cut = ConnectionData.for_cluster('clusterEnvJson')

        self.assertEqual('clusterEnvJson', cut._name)
        self.assertEqual('someDummyUser', cut.user)
        self.assertEqual('topSecretPassword', cut.password)
        self.assertEqual('default', cut.tenant)
        self.assertEqual(
            'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com', cut.baseurl)

        os.environ.pop('CLUSTERENVJSON')

    def test_for_cluster_file_and_environment(self):
        os.environ['CLUSTERWITHOUTSECRETS_USER'] = 'username'
        os.environ['CLUSTERWITHOUTSECRETS_PASSWORD'] = 'secret'

        cut = ConnectionData.for_cluster('clusterWithoutSecrets', test_path)

        self.assertEqual('clusterWithoutSecrets', cut._name)
        self.assertEqual('username', cut.user)
        self.assertEqual('secret', cut.password)
        self.assertEqual('default', cut.tenant)
        self.assertEqual(
            'https://vsystem.ingress.dh-k250epza0g0.di-dev2.shoot.canary.k8s-hana.ondemand.com', cut.baseurl)

        os.environ.pop('CLUSTERWITHOUTSECRETS_USER')
        os.environ.pop('CLUSTERWITHOUTSECRETS_PASSWORD')

    def test_for_abap_file(self):
        cut = ConnectionData.for_abap('dummyABAP', test_path)

        self.assertEqual('dummyABAP', cut.name)
        self.assertEqual('ANZEIGER', cut.user)
        self.assertEqual('topSecretPassword', cut.password)
        self.assertEqual('244', cut.client)
        self.assertEqual('00', cut.sysnr)
        self.assertEqual('ldciqoi.wdf.sap.corp', cut.ashost)

    def test_for_abap_environemt(self):
        os.environ['ABAPENVONLY_USER'] = 'ANZEIGER'
        os.environ['ABAPENVONLY_PASSWORD'] = 'topSecretPassword'
        os.environ['ABAPENVONLY_CLIENT'] = '244'
        os.environ['ABAPENVONLY_SYSNR'] = '0'
        os.environ['ABAPENVONLY_ASHOST'] = 'ldciqoi.wdf.sap.corp'

        cut = ConnectionData.for_abap('abapEnvOnly')

        self.assertEqual('abapEnvOnly', cut.name)
        self.assertEqual('ANZEIGER', cut.user)
        self.assertEqual('topSecretPassword', cut.password)
        self.assertEqual('244', cut.client)
        self.assertEqual('00', cut.sysnr)
        self.assertEqual('ldciqoi.wdf.sap.corp', cut.ashost)

        os.environ.pop('ABAPENVONLY_USER')
        os.environ.pop('ABAPENVONLY_PASSWORD')
        os.environ.pop('ABAPENVONLY_CLIENT')
        os.environ.pop('ABAPENVONLY_SYSNR')
        os.environ.pop('ABAPENVONLY_ASHOST')

    def test_for_abap_file_and_environment(self):
        os.environ['DUMMYABAP_USER'] = 'SUPERUSER'
        os.environ['DUMMYABAP_PASSWORD'] = '*****'

        cut = ConnectionData.for_abap('dummyABAP', test_path)

        self.assertEqual('dummyABAP', cut.name)
        self.assertEqual('SUPERUSER', cut.user)
        self.assertEqual('*****', cut.password)
        self.assertEqual('244', cut.client)
        self.assertEqual('00', cut.sysnr)
        self.assertEqual('ldciqoi.wdf.sap.corp', cut.ashost)

        os.environ.pop('DUMMYABAP_USER')
        os.environ.pop('DUMMYABAP_PASSWORD')

    def test_for_abap_jsonenvironment(self):
        os.environ['DUMMYABAP'] = '{"name": "dummyABAP", "ashost": "ldciqoi.wdf.sap.corp", "sysnr": 0, "client": "244", "user": "ANZEIGER", "password": "topSecretPassword"}'

        cut = ConnectionData.for_abap('dummyABAP', )

        self.assertEqual('dummyABAP', cut.name)
        self.assertEqual('ANZEIGER', cut.user)
        self.assertEqual('topSecretPassword', cut.password)
        self.assertEqual('244', cut.client)
        self.assertEqual('00', cut.sysnr)
        self.assertEqual('ldciqoi.wdf.sap.corp', cut.ashost)

        os.environ.pop('DUMMYABAP')

    def test_for_hana_file(self):
        cut = ConnectionData.for_hana('dummyHana', test_path)

        self.assertEqual('dummyHana', cut.name)
        self.assertEqual('testuser', cut.user)
        self.assertEqual('******', cut.password)
        self.assertEqual(443, cut.port)
        self.assertEqual('eu10.hana.com', cut.address)

    def test_for_hana_environemt(self):
        os.environ['HANAENVONLY_ADDRESS'] = 'eu10.hana.com'
        os.environ['HANAENVONLY_PORT'] = "443"
        os.environ['HANAENVONLY_USER'] = 'testuser'
        os.environ['HANAENVONLY_PASSWORD'] = '******'

        cut = ConnectionData.for_hana('hanaEnvOnly')

        self.assertEqual('hanaEnvOnly', cut.name)
        self.assertEqual('testuser', cut.user)
        self.assertEqual('******', cut.password)
        self.assertEqual('eu10.hana.com', cut.address)
        self.assertEqual("443", cut.port)

        os.environ.pop('HANAENVONLY_ADDRESS')
        os.environ.pop('HANAENVONLY_PORT')
        os.environ.pop('HANAENVONLY_USER')
        os.environ.pop('HANAENVONLY_PASSWORD')

    def test_for_hana_file_and_environment(self):
        os.environ['DUMMYHANA_USER'] = 'SUPERUSER'
        os.environ['DUMMYHANA_PASSWORD'] = '*****'

        cut = ConnectionData.for_hana('dummyHana', test_path)

        self.assertEqual('dummyHana', cut.name)
        self.assertEqual('SUPERUSER', cut.user)
        self.assertEqual('*****', cut.password)
        self.assertEqual('eu10.hana.com', cut.address)
        self.assertEqual(443, cut.port)

        os.environ.pop('DUMMYHANA_USER')
        os.environ.pop('DUMMYHANA_PASSWORD')

    def test_for_hana_jsonenvironment(self):
        os.environ['DUMMYHANA'] = '{"name": "dummyHana", "address": "eu10.hana.com", "port": 443, "user": "testuser", "password": "******"}'

        cut = ConnectionData.for_hana('dummyHana', )

        self.assertEqual('dummyHana', cut.name)
        self.assertEqual('eu10.hana.com', cut.address)
        self.assertEqual(443, cut.port)
        self.assertEqual('testuser', cut.user)
        self.assertEqual('******', cut.password)

        os.environ.pop('DUMMYHANA')

    def test_for_datalake_file(self):
        cut = ConnectionData.for_datalake('dummyDatalake', test_path)

        self.assertEqual('dummyAdl', cut.accountname)
        self.assertEqual('dataintegration2021', cut.container)
        self.assertEqual('******', cut.accountkey)

    def test_for_datalake_environemt(self):
        os.environ['ADLENVONLY_ACCOUNTNAME'] = 'dummyAdl'
        os.environ['ADLENVONLY_CONTAINER'] = "dataintegration2021"
        os.environ['ADLENVONLY_ACCOUNTKEY'] = 'env******'

        cut = ConnectionData.for_datalake('adlEnvOnly')

        self.assertEqual('dummyAdl', cut.accountname)
        self.assertEqual('dataintegration2021', cut.container)
        self.assertEqual('env******', cut.accountkey)

        os.environ.pop('ADLENVONLY_ACCOUNTNAME')
        os.environ.pop('ADLENVONLY_CONTAINER')
        os.environ.pop('ADLENVONLY_ACCOUNTKEY')

    def test_for_datalake_file_and_environment(self):
        os.environ['DUMMYDATALAKE_ACCOUNTKEY'] = 'env******'

        cut = ConnectionData.for_datalake('dummyDatalake', test_path)

        self.assertEqual('dummyAdl', cut.accountname)
        self.assertEqual('dataintegration2021', cut.container)
        self.assertEqual('env******', cut.accountkey)

        os.environ.pop('DUMMYDATALAKE_ACCOUNTKEY')

    def test_for_datalake_jsonenvironment(self):
        os.environ['DUMMYDATALAKE'] = '{"accountname": "dummyAdl", "container": "dataintegration2021", "accountkey": "******"}'

        cut = ConnectionData.for_datalake('dummyDatalake')

        self.assertEqual('dummyAdl', cut.accountname)
        self.assertEqual('dataintegration2021', cut.container)
        self.assertEqual('******', cut.accountkey)

        os.environ.pop('DUMMYDATALAKE')
