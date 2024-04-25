import unittest
import os


from framework.infrastructure.App import App


class testApp(unittest.TestCase):
    def test_basics(self):
        app = App()

    def test_getTestEnv_defaultValues(self):
        app = App()
        app.getTestEnv()
        self.assertEqual(
            app._baseUrl, 'https://vsystem.ingress.dh-9w8tyh8hqzm.di-dev2.shoot.canary.k8s-hana.ondemand.com')
        self.assertEqual(app._tenant, 'cit-tenant')
        self.assertEqual(app._user, '')
        self.assertEqual(app._password, '')

    def test_getTestEnv(self):
        app = App()
        os.environ["VSYSTEM_ENDPOINT"] = "https://vsystem.ingress.dh-pfx6eyiihnz.di-dev3.shoot.canary.k8s-hana.ondemand.com"
        os.environ["VORA_USERNAME"] = "admin"
        os.environ["VORA_PASSWORD"] = "Admin123"
        os.environ["VORA_TENANT"] = "default"
        app.getTestEnv()
        self.assertEqual(
            app._baseUrl, "https://vsystem.ingress.dh-pfx6eyiihnz.di-dev3.shoot.canary.k8s-hana.ondemand.com")
        self.assertEqual(app._tenant, 'default')
        self.assertEqual(app._user, 'admin')
        self.assertEqual(app._password, 'Admin123')
        del os.environ["VSYSTEM_ENDPOINT"]
        del os.environ["VORA_USERNAME"]
        del os.environ["VORA_PASSWORD"]
        del os.environ["VORA_TENANT"]

    def test_getTableSuffix(self):
        app = App()
        app.getTestEnv()
        self.assertEqual(app.getTableSuffix(), 'MASTER')
        os.environ["TABLE_SUFFIX"] = "rel2110"
        app.getTestEnv()
        self.assertEqual(app.getTableSuffix(), 'rel2110')
