import unittest
import sys
import xmlrunner
import os
#  Add the project's root directory into sys.path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


def get_suite(prefix, path, cuspattern):
    loader = unittest.defaultTestLoader
    loader.testMethodPrefix = prefix
    discover = loader.discover(path, pattern=cuspattern)
    suite = unittest.TestSuite()
    suite.addTest(discover)
    return suite


# prefix values: "test_runGraph"
# python runtestcases.py "test_validation" "tests" "testJobs*.py"
if __name__ == '__main__':
    argsLen = len(sys.argv)
    if argsLen < 2:
        raise Exception("please set testcases'prefix")
    prefix = sys.argv[1]
    if argsLen >= 3:
        path = sys.argv[2]
    else:
        path = "tests"
    if argsLen >= 4:
        pattern = sys.argv[3]
    else:
        pattern = "test*.py"
    if argsLen >= 5:
        output = sys.argv[4]
    else:
        output = "testresult"
    runner = xmlrunner.XMLTestRunner(output=output)
    ret = not runner.run(get_suite(prefix, path, pattern)).wasSuccessful()
    sys.exit(ret) 