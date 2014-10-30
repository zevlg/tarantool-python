import os
import unittest

from test_request_15 import (
        Test_RequestInsert15,
        Test_RequestDelete15,
        Test_RequestSelect15,
        Test_RequestUpdate15)
from test_response_15 import (
        Test_Field15,
        Test_Response15)
from test_server_15 import Test_Server15

from test_schema_16 import Test_Schema16
from test_dml_16 import Test_Request16

test_cases = (
    # 1.6
    Test_Schema16,
    Test_Request16,
    # 1.5
    Test_RequestInsert15,
    Test_RequestDelete15,
    Test_RequestSelect15,
    Test_RequestUpdate15,
    Test_Field15,
    Test_Response15
)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for testc in test_cases:
        suite.addTests(loader.loadTestsFromTestCase(testc))
    return suite
