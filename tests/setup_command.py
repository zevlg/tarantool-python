#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import setuptools

class test(setuptools.Command):
    user_options = []
    description = 'Run unit tests'

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        '''
        Find all tests in test/tarantool/ and run them
        '''
        tests = unittest.defaultTestLoader.discover('tests')
        test_runner = unittest.TextTestRunner(verbosity = 2)
        test_runner.run(tests)
