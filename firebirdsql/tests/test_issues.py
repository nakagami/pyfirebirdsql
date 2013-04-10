import os
import sys
import firebirdsql
import unittest

from firebirdsql.tests.base import BaseTestCase

class TestIssues(BaseTestCase):
    def test_issue_39(self):
        """
        .description attribute should be None when .execute has not run yet
        """
        cur = self.connection.cursor()
        self.assertEqual(None, cur.description)


