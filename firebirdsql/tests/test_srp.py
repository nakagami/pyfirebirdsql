import binascii
import hashlib
import unittest
from firebirdsql.tests.base import *    # noqa
from firebirdsql import srp


class TestSrp(unittest.TestCase):
    def test_sha1(self):
        user = b'SYSDBA'
        password = b'masterkey'

        A, a = srp.client_seed(srp.DEBUG_PRIVATE_KEY)

        salt = srp.DEBUG_SALT
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v, srp.DEBUG_PRIVATE_KEY)

        serverKey = srp.server_session(user, password, salt, A, B, b)

        M, clientKey = srp.client_proof(user, password, salt, A, B, a, hashlib.sha1)
        self.assertEqual(clientKey, serverKey)
        self.assertEqual(M, binascii.unhexlify('8c12324bb6e9e683a3ee62e13905b95d69f028a9'))

    def test_sha256(self):
        user = b'SYSDBA'
        password = b'masterkey'

        A, a = srp.client_seed(srp.DEBUG_PRIVATE_KEY)

        salt = srp.DEBUG_SALT
        v = srp.get_verifier(user, password, salt)
        B, b = srp.server_seed(v, srp.DEBUG_PRIVATE_KEY)

        serverKey = srp.server_session(user, password, salt, A, B, b)

        M, clientKey = srp.client_proof(user, password, salt, A, B, a, hashlib.sha256)
        self.assertEqual(clientKey, serverKey)
        self.assertEqual(M, binascii.unhexlify('4675c18056c04b00cc2b991662324c22c6f08bb90beb3677416b03469a770308'))
