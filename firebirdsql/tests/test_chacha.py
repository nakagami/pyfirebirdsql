import unittest
from firebirdsql import utils
from firebirdsql.chacha import ChaCha20


class TestChaCha(unittest.TestCase):
    def test_chacha20(self):
        key = utils.hex_to_bytes("23AD52B15FA7EBDC4672D72289253D95DC9A4324FC369F593FDCC7733AD77617")
        nonce = utils.hex_to_bytes("5A5F6C13C1F12653")
        enc = utils.hex_to_bytes("6bd00ba222523f58de196fb471eea08d9fff95b5bbe6123dd3a8b9026ac0fa84")
        chacha = ChaCha20(key, nonce)
        self.assertEqual(chacha.translate(enc), b'TMCTF{Whose_garden_is_internet?}')

        chacha1 = ChaCha20(key, nonce, 123)
        enc = chacha1.translate(b'plain text')
        self.assertEqual(enc, utils.hex_to_bytes("39df7fdfcdd66c56e762"))
        chacha2 = ChaCha20(key, nonce, 123)
        plain = chacha2.translate(enc)
        self.assertEqual(plain, b'plain text')
