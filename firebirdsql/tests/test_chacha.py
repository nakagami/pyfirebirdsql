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

    def test_rfc7539(self):
        # RFC 7539 2.4.2: 12-byte nonce, 32-bit counter starting at 1
        key = bytes(range(32))
        nonce = utils.hex_to_bytes("000000000000004a00000000")
        plain = (b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip "
                 b"for the future, sunscreen would be it.")
        enc = utils.hex_to_bytes(
            "6e2e359a2568f98041ba0728dd0d6981e97e7aec1d4360c20a27afccfd9fae0b"
            "f91b65c5524733ab8f593dabcd62b3571639d624e65152ab8f530c359f0861d8"
            "07ca0dbf500d6a6156a38e088a22b65e52bc514d16ccf806818ce91ab7793736"
            "5af90bbf74a35be6b40b8eedf2785e42874d")
        self.assertEqual(ChaCha20(key, nonce, 1).translate(plain), enc)

    def test_chunks(self):
        # the key stream continues across calls of any size
        key = bytes(range(32))
        for nonce in (bytes(8), bytes(range(12))):
            data = bytes(i % 251 for i in range(20000))
            whole = ChaCha20(key, nonce).translate(data)
            chacha = ChaCha20(key, nonce)
            parts, pos = [], 0
            for size in (0, 1, 63, 64, 65, 7, 4096, 5000, 1, 10000):
                parts.append(chacha.translate(data[pos:pos + size]))
                pos += size
            self.assertEqual(b''.join(parts), whole[:pos])

    def test_64bit_counter(self):
        # ChaCha64 (8-byte nonce): the block counter carries into the next word
        key = bytes(range(32))
        nonce = bytes(range(8))
        start = 2 ** 32 - 2
        stream = ChaCha20(key, nonce, start).translate(bytes(64 * 4))
        for i in range(4):
            block = ChaCha20(key, nonce, start + i).translate(bytes(64))
            self.assertEqual(stream[64 * i:64 * (i + 1)], block)
        self.assertNotEqual(stream[128:192], ChaCha20(key, nonce, 0).translate(bytes(64)))
