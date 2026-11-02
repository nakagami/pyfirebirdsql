from firebirdsql.tests.base import TestBase
import firebirdsql
import unittest
import zlib
from unittest.mock import MagicMock, patch
from firebirdsql.stream import SocketStream
from firebirdsql.consts import pflag_compress, ptype_MASK, ptype_lazy_send


class TestCompressionStream(unittest.TestCase):
    """Test zlib compression/decompression in SocketStream."""

    def test_roundtrip_compression(self):
        """Test that data compressed by compressor can be decompressed by decompressor."""
        compressor = zlib.compressobj()
        decompressor = zlib.decompressobj()

        original = b'Hello, Firebird wire protocol compression!'
        compressed = compressor.compress(original) + compressor.flush(zlib.Z_SYNC_FLUSH)
        decompressed = decompressor.decompress(compressed)
        self.assertEqual(decompressed, original)

    def test_streaming_compression(self):
        """Test that streaming compression works across multiple messages."""
        compressor = zlib.compressobj()
        decompressor = zlib.decompressobj()

        messages = [b'first message', b'second message', b'third message with more data']
        recovered = []
        for msg in messages:
            compressed = compressor.compress(msg) + compressor.flush(zlib.Z_SYNC_FLUSH)
            decompressed = decompressor.decompress(compressed)
            recovered.append(decompressed)

        self.assertEqual(recovered, messages)

    def test_compression_with_encryption(self):
        """Test that compression + encryption layering works correctly.
        Send order: compress -> encrypt. Receive order: decrypt -> decompress."""
        from firebirdsql.arc4 import ARC4

        compressor = zlib.compressobj()
        decompressor = zlib.decompressobj()
        enc = ARC4.new(b'test_key')
        dec = ARC4.new(b'test_key')

        original = b'Test data for compress+encrypt round-trip'
        # Compress then encrypt
        compressed = compressor.compress(original) + compressor.flush(zlib.Z_SYNC_FLUSH)
        encrypted = enc.translate(compressed)
        # Decrypt then decompress
        decrypted = dec.translate(encrypted)
        decompressed = decompressor.decompress(decrypted)
        self.assertEqual(decompressed, original)

    def test_pflag_compress_detection(self):
        """Test that pflag_compress flag is correctly detected and stripped."""
        accept_type = ptype_lazy_send | pflag_compress  # 5 | 0x100 = 0x105
        self.assertTrue(accept_type & pflag_compress)
        stripped = accept_type & ptype_MASK
        self.assertEqual(stripped, ptype_lazy_send)

    def test_pflag_compress_not_set(self):
        """Test that missing pflag_compress is correctly detected."""
        accept_type = ptype_lazy_send  # 5
        self.assertFalse(accept_type & pflag_compress)

    def test_enable_compression_sets_compressor(self):
        """Test that enable_compression initializes zlib objects."""
        with patch('socket.create_connection') as mock_conn:
            mock_sock = MagicMock()
            mock_conn.return_value = mock_sock
            stream = SocketStream('localhost', 3050)
            self.assertIsNone(stream._compressor)
            self.assertIsNone(stream._decompressor)
            stream.enable_compression()
            self.assertIsNotNone(stream._compressor)
            self.assertIsNotNone(stream._decompressor)

    def test_large_data_compression(self):
        """Test compression with larger data payloads."""
        compressor = zlib.compressobj()
        decompressor = zlib.decompressobj()

        # Simulate a large query result
        original = b'A' * 100000
        compressed = compressor.compress(original) + compressor.flush(zlib.Z_SYNC_FLUSH)
        # Compressed size should be significantly smaller for repetitive data
        self.assertLess(len(compressed), len(original))
        decompressed = decompressor.decompress(compressed)
        self.assertEqual(decompressed, original)


if __name__ == '__main__':
    unittest.main()


class TestWireCompression(TestBase):
    """Live connection with wire_compress=True (needs Firebird 3+)."""

    def _connect(self):
        return firebirdsql.connect(
            auth_plugin_name=self.auth_plugin_name,
            wire_crypt=self.wire_crypt,
            wire_compress=True,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            timeout=1)

    def test_compressed_connection(self):
        cur = self.connection.cursor()
        cur.execute("SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database")
        if int(cur.fetchone()[0].split('.')[0]) < 3:
            self.skipTest("wire compression needs Firebird 3+")
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE")
        self.assertEqual(cur.fetchone()[0], 'TRUE')
        # many rows: decompressed bytes wait in the stream's buffer between reads,
        # which must not be mistaken for a timeout
        cur.execute("SELECT RDB$FIELD_NAME, RDB$FIELD_TYPE FROM RDB$FIELDS")
        rows = cur.fetchall()
        self.assertGreater(len(rows), 100)
        cur.execute(
            "SELECT RDB$RELATION_NAME, RDB$FIELD_NAME, RDB$FIELD_SOURCE "
            "FROM RDB$RELATION_FIELDS ORDER BY 1, 2")
        self.assertGreater(len(cur.fetchall()), 100)
        conn.close()
