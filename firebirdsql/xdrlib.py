from firebirdsql import utils

class Packer(object):
    def __init__(self):
        self.buf = b''

    def pack_int(self, v):
        self.buf += utils.bint_to_bytes(v, 4)

    def pack_bytes(self, v):
        n = len(v)
        self.buf += utils.bint_to_bytes(n, 4)
        n = ((n+3)//4)*4
        self.buf += v + (n - len(v)) * b'\0'

    def get_buffer(self):
        return self.buf
