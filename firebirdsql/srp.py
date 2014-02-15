##############################################################################
# Copyright (c) 2014 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
# This SRP implementation is in reference to
'''
Following document was copied from <http://srp.stanford.edu/design.html>.
-----
SRP Protocol Design

SRP is the newest addition to a new class of strong authentication protocols that resist all the well-known passive and active attacks over the network. SRP borrows some elements from other key-exchange and identification protcols and adds some subtlee
 modifications and refinements. The result is a protocol that preserves the strength and efficiency of the EKE family protocols while fixing some of their shortcomings. 

The following is a description of SRP-6 and 6a, the latest versions of SRP: 

  N    A large safe prime (N = 2q+1, where q is prime)
       All arithmetic is done modulo N.
  g    A generator modulo N
  k    Multiplier parameter (k = H(N, g) in SRP-6a, k = 3 for legacy SRP-6)
  s    User's salt
  I    Username
  p    Cleartext Password
  H()  One-way hash function
  ^    (Modular) Exponentiation
  u    Random scrambling parameter
  a,b  Secret ephemeral values
  A,B  Public ephemeral values
  x    Private key (derived from p and s)
  v    Password verifier

The host stores passwords using the following formula: 

  x = H(s, p)               (s is chosen randomly)
  v = g^x                   (computes password verifier)

The host then keeps {I, s, v} in its password database. The authentication protocol itself goes as follows: 

User -> Host:  I, A = g^a                  (identifies self, a = random number)
Host -> User:  s, B = kv + g^b             (sends salt, b = random number)

        Both:  u = H(A, B)

        User:  x = H(s, p)                 (user enters password)
        User:  S = (B - kg^x) ^ (a + ux)   (computes session key)
        User:  K = H(S)

        Host:  S = (Av^u) ^ b              (computes session key)
        Host:  K = H(S)

Now the two parties have a shared, strong session key K. To complete authentication, they need to prove to each other that their keys match. One possible way: 

User -> Host:  M = H(H(N) xor H(g), H(I), s, A, B, K)
Host -> User:  H(A, M, K)

The two parties also employ the following safeguards: 

  1. The user will abort if he receives B == 0 (mod N) or u == 0. 
  2. The host will abort if it detects that A == 0 (mod N). 
  3. The user must show his proof of K first. If the server detects that the user's proof is incorrect, it must abort without showing its own proof of K. 

See http://srp.stanford.edu/ for more information.
'''
from __future__ import print_function
import sys
import hashlib
import hmac
import random
import binascii

DEBUG=False
DEBUG_PRINT=False
if DEBUG:
    DEBUG_PRIVATE_KEY = 0x60975527035CF2AD1989806F0407210BC81EDC04E2762A56AFD529DDDA2D4393

PYTHON_MAJOR_VER = sys.version_info[0]

if PYTHON_MAJOR_VER == 3:
    def ord(c):
        return c

SRP_KEY_SIZE = 128
SRP_SALT_SIZE = 32

def get_prime():
    N = 0xE67D2E994B2F900C3F41F08F5BB2627ED0D49EE1FE767A52EFCD565CD6E768812C3E1E9CE8F0A8BEA6CB13CD29DDEBF7A96D4A93B55D488DF099A15C89DCB0640738EB2CBDD9A8F7BAB561AB1B0DC1C6CDABF303264A08D1BCA932D1F1EE428B619D970F342ABA9A65793B8B2F041AE5364350C16F735F56ECBCA87BD57B29E7
    g = 2

    #n = N
    #scale = 0
    #while n > 0:
    #  scale += 1
    #  n >>= 8
    scale = 128

    #k = bytes2long(sha1(pad(N, scale), pad(g, scale)))
    k = 1277432915985975349439481660349303019122249719989

    return N, g, scale, k


def bytes2long(s):
    n = 0
    for c in s:
        n <<= 8
        n += ord(c)
    return n

def long2bytes(n):
    s = []
    while n > 0:
      s.insert(0, n & 255)
      n >>= 8
    if PYTHON_MAJOR_VER == 3:
        return bytes(s)
    else:
        return b''.join([chr(c) for c in s])

def sha1(*args):
    sha1 = hashlib.sha1()
    for v in args:
        if not isinstance(v, bytes):
            v = long2bytes(v)
        sha1.update(v)
    return sha1.digest()

def pad(n, scale):
    s = []
    for x in range(scale):
        s.insert(0, n & 255)
        n >>= 8
    if PYTHON_MAJOR_VER == 3:
        return bytes(s)
    else:
        return b''.join([chr(c) for c in s])

def get_scramble(x, y, scale):
    return bytes2long(sha1(pad(x, scale), pad(y, scale)))

def getUserHash(salt, user, password):
    assert isinstance(user, bytes)
    assert isinstance(password, bytes)
    hash1 = sha1(user, b':', password)
    hash2 = sha1(salt, hash1)
    rc =  bytes2long(hash2)
    if DEBUG_PRINT:
        print('user=', user)
        print('password=',  password)
        print('salt=', salt, end='\n')
        print('hash1=', binascii.b2a_hex(hash1), end='\n')
        print('hash2=', binascii.b2a_hex(hash2), end='\n')
        print('rc=', binascii.b2a_hex(long2bytes(rc)), end='\n')

    return rc

def client_seed(user, password):
    """
        A: Client public key
        a: Client private key
    """
    N, g, scale, k  = get_prime()
    a = random.randrange(0, 1 << SRP_KEY_SIZE)
    A = pow(g, a, N)
    if DEBUG:
        a = DEBUG_PRIVATE_KEY
        A = pow(g, a, N)
    if DEBUG_PRINT:
        print('A=', binascii.b2a_hex(long2bytes(A)), end='\n')
        print('a=', binascii.b2a_hex(long2bytes(a)), end='\n')
    return A, a

def server_seed(v):
    """
        B: Server public key
        b: Server private key
    """
    N, g, scale, k = get_prime()
    b = random.randrange(0, 1 << SRP_KEY_SIZE)
    gb = pow(g, b, N)
    kv = (k * v) % N
    B = (kv + gb) % N
    if DEBUG:
        b = DEBUG_PRIVATE_KEY
        gb = pow(g, b, N)
        kv = (k * v) % N
        B = (kv + gb) % N
    if DEBUG_PRINT:
        print('B=', binascii.b2a_hex(long2bytes(B)), end='\n')
        print('b=', binascii.b2a_hex(long2bytes(b)), end='\n')
    return B, b

def client_session(user, password, salt, A, B, a):
    """
    Client session secret
        Both:  u = H(A, B)

        User:  x = H(s, p)                 (user enters password)
        User:  S = (B - kg^x) ^ (a + ux)   (computes session key)
        User:  K = H(S)
    """
    N, g, scale, k = get_prime()
    u = get_scramble(A, B, scale)
    x = getUserHash(salt, user, password)   # x
    gx = pow(g, x, N)                       # g^x
    kgx = (k * gx) % N                      # kg^x
    diff = (B - kgx) % N                    # B - kg^x
    ux = (u * x) % N
    aux = (a + ux) % N
    session_secret = pow(diff, aux, N)      # (B - kg^x) ^ (a + ux)
    K = sha1(session_secret)
    if DEBUG_PRINT:
        print('\tclient_settion() salt=', salt, end='\n')
        print('\tclient_settion() B=',
            binascii.b2a_hex(long2bytes(B)), end='\n')
        print('\tclient_session() scramble=',
            binascii.b2a_hex(long2bytes(u)), end='\n')
        print('\tclient_session() x=',
            binascii.b2a_hex(long2bytes(x)), end='\n')
        print('\tclient_session() gx=',
            binascii.b2a_hex(long2bytes(gx)), end='\n')
        print('\tclient_session() kgx=',
            binascii.b2a_hex(long2bytes(kgx)), end='\n')
        print('\tclient_session() ux=',
            binascii.b2a_hex(long2bytes(ux)), end='\n')
        print('\tclient_session() aux=',
            binascii.b2a_hex(long2bytes(aux)), end='\n')
        print('\tclient_session() session_secret=',
            binascii.b2a_hex(long2bytes(session_secret)), end='\n')
        print('\tclient_session() K=', binascii.b2a_hex(K))

    return K

def server_session(user, password, salt, A, B, b):
    """
    Server session secret
        Both:  u = H(A, B)

        Host:  S = (Av^u) ^ b              (computes session key)
        Host:  K = H(S)
    """
    N, g, scale, k = get_prime()
    u = get_scramble(A, B, scale)
    v = get_verifier(user, password, salt)
    vu = pow(v, u, N)                       # v^u
    Avu = (A * vu) % N                      # Av^u
    session_secret = pow(Avu, b, N)         # (Av^u) ^ b
    K = sha1(session_secret)
    if DEBUG_PRINT:
        print('server session_secret=',
            binascii.b2a_hex(long2bytes(session_secret)), end='\n')
        print('server session hash K=', binascii.b2a_hex(K))

    return K

def client_proof(user, password, salt, A, B, a):
    """
    M = H(H(N) xor H(g), H(I), s, A, B, K)
    """
    N, g, scale, k = get_prime()
    K = client_session(user, password, salt, A, B, a)

    n1 = bytes2long(sha1(N))
    n2 = bytes2long(sha1(g))
    n1 = pow(n1, n2, N)
    n2 = bytes2long(sha1(user))
    M = sha1(n1, n2, salt, A, B, K)
    if DEBUG_PRINT:
        print('client_proof:M=', binascii.b2a_hex(M), end='\n')
        print('client_proof:K=', binascii.b2a_hex(K), end='\n')

    return M, K

def get_salt():
    if DEBUG:
        return long2bytes(0x2E268803000000079A478A700000002D1A6979000000026E1601C000000054F)
    if PYTHON_MAJOR_VER == 3:
        return bytes([random.randrange(0, 256) for x in range(SRP_SALT_SIZE)])
    else:
        return b''.join([chr(random.randrange(0, 256)) for x in range(SRP_SALT_SIZE)])

def get_verifier(user, password, salt):
    N, g, scale, k = get_prime()
    x = getUserHash(salt, user, password)
    return pow(g, x, N)

if __name__ == '__main__':
    """
    A, a, B, b are long.
    salt, M are bytes.
    """
    # Both
    user = b'SYSDBA'
    password = b'masterkey'

    # Client send A to Server
    A, a = client_seed(user, password)

    # Server send B, salt to Client
    salt = get_salt()
    v = get_verifier(user, password, salt)
    B, b = server_seed(v)

    serverKey = server_session(user, password, salt, A, B, b)

    # Client send M to Server
    M, clientKey = client_proof(user, password, salt, A, B, a)

    # Client and Server has same key
    assert clientKey == serverKey

