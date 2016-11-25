#!/usr/bin/env python
##############################################################################
# Copyright (c) 2011-2016, Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#  this list of conditions and the following disclaimer in the documentation
#  and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Firebird RDBMS (http://www.firebirdsql.org/) proxy tool for debug.
##############################################################################
import sys
sys.path.append('./../')
from firebirdsql import services

HOST = 'localhost'
USER = 'sysdba'
PASS = 'masterkey'


def debug_print(msg):
    print(msg)


def print_usage():
    print(sys.argv[0] + ' start [name]|stop <trc_id>|suspend <trc_id>|resume <trc_id>|list')


if len(sys.argv) < 2:
    print_usage()
    sys.exit(0)

svc = services.connect(host=HOST, user=USER, password=PASS)
if sys.argv[1] == 'start':
    name = sys.argv[2] if len(sys.argv) == 3 else None
    cfg = open('/opt/firebird/fbtrace.conf').read()
    svc.trace_start(name=name, cfg=cfg, callback=debug_print)
elif sys.argv[1] == 'stop':
    svc.trace_stop(id=sys.argv[2], callback=debug_print)
elif sys.argv[1] == 'suspend':
    svc.trace_suspend(id=sys.argv[2], callback=debug_print)
elif sys.argv[1] == 'resume':
    svc.trace_resume(id=sys.argv[2], callback=debug_print)
elif sys.argv[1] == 'list':
    svc.trace_list(callback=debug_print)
svc.close()
