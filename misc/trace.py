#!/usr/bin/env python
##############################################################################
# Copyright (c) 2011 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import os,sys
sys.path.append('./../')
from firebirdsql import services

HOST = 'localhost'
USER = 'sysdba'
PASS = 'masterkey'

def debug_print(msg):
    print(msg)

def print_usage():
    print(sys.argv[0] + 
        ' start [name]|stop <trc_id>|suspend <trc_id>|resume <trc_id>|list')

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
