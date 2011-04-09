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
import firebirdsql

HOST = 'localhost'
USER = 'sysdba'
PASS = 'masterkey'

def print_usage():
    print(sys.argv[0] + 
                ' start|stop <trc_id>|suspend <trc_id>|resume <trc_id>|list')

if len(sys.argv) < 2:
    print_usage()
    sys.exit(0)

svc = firebirdsql.service_mgr(host=HOST, user=USER, password=PASS)
if sys.argv[1] == 'start':
    cfg = open('/opt/firebird/fbtrace.conf').read()
    svc.trace_start(cfg=cfg, file=sys.stdout)
elif sys.argv[1] == 'stop':
    svc.trace_stop(id=sys.argv[2], file=sys.stdout)
elif sys.argv[1] == 'suspend':
    svc.trace_suspend(id=sys.argv[2], file=sys.stdout)
elif sys.argv[1] == 'resume':
    svc.trace_resume(id=sys.argv[2], file=sys.stdout)
elif sys.argv[1] == 'list':
    svc.trace_list()
svc.close()
