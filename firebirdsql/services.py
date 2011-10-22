##############################################################################
# Copyright (c) 2009-2011 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Python DB-API 2.0 module for Firebird. 
##############################################################################
import sys, os, socket
import xdrlib, time, datetime, decimal, struct
from firebirdsql.consts import *
from firebirdsql.fbcore import *

class connect(BaseConnect):
    def __init__(self, dsn=None, user=None, password=None, host=None,
            database=None, charset=DEFAULT_CHARSET, port=3050):
        BaseConnect.__init__(self, dsn=dsn, user=user, password=password,
                    host=host, database=database, charset=charset, port=port)
        self._op_connect()
        self._op_accept()
        self._op_service_attach()
        (h, oid, buf) = self._op_response()
        self.db_handle = h

    def backup_database(self, backup_filename, callback=None):
        spb = bs([isc_action_svc_backup])
        s = self.str_to_bytes(self.filename)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(backup_filename)
        spb += bs([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        if callback:
            spb += bs([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def restore_database(self, restore_filename, callback=None):
        spb = bs([isc_action_svc_restore])
        s = self.str_to_bytes(restore_filename)
        spb += bs([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(self.filename)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        if callback:
            spb += bs([isc_spb_verbose])
        spb += bs([isc_spb_res_buffers,0x00,0x08,0x00,0x00,isc_spb_res_page_size,0x00,0x10,0x00,0x00,isc_spb_options,0x00,0x30,0x00,0x00])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_start(self, name=None, cfg=None, callback=None):
        spb = bs([isc_action_svc_trace_start])
        if name:
            s = self.str_to_bytes(name)
            spb += bs([isc_spb_trc_name]) + int_to_bytes(len(s), 2) + s
        if cfg:
            s = self.str_to_bytes(cfg)
            spb += bs([isc_spb_trc_cfg]) + int_to_bytes(len(s), 2) + s
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_stop(self, id, callback=None):
        id = int(id)
        spb = bs([isc_action_svc_trace_stop])
        spb += bs([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bs([0x02]), bs([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_suspend(self, id, callback=None):
        id = int(id)
        spb = bs([isc_action_svc_trace_suspend])
        spb += bs([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bs([0x02]), bs([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_resume(self, id, callback=None):
        id = int(id)
        spb = bs([isc_action_svc_trace_resume])
        spb += bs([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bs([0x02]), bs([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_list(self, callback=None):
        spb = bs([isc_action_svc_trace_list])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def getServiceManagerVersion(self):
        self._op_service_info(bs([]), bs([isc_info_svc_version]))
        (h, oid, buf) = self._op_response()
        assert bi(buf[0]) == isc_info_svc_version
        return bi(buf[1])

    def getServerVersion(self):
        self._op_service_info(bs([]), bs([isc_info_svc_server_version]))
        (h, oid, buf) = self._op_response()
        assert bi(buf[0]) == isc_info_svc_server_version
        ln = bytes_to_int(buf[1:3])
        return self.bytes_to_str(buf[3:3+ln])

    def getArchitecture(self):
        return ''

    def getHomeDir(self):
        return ''

    def getSecurityDatabasePath(self):
        return ''

    def getLockFileDir(self):
        return ''

    def getCapabilityMask(self):
        return 0

    def getMessageFileDir(self):
        return ''

    def getConnectionCount(self):
        return 0

    def getAttachedDatabaseNames(self):
        return []

    def getLog(self):
        return ''

    def getStatistics(self):
        return ''

    def close(self):
        if not hasattr(self, "db_handle"):
            return
        self._op_service_detach()
        (h, oid, buf) = self._op_response()
        delattr(self, "db_handle")

    def __del__(self):
        if hasattr(self, "db_handle"):
            self.close()
