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

class Services(Connection):

    def backup_database(self, database_name, backup_filename, callback=None):
        spb = bytes([isc_action_svc_backup])
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(backup_filename)
        spb += bytes([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        if callback:
            spb += bytes([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e,0x00,0x00,0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def restore_database(self, restore_filename, database_name, callback=None):
        spb = bytes([isc_action_svc_restore])
        s = self.str_to_bytes(restore_filename)
        spb += bytes([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        if callback:
            spb += bytes([isc_spb_verbose])
        spb += bytes([isc_spb_res_buffers,0x00,0x08,0x00,0x00,isc_spb_res_page_size,0x00,0x10,0x00,0x00,isc_spb_options,0x00,0x30,0x00,0x00])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e,0x00,0x00,0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_start(self, name=None, cfg=None, callback=None):
        spb = bytes([isc_action_svc_trace_start])
        if name:
            s = self.str_to_bytes(name)
            spb += bytes([isc_spb_trc_name]) + int_to_bytes(len(s), 2) + s
        if cfg:
            s = self.str_to_bytes(cfg)
            spb += bytes([isc_spb_trc_cfg]) + int_to_bytes(len(s), 2) + s
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_stop(self, id, callback=None):
        id = int(id)
        spb = bytes([isc_action_svc_trace_stop])
        spb += bytes([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bytes([0x02]), bytes([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_suspend(self, id, callback=None):
        id = int(id)
        spb = bytes([isc_action_svc_trace_suspend])
        spb += bytes([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bytes([0x02]), bytes([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_resume(self, id, callback=None):
        id = int(id)
        spb = bytes([isc_action_svc_trace_resume])
        spb += bytes([isc_spb_trc_id]) + int_to_bytes(id, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h

        self._op_service_info(bytes([0x02]), bytes([0x3e]))
        (h, oid, buf) = self._op_response()
        ln = bytes_to_int(buf[1:2])
        if callback:
            callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_list(self, callback=None):
        spb = bytes([isc_action_svc_trace_list])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def _getIntegerVal(self, item_id):
        self._op_service_info(bytes([]), bytes([item_id]))
        (h, oid, buf) = self._op_response()
        assert b2i(buf[0]) == item_id
        return b2i(buf[1])

    def _getStringVal(self, item_id):
        self._op_service_info(bytes([]), bytes([item_id]))
        (h, oid, buf) = self._op_response()
        assert b2i(buf[0]) == item_id
        ln = bytes_to_int(buf[1:3])
        return self.bytes_to_str(buf[3:3+ln])

    def _getSvrDbInfo(self):
        self._op_service_info(bytes([]), bytes([isc_info_svc_svr_db_info]))
        (h, oid, buf) = self._op_response()
        assert b2i(buf[0]) == isc_info_svc_svr_db_info
        db_names=[]
        i = 1
        while i < len(buf) and b2i(buf[i]) != isc_info_flag_end:
            if b2i(buf[i]) == isc_spb_num_att:
                num_attach =  bytes_to_int(buf[i+1:i+5])
                i += 5
            elif b2i(buf[i]) == isc_spb_num_db:
                num_db =  bytes_to_int(buf[7:11])
                i += 5
            elif b2i(buf[i]) == isc_spb_dbname:
                ln = bytes_to_int(buf[i+1:i+3])
                db_name = self.bytes_to_str(buf[i+3:i+3+ln])
                db_names.append(db_name)
                i += 3 + ln

        return (num_attach, db_names)

    def _getLogLines(self, spb):
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        logs = ''
        while True:
            self._op_service_info(bytes([]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e,0x00,0x00,0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            logs += self.bytes_to_str(buf[3:3+ln]) + '\n'
        return logs

    def getServiceManagerVersion(self):
        return self._getIntegerVal(isc_info_svc_version)

    def getServerVersion(self):
        return self._getStringVal(isc_info_svc_server_version)

    def getArchitecture(self):
        return self._getStringVal(isc_info_svc_implementation)

    def getHomeDir(self):
        return self._getStringVal(isc_info_svc_get_env)

    def getSecurityDatabasePath(self):
        return self._getStringVal(isc_info_svc_user_dbpath)

    def getLockFileDir(self):
        return self._getStringVal(isc_info_svc_get_env_lock)

    def getCapabilityMask(self):
        return self._getIntegerVal(isc_info_svc_capabilities)

    def getMessageFileDir(self):
        return self._getStringVal(isc_info_svc_get_env_msg)

    def getConnectionCount(self):
        return self._getSvrDbInfo()[0]

    def getAttachedDatabaseNames(self):
        return self._getSvrDbInfo()[1]

    def getLog(self):
        spb = bytes([isc_action_svc_get_fb_log])
        return self._getLogLines(spb)

    def getStatistics(self, dbname, showOnlyDatabaseLogPages=False,
                                    showOnlyDatabaseHeaderPages=False,
                                    showUserDataPages=True,
                                    showUserIndexPages=True,
                                    showSystemTablesAndIndexes=False):
        optionMask = 0
        if showUserDataPages:
            optionMask |= isc_spb_sts_data_pages
        if showOnlyDatabaseLogPages:
            optionMask |= isc_spb_sts_db_log
        if showOnlyDatabaseHeaderPages:
            optionMask |= isc_spb_sts_hdr_pages
        if showUserIndexPages:
            optionMask |= isc_spb_sts_idx_pages
        if showSystemTablesAndIndexes:
            optionMask |= isc_spb_sts_sys_relations

        spb = bytes([isc_spb_res_length])
        s = self.str_to_bytes(dbname)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        spb += bytes([isc_spb_options]) + int_to_bytes(optionMask, 4)
        return self._getLogLines(spb)


def connect(user=None, password=None, host=None, charset=DEFAULT_CHARSET,  port=3050):
    return Services(user=user, password=password, host=host, charset=charset, is_services=True, port=3050)
