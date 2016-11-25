##############################################################################
# Copyright (c) 2009-2016, Hajime Nakagami<nakagami@gmail.com>
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
# Python DB-API 2.0 module for Firebird.
##############################################################################
from firebirdsql.consts import *
from firebirdsql.utils import *
from firebirdsql.fbcore import Connection


class Services(Connection):
    def sweep(self, database_name, callback=None):
        spb = bs([isc_spb_rpr_validate_db | isc_spb_rpr_sweep_db])
        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        optionMask = 0
        optionMask |= 0x02
        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def bringOnline(self, database_name, callback=None):
        spb = bs([isc_action_svc_properties])
        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        optionMask = 0
        optionMask |= 0x0200

        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def shutdown(
        self, database_name, timeout=0, shutForce=True,
        shutDenyNewAttachments=False, shutDenyNewTransactions=False,
        callback=None
    ):
        spb = bs([isc_action_svc_properties])
        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        if shutForce:
            spb += bs([isc_spb_prp_shutdown_db]) + int_to_bytes(timeout, 4)
        if shutDenyNewAttachments:
            spb += bs([isc_spb_prp_deny_new_attachments]) + int_to_bytes(timeout, 4)
        if shutDenyNewTransactions:
            spb += bs([isc_spb_prp_deny_new_transactions]) + int_to_bytes(timeout, 4)

        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def repair(
        self, database_name,
        readOnlyValidation=True, ignoreChecksums=False,
        killUnavailableShadows=False, mendDatabase=False,
        validateDatabase=False, validateRecordFragments=False, callback=None
    ):
        spb = bs([isc_action_svc_repair])

        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        optionMask = 0

        if readOnlyValidation:
            optionMask |= isc_spb_rpr_check_db
        if ignoreChecksums:
            optionMask |= isc_spb_rpr_ignore_checksum
        if killUnavailableShadows:
            optionMask |= isc_spb_rpr_kill_shadows
        if mendDatabase:
            optionMask |= isc_spb_rpr_mend_db
        if validateDatabase:
            optionMask |= isc_spb_rpr_validate_db
        if validateRecordFragments:
            optionMask |= isc_spb_rpr_full

        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def backup_database(
        self, database_name, backup_filename,
        transportable=True, metadataOnly=False, garbageCollect=True,
        ignoreLimboTransactions=False, ignoreChecksums=False,
        convertExternalTablesToInternalTables=True, expand=False, callback=None
    ):
        spb = bs([isc_action_svc_backup])
        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(backup_filename)
        spb += bs([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        optionMask = 0
        if ignoreChecksums:
            optionMask |= isc_spb_bkp_ignore_checksums
        if ignoreLimboTransactions:
            optionMask |= isc_spb_bkp_ignore_limbo
        if metadataOnly:
            optionMask |= isc_spb_bkp_metadata_only
        if not garbageCollect:
            optionMask |= isc_spb_bkp_no_garbage_collect
        if not transportable:
            optionMask |= isc_spb_bkp_non_transportable
        if convertExternalTablesToInternalTables:
            optionMask |= isc_spb_bkp_convert
        if expand:
            optionMask |= isc_spb_bkp_expand
        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        if callback:
            spb += bs([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def restore_database(
        self, restore_filename, database_name,
        replace=False, create=False, deactivateIndexes=False,
        doNotRestoreShadows=False, doNotEnforceConstraints=False,
        commitAfterEachTable=False, useAllPageSpace=False, pageSize=None,
        cacheBuffers=None, callback=None
    ):
        spb = bs([isc_action_svc_restore])
        s = self.str_to_bytes(restore_filename)
        spb += bs([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(database_name)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        optionMask = 0
        if replace:
            optionMask |= isc_spb_res_replace
        if create:
            optionMask |= isc_spb_res_create
        if deactivateIndexes:
            optionMask |= isc_spb_res_deactivate_idx
        if doNotRestoreShadows:
            optionMask |= isc_spb_res_no_shadow
        if doNotEnforceConstraints:
            optionMask |= isc_spb_res_no_validity
        if commitAfterEachTable:
            optionMask |= isc_spb_res_one_at_a_time
        if useAllPageSpace:
            optionMask |= isc_spb_res_use_all_space
        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        if pageSize:
            spb += bs([isc_spb_res_page_size]) + int_to_bytes(pageSize, 4)
        if cacheBuffers:
            spb += bs([isc_spb_res_buffers]) + int_to_bytes(cacheBuffers, 4)
        if callback:
            spb += bs([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bs([0x02]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
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
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
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
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def _getIntegerVal(self, item_id):
        self._op_service_info(bs([]), bs([item_id]))
        (h, oid, buf) = self._op_response()
        assert byte_to_int(buf[0]) == item_id
        return byte_to_int(buf[1])

    def _getStringVal(self, item_id):
        self._op_service_info(bs([]), bs([item_id]))
        (h, oid, buf) = self._op_response()
        assert byte_to_int(buf[0]) == item_id
        ln = bytes_to_int(buf[1:3])
        return self.bytes_to_str(buf[3:3+ln])

    def _getSvrDbInfo(self):
        self._op_service_info(bs([]), bs([isc_info_svc_svr_db_info]))
        (h, oid, buf) = self._op_response()
        assert byte_to_int(buf[0]) == isc_info_svc_svr_db_info
        db_names = []
        i = 1
        while i < len(buf) and byte_to_int(buf[i]) != isc_info_flag_end:
            if byte_to_int(buf[i]) == isc_spb_num_att:
                num_attach = bytes_to_int(buf[i+1:i+5])
                i += 5
            elif byte_to_int(buf[i]) == isc_spb_num_db:
                bytes_to_int(buf[7:11])     # db_num
                i += 5
            elif byte_to_int(buf[i]) == isc_spb_dbname:
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
            self._op_service_info(bs([]), bs([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bs([0x3e, 0x00, 0x00, 0x01]):
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
        spb = bs([isc_action_svc_get_fb_log])
        return self._getLogLines(spb)

    def getStatistics(
        self, dbname, showOnlyDatabaseLogPages=False,
        showOnlyDatabaseHeaderPages=False,
        showUserDataPages=True,
        showUserIndexPages=True,
        showSystemTablesAndIndexes=False
    ):
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

        spb = bs([isc_spb_res_length])
        s = self.str_to_bytes(dbname)
        spb += bs([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        spb += bs([isc_spb_options]) + int_to_bytes(optionMask, 4)
        return self._getLogLines(spb)


def connect(**kwargs):
    kwargs['is_services'] = True
    return Services(**kwargs)
