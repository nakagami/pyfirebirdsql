##############################################################################
# Copyright (c) 2009-2025, Hajime Nakagami<nakagami@gmail.com>
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
from collections.abc import Callable, Sequence
from typing import Any
from firebirdsql.consts import *    # noqa
from firebirdsql.utils import *     # noqa
from firebirdsql.fbcore import Connection


class Services(Connection):
    def sweep(self, database_name: str, callback: Callable[[str], Any] | None = None) -> None:
        spb = bytes([isc_spb_rpr_validate_db | isc_spb_rpr_sweep_db])
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        optionMask = 0
        optionMask |= 0x02
        spb += bytes([isc_spb_options]) + int_to_bytes(optionMask, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def bringOnline(self, database_name: str, callback: Callable[[str], Any] | None = None) -> None:
        spb = bytes([isc_action_svc_properties])
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        optionMask = 0
        optionMask |= 0x0200

        spb += bytes([isc_spb_options]) + int_to_bytes(optionMask, 4)
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def shutdown(
        self,
        database_name: str,
        timeout: int = 0,
        shutForce: bool = True,
        shutDenyNewAttachments: bool = False,
        shutDenyNewTransactions: bool = False,
        callback: Callable[[str], Any] | None = None,
    ) -> None:
        spb = bytes([isc_action_svc_properties])
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s

        if shutForce:
            spb += bytes([isc_spb_prp_shutdown_db]) + int_to_bytes(timeout, 4)
        if shutDenyNewAttachments:
            spb += bytes([isc_spb_prp_deny_new_attachments]) + int_to_bytes(timeout, 4)
        if shutDenyNewTransactions:
            spb += bytes([isc_spb_prp_deny_new_transactions]) + int_to_bytes(timeout, 4)

        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def backup_database(
        self,
        database_name: str,
        backup_filename: str,
        transportable: bool = True,
        metadataOnly: bool = False,
        garbageCollect: bool = True,
        ignoreLimboTransactions: bool = False,
        ignoreChecksums: bool = False,
        convertExternalTablesToInternalTables: bool = True,
        expand: bool = False,
        callback: Callable[[str], Any] | None = None,
    ) -> None:
        spb = bytes([isc_action_svc_backup])
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(backup_filename)
        spb += bytes([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
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
        spb += bytes([isc_spb_options]) + int_to_bytes(optionMask, 4)
        if callback:
            spb += bytes([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def restore_database(
        self,
        restore_filename: str,
        database_name: str,
        replace: bool = False,
        create: bool = False,
        deactivateIndexes: bool = False,
        doNotRestoreShadows: bool = False,
        doNotEnforceConstraints: bool = False,
        commitAfterEachTable: bool = False,
        useAllPageSpace: bool = False,
        pageSize: int | None = None,
        cacheBuffers: int | None = None,
        callback: Callable[[str], Any] | None = None,
    ) -> None:
        spb = bytes([isc_action_svc_restore])
        s = self.str_to_bytes(restore_filename)
        spb += bytes([isc_spb_bkp_file]) + int_to_bytes(len(s), 2) + s
        s = self.str_to_bytes(database_name)
        spb += bytes([isc_spb_dbname]) + int_to_bytes(len(s), 2) + s
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
        spb += bytes([isc_spb_options]) + int_to_bytes(optionMask, 4)
        if pageSize:
            spb += bytes([isc_spb_res_page_size]) + int_to_bytes(pageSize, 4)
        if cacheBuffers:
            spb += bytes([isc_spb_res_buffers]) + int_to_bytes(cacheBuffers, 4)
        if callback:
            spb += bytes([isc_spb_verbose])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            if callback:
                ln = bytes_to_int(buf[1:3])
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_start(self, name: str | None = None, cfg: str | None = None, callback: Callable[[str], Any] | None = None) -> None:
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
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def trace_stop(self, id: int, callback: Callable[[str], Any] | None = None) -> None:
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

    def trace_suspend(self, id: int, callback: Callable[[str], Any] | None = None) -> None:
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

    def trace_resume(self, id: int, callback: Callable[[str], Any] | None = None) -> None:
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

    def trace_list(self, callback: Callable[[str], Any] | None = None) -> None:
        spb = bytes([isc_action_svc_trace_list])
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        while True:
            self._op_service_info(bytes([0x02]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            if callback:
                callback(self.bytes_to_str(buf[3:3+ln]))

    def _getIntegerVal(self, item_id: int) -> int:
        self._op_service_info(bytes([]), bytes([item_id]))
        (h, oid, buf) = self._op_response()
        assert buf[0] == item_id
        return buf[1]

    def _getStringVal(self, item_id: int) -> str:
        self._op_service_info(bytes([]), bytes([item_id]))
        (h, oid, buf) = self._op_response()
        assert buf[0] == item_id
        ln = bytes_to_int(buf[1:3])
        return self.bytes_to_str(buf[3:3+ln])

    def _getSvrDbInfo(self) -> tuple[int, list[str]]:
        self._op_service_info(bytes([]), bytes([isc_info_svc_svr_db_info]))
        (h, oid, buf) = self._op_response()
        assert buf[0] == isc_info_svc_svr_db_info
        db_names = []
        i = 1
        num_attach = 0
        while i < len(buf) and buf[i] != isc_info_flag_end:
            if buf[i] == isc_spb_num_att:
                num_attach = bytes_to_int(buf[i+1:i+5])
                i += 5
            elif buf[i] == isc_spb_num_db:
                bytes_to_int(buf[7:11])     # db_num
                i += 5
            elif buf[i] == isc_spb_dbname:
                ln = bytes_to_int(buf[i+1:i+3])
                db_name = self.bytes_to_str(buf[i+3:i+3+ln])
                db_names.append(db_name)
                i += 3 + ln

        return (num_attach, db_names)

    def _getLogLines(self, spb: bytes) -> str:
        self._op_service_start(spb)
        (h, oid, buf) = self._op_response()
        self.svc_handle = h
        logs = ''
        while True:
            self._op_service_info(bytes([]), bytes([0x3e]))
            (h, oid, buf) = self._op_response()
            if buf[:4] == bytes([0x3e, 0x00, 0x00, 0x01]):
                break
            ln = bytes_to_int(buf[1:2])
            logs += self.bytes_to_str(buf[3:3+ln]) + '\n'
        return logs

    def getServiceManagerVersion(self) -> int:
        return self._getIntegerVal(isc_info_svc_version)

    def getServerVersion(self) -> str:
        return self._getStringVal(isc_info_svc_server_version)

    def getArchitecture(self) -> str:
        return self._getStringVal(isc_info_svc_implementation)

    def getHomeDir(self) -> str:
        return self._getStringVal(isc_info_svc_get_env)

    def getSecurityDatabasePath(self) -> str:
        return self._getStringVal(isc_info_svc_user_dbpath)

    def getLockFileDir(self) -> str:
        return self._getStringVal(isc_info_svc_get_env_lock)

    def getCapabilityMask(self) -> int:
        return self._getIntegerVal(isc_info_svc_capabilities)

    def getMessageFileDir(self) -> str:
        return self._getStringVal(isc_info_svc_get_env_msg)

    def getConnectionCount(self) -> int:
        return self._getSvrDbInfo()[0]

    def getAttachedDatabaseNames(self) -> list[str]:
        return self._getSvrDbInfo()[1]

    def getLog(self) -> str:
        spb = bytes([isc_action_svc_get_fb_log])
        return self._getLogLines(spb)

    def getStatistics(
        self,
        dbname: str,
        showOnlyDatabaseLogPages: bool = False,
        showOnlyDatabaseHeaderPages: bool = False,
        showUserDataPages: bool = True,
        showUserIndexPages: bool = True,
        showSystemTablesAndIndexes: bool = False,
    ) -> str:
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


def connect(
    dsn: str | None = None,
    user: str | None = None,
    password: str | None = None,
    role: str | None = None,
    host: str | None = None,
    database: str | None = None,
    charset: str = DEFAULT_CHARSET,
    port: int | None = None,
    page_size: int = 4096,
    cloexec: bool = False,
    timeout: float | None = None,
    isolation_level: int | None = None,
    auth_plugin_name: str | None = None,
    wire_crypt: bool = True,
    timezone: str | None = None,
    wire_compress: bool = False,
    **kwargs: Any,
) -> Services:
    kwargs['is_services'] = True
    services = Services(
        dsn=dsn,
        user=user,
        password=password,
        role=role,
        host=host,
        database=database,
        charset=charset,
        port=port,
        page_size=page_size,
        cloexec=cloexec,
        timeout=timeout,
        isolation_level=isolation_level,
        auth_plugin_name=auth_plugin_name,
        wire_crypt=wire_crypt,
        timezone=timezone,
        wire_compress=wire_compress,
        **kwargs,
    )
    services._initialize()
    return services

