##############################################################################
# Copyright (c) 2011-2018, Hajime Nakagami<nakagami@gmail.com>
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
import sys

PYTHON_MAJOR_VER = sys.version_info[0]

DEFAULT_CHARSET = 'UTF8'

ISC_TIME_SECONDS_PRECISION = 10000
MAX_CHAR_LENGTH = 32767
BLOB_SEGMENT_SIZE = 32000

DESCRIPTION_NAME = 0
DESCRIPTION_TYPE_CODE = 1
DESCRIPTION_DISPLAY_SIZE = 2
DESCRIPTION_INTERNAL_SIZE = 3
DESCRIPTION_PRECISION = 4
DESCRIPTION_SCALE = 5
DESCRIPTION_NULL_OK = 6

isc_info_end = 1
isc_info_truncated = 2
isc_info_error = 3
isc_info_data_not_ready = 4
isc_info_length = 126
isc_info_flag_end = 127

isc_info_db_id = 4
isc_info_reads = 5
isc_info_writes = 6
isc_info_fetches = 7
isc_info_marks = 8
isc_info_implementation = 11
isc_info_version = 12
isc_info_base_level = 13
isc_info_page_size = 14
isc_info_num_buffers = 15
isc_info_limbo = 16
isc_info_current_memory = 17
isc_info_max_memory = 18
isc_info_window_turns = 19
isc_info_license = 20
isc_info_allocation = 21
isc_info_attachment_id = 22
isc_info_read_seq_count = 23
isc_info_read_idx_count = 24
isc_info_insert_count = 25
isc_info_update_count = 26
isc_info_delete_count = 27
isc_info_backout_count = 28
isc_info_purge_count = 29
isc_info_expunge_count = 30
isc_info_sweep_interval = 31
isc_info_ods_version = 32
isc_info_ods_minor_version = 33
isc_info_no_reserve = 34
isc_info_logfile = 35
isc_info_cur_logfile_name = 36
isc_info_cur_log_part_offset = 37
isc_info_num_wal_buffers = 38
isc_info_wal_buffer_size = 39
isc_info_wal_ckpt_length = 40
isc_info_wal_cur_ckpt_interval = 41
isc_info_wal_prv_ckpt_fname = 42
isc_info_wal_prv_ckpt_poffset = 43
isc_info_wal_recv_ckpt_fname = 44
isc_info_wal_recv_ckpt_poffset = 45
isc_info_wal_grpc_wait_usecs = 47
isc_info_wal_num_io = 48
isc_info_wal_avg_io_size = 49
isc_info_wal_num_commits = 50
isc_info_wal_avg_grpc_size = 51
isc_info_forced_writes = 52
isc_info_user_names = 53
isc_info_page_errors = 54
isc_info_record_errors = 55
isc_info_bpage_errors = 56
isc_info_dpage_errors = 57
isc_info_ipage_errors = 58
isc_info_ppage_errors = 59
isc_info_tpage_errors = 60
isc_info_set_page_buffers = 61
isc_info_db_sql_dialect = 62
isc_info_db_read_only = 63
isc_info_db_size_in_pages = 64
isc_info_att_charset = 101
isc_info_db_class = 102
isc_info_firebird_version = 103
isc_info_oldest_transaction = 104
isc_info_oldest_active = 105
isc_info_oldest_snapshot = 106
isc_info_next_transaction = 107
isc_info_db_provider = 108
isc_info_active_transactions = 109
isc_info_active_tran_count = 110
isc_info_creation_date = 111
isc_info_db_file_size = 112

# isc_info_sql_records items
isc_info_req_select_count = 13
isc_info_req_insert_count = 14
isc_info_req_update_count = 15
isc_info_req_delete_count = 16

isc_info_svc_svr_db_info = 50
isc_info_svc_get_license = 51
isc_info_svc_get_license_mask = 52
isc_info_svc_get_config = 53
isc_info_svc_version = 54
isc_info_svc_server_version = 55
isc_info_svc_implementation = 56
isc_info_svc_capabilities = 57
isc_info_svc_user_dbpath = 58
isc_info_svc_get_env = 59
isc_info_svc_get_env_lock = 60
isc_info_svc_get_env_msg = 61
isc_info_svc_line = 62
isc_info_svc_to_eof = 63
isc_info_svc_timeout = 64
isc_info_svc_get_licensed_users = 65
isc_info_svc_limbo_trans = 66
isc_info_svc_running = 67
isc_info_svc_get_users = 68

SQL_TYPE_TEXT = 452
SQL_TYPE_VARYING = 448
SQL_TYPE_SHORT = 500
SQL_TYPE_LONG = 496
SQL_TYPE_FLOAT = 482
SQL_TYPE_DOUBLE = 480
SQL_TYPE_D_FLOAT = 530
SQL_TYPE_TIMESTAMP = 510
SQL_TYPE_BLOB = 520
SQL_TYPE_ARRAY = 540
SQL_TYPE_QUAD = 550
SQL_TYPE_TIME = 560
SQL_TYPE_DATE = 570
SQL_TYPE_INT64 = 580
SQL_TYPE_DEC_FIXED = 32758
SQL_TYPE_DEC64 = 32760
SQL_TYPE_DEC128 = 32762
SQL_TYPE_BOOLEAN = 32764
SQL_TYPE_NULL = 32766


ISOLATION_LEVEL_READ_COMMITED_LEGACY = 0
ISOLATION_LEVEL_READ_COMMITED = 1
ISOLATION_LEVEL_REPEATABLE_READ = 2
ISOLATION_LEVEL_SNAPSHOT = ISOLATION_LEVEL_REPEATABLE_READ
ISOLATION_LEVEL_SERIALIZABLE = 3
ISOLATION_LEVEL_READ_COMMITED_RO = 4

# Database Parameter Block parameter
isc_dpb_version1 = 1
isc_dpb_version2 = 2
isc_dpb_cdd_pathname = 1
isc_dpb_allocation = 2
isc_dpb_journal = 3
isc_dpb_page_size = 4
isc_dpb_num_buffers = 5
isc_dpb_buffer_length = 6
isc_dpb_debug = 7
isc_dpb_garbage_collect = 8
isc_dpb_verify = 9
isc_dpb_sweep = 10
isc_dpb_enable_journal = 11
isc_dpb_disable_journal = 12
isc_dpb_dbkey_scope = 13
isc_dpb_number_of_users = 14
isc_dpb_trace = 15
isc_dpb_no_garbage_collect = 16
isc_dpb_damaged = 17
isc_dpb_license = 18
isc_dpb_sys_user_name = 19
isc_dpb_encrypt_key = 20
isc_dpb_activate_shadow = 21
isc_dpb_sweep_interval = 22
isc_dpb_delete_shadow = 23
isc_dpb_force_write = 24
isc_dpb_begin_log = 25
isc_dpb_quit_log = 26
isc_dpb_no_reserve = 27
isc_dpb_user_name = 28
isc_dpb_password = 29
isc_dpb_password_enc = 30
isc_dpb_sys_user_name_enc = 31
isc_dpb_interp = 32
isc_dpb_online_dump = 33
isc_dpb_old_file_size = 34
isc_dpb_old_num_files = 35
isc_dpb_old_file = 36
isc_dpb_old_start_page = 37
isc_dpb_old_start_seqno = 38
isc_dpb_old_start_file = 39
isc_dpb_drop_walfile = 40
isc_dpb_old_dump_id = 41
isc_dpb_wal_backup_dir = 42
isc_dpb_wal_chkptlen = 43
isc_dpb_wal_numbufs = 44
isc_dpb_wal_bufsize = 45
isc_dpb_wal_grp_cmt_wait = 46
isc_dpb_lc_messages = 47
isc_dpb_lc_ctype = 48
isc_dpb_cache_manager = 49
isc_dpb_shutdown = 50
isc_dpb_online = 51
isc_dpb_shutdown_delay = 52
isc_dpb_reserved = 53
isc_dpb_overwrite = 54
isc_dpb_sec_attach = 55
isc_dpb_disable_wal = 56
isc_dpb_connect_timeout = 57
isc_dpb_dummy_packet_interval = 58
isc_dpb_gbak_attach = 59
isc_dpb_sql_role_name = 60
isc_dpb_set_page_buffers = 61
isc_dpb_working_directory = 62
isc_dpb_sql_dialect = 63
isc_dpb_set_db_readonly = 64
isc_dpb_set_db_sql_dialect = 65
isc_dpb_gfix_attach = 66
isc_dpb_gstat_attach = 67
isc_dpb_set_db_charset = 68
isc_dpb_gsec_attach = 69
isc_dpb_address_path = 70
isc_dpb_process_id = 71
isc_dpb_no_db_triggers = 72
isc_dpb_trusted_auth = 73
isc_dpb_process_name = 74
isc_dpb_trusted_role = 75
isc_dpb_org_filename = 76
isc_dpb_utf8_filename = 77
isc_dpb_ext_call_depth = 78
isc_dpb_auth_block = 79
isc_dpb_client_version = 80
isc_dpb_remote_protocol = 81
isc_dpb_host_name = 82
isc_dpb_os_user = 83
isc_dpb_specific_auth_data = 84
isc_dpb_auth_plugin_list = 85
isc_dpb_auth_plugin_name = 86
isc_dpb_config = 87
isc_dpb_nolinger = 88

# Transaction Parameter Block parameter
isc_tpb_version1 = 1
isc_tpb_version3 = 3
isc_tpb_consistency = 1
isc_tpb_concurrency = 2
isc_tpb_shared = 3
isc_tpb_protected = 4
isc_tpb_exclusive = 5
isc_tpb_wait = 6
isc_tpb_nowait = 7
isc_tpb_read = 8
isc_tpb_write = 9
isc_tpb_lock_read = 10
isc_tpb_lock_write = 11
isc_tpb_verb_time = 12
isc_tpb_commit_time = 13
isc_tpb_ignore_limbo = 14
isc_tpb_read_committed = 15
isc_tpb_autocommit = 16
isc_tpb_rec_version = 17
isc_tpb_no_rec_version = 18
isc_tpb_restart_requests = 19
isc_tpb_no_auto_undo = 20
isc_tpb_lock_timeout = 21


# Service Parameter Block parameter
isc_spb_version1 = 1
isc_spb_current_version = 2
isc_spb_version = isc_spb_current_version
isc_spb_user_name = 28              # isc_dpb_user_name
isc_spb_sys_user_name = 19          # isc_dpb_sys_user_name
isc_spb_sys_user_name_enc = 31      # isc_dpb_sys_user_name_enc
isc_spb_password = 29               # isc_dpb_password
isc_spb_password_enc = 30           # isc_dpb_password_enc
isc_spb_command_line = 105
isc_spb_dbname = 106
isc_spb_verbose = 107
isc_spb_options = 108
isc_spb_address_path = 109
isc_spb_process_id = 110
isc_spb_trusted_auth = 111
isc_spb_process_name = 112
isc_spb_trusted_role = 113
isc_spb_connect_timeout = 57        # isc_dpb_connect_timeout
isc_spb_dummy_packet_interval = 58  # isc_dpb_dummy_packet_interval
isc_spb_sql_role_name = 60          # isc_dpb_sql_role_name

# isc_action_svc_properties params
isc_spb_prp_page_buffers = 5
isc_spb_prp_sweep_interval = 6
isc_spb_prp_shutdown_db = 7
isc_spb_prp_deny_new_attachments = 9
isc_spb_prp_deny_new_transactions = 10
isc_spb_prp_reserve_space = 11
isc_spb_prp_write_mode = 12
isc_spb_prp_access_mode = 13
isc_spb_prp_set_sql_dialect = 14
isc_spb_prp_activate = 0x0100
isc_spb_prp_db_online = 0x0200
isc_spb_prp_force_shutdown = 41
isc_spb_prp_attachments_shutdown = 42
isc_spb_prp_transactions_shutdown = 43
isc_spb_prp_shutdown_mode = 44
isc_spb_prp_online_mode = 45

# backup
isc_spb_bkp_file = 5
isc_spb_bkp_factor = 6
isc_spb_bkp_length = 7
isc_spb_bkp_ignore_checksums = 0x01
isc_spb_bkp_ignore_limbo = 0x02
isc_spb_bkp_metadata_only = 0x04
isc_spb_bkp_no_garbage_collect = 0x08
isc_spb_bkp_old_descriptions = 0x10
isc_spb_bkp_non_transportable = 0x20
isc_spb_bkp_convert = 0x40
isc_spb_bkp_expand = 0x8

# restore
isc_spb_res_buffers = 9
isc_spb_res_page_size = 10
isc_spb_res_length = 11
isc_spb_res_access_mode = 12
isc_spb_res_deactivate_idx = 0x0100
isc_spb_res_no_shadow = 0x0200
isc_spb_res_no_validity = 0x0400
isc_spb_res_one_at_a_time = 0x0800
isc_spb_res_replace = 0x1000
isc_spb_res_create = 0x2000
isc_spb_res_use_all_space = 0x4000

# trace
isc_spb_trc_id = 1
isc_spb_trc_name = 2
isc_spb_trc_cfg = 3

# isc_info_svc_svr_db_info params
isc_spb_num_att = 5
isc_spb_num_db = 6

# isc_info_svc_db_stats params
isc_spb_sts_data_pages = 0x01
isc_spb_sts_db_log = 0x02
isc_spb_sts_hdr_pages = 0x04
isc_spb_sts_idx_pages = 0x08
isc_spb_sts_sys_relations = 0x10
isc_spb_sts_record_versions = 0x20
isc_spb_sts_table = 0x40
isc_spb_sts_nocreation = 0x80

# isc_action_svc_repair params
isc_spb_rpr_validate_db = 0x01
isc_spb_rpr_sweep_db = 0x02
isc_spb_rpr_mend_db = 0x04
isc_spb_rpr_list_limbo_trans = 0x08
isc_spb_rpr_check_db = 0x10
isc_spb_rpr_ignore_checksum = 0x20
isc_spb_rpr_kill_shadows = 0x40
isc_spb_rpr_full = 0x80

# Service Action Items
isc_action_svc_backup = 1
isc_action_svc_restore = 2
isc_action_svc_repair = 3
isc_action_svc_add_user = 4
isc_action_svc_delete_user = 5
isc_action_svc_modify_user = 6
isc_action_svc_display_user = 7
isc_action_svc_properties = 8
isc_action_svc_add_license = 9
isc_action_svc_remove_license = 10
isc_action_svc_db_stats = 11
isc_action_svc_get_ib_log = 12
isc_action_svc_get_fb_log = 12
isc_action_svc_nbak = 20
isc_action_svc_nrest = 21
isc_action_svc_trace_start = 22
isc_action_svc_trace_stop = 23
isc_action_svc_trace_suspend = 24
isc_action_svc_trace_resume = 25
isc_action_svc_trace_list = 26
isc_action_svc_set_mapping = 27
isc_action_svc_drop_mapping = 28
isc_action_svc_display_user_adm = 29
isc_action_svc_last = 30

# Transaction informatino items
isc_info_tra_id = 4
isc_info_tra_oldest_interesting = 5
isc_info_tra_oldest_snapshot = 6
isc_info_tra_oldest_active = 7
isc_info_tra_isolation = 8
isc_info_tra_access = 9
isc_info_tra_lock_timeout = 10

# SQL information items
isc_info_sql_select = 4
isc_info_sql_bind = 5
isc_info_sql_num_variables = 6
isc_info_sql_describe_vars = 7
isc_info_sql_describe_end = 8
isc_info_sql_sqlda_seq = 9
isc_info_sql_message_seq = 10
isc_info_sql_type = 11
isc_info_sql_sub_type = 12
isc_info_sql_scale = 13
isc_info_sql_length = 14
isc_info_sql_null_ind = 15
isc_info_sql_field = 16
isc_info_sql_relation = 17
isc_info_sql_owner = 18
isc_info_sql_alias = 19
isc_info_sql_sqlda_start = 20
isc_info_sql_stmt_type = 21
isc_info_sql_get_plan = 22
isc_info_sql_records = 23
isc_info_sql_batch_fetch = 24

isc_info_sql_stmt_select = 1
isc_info_sql_stmt_insert = 2
isc_info_sql_stmt_update = 3
isc_info_sql_stmt_delete = 4
isc_info_sql_stmt_ddl = 5
isc_info_sql_stmt_get_segment = 6
isc_info_sql_stmt_put_segment = 7
isc_info_sql_stmt_exec_procedure = 8
isc_info_sql_stmt_start_trans = 9
isc_info_sql_stmt_commit = 10
isc_info_sql_stmt_rollback = 11
isc_info_sql_stmt_select_for_upd = 12
isc_info_sql_stmt_set_generator = 13
isc_info_sql_stmt_savepoint = 14

isc_arg_end = 0
isc_arg_gds = 1
isc_arg_string = 2
isc_arg_cstring = 3
isc_arg_number = 4
isc_arg_interpreted = 5
isc_arg_vms = 6
isc_arg_unix = 7
isc_arg_domain = 8
isc_arg_dos = 9
isc_arg_mpexl = 10
isc_arg_mpexl_ipc = 11
isc_arg_next_mach = 15
isc_arg_netware = 16
isc_arg_win32 = 17
isc_arg_warning = 18
isc_arg_sql_state = 19

# Protocol Types (accept_type)
ptype_batch_send = 3    # Batch sends, no asynchrony
ptype_out_of_band = 4   # Batch sends w/ out of band notification
ptype_lazy_send = 5     # Deferred packets delivery

PROTOCOL_VERSION10 = 10
PROTOCOL_VERSION11 = 11
PROTOCOL_VERSION12 = 12
PROTOCOL_VERSION13 = 13

CNCT_user = 1
CNCT_passwd = 2
CNCT_host = 4
CNCT_group = 5
CNCT_user_verification = 6
CNCT_specific_data = 7
CNCT_plugin_name = 8
CNCT_login = 9
CNCT_plugin_list = 10
CNCT_client_crypt = 11

DSQL_close = 1
DSQL_drop = 2

charset_map = {
    # DB CHAR SET NAME    :   PYTHON CODEC NAME (CANONICAL)
    # --------------------------------------------------------------------------
    'OCTETS':   None,   # Allow to pass through unchanged.
    'UNICODE_FSS':   'utf_8',
    'UTF8':   'utf_8',  # (Firebird 2.0+)
    'SJIS_0208':   'shift_jis',
    'EUCJ_0208':   'euc_jp',
    'DOS737':   'cp737',
    'DOS437':   'cp437',
    'DOS850':   'cp850',
    'DOS865':   'cp865',
    'DOS860':   'cp860',
    'DOS863':   'cp863',
    'DOS775':   'cp775',
    'DOS862':   'cp862',
    'DOS864':   'cp864',
    'ISO8859_1':   'iso8859_1',
    'ISO8859_2':   'iso8859_2',
    'ISO8859_3':   'iso8859_3',
    'ISO8859_4':   'iso8859_4',
    'ISO8859_5':   'iso8859_5',
    'ISO8859_6':   'iso8859_6',
    'ISO8859_7':   'iso8859_7',
    'ISO8859_8':   'iso8859_8',
    'ISO8859_9':   'iso8859_9',
    'ISO8859_13':   'iso8859_13',
    'KSC_5601':   'euc_kr',
    'DOS852':   'cp852',
    'DOS857':   'cp857',
    'DOS861':   'cp861',
    'DOS866':   'cp866',
    'DOS869':   'cp869',
    'WIN1250':   'cp1250',
    'WIN1251':   'cp1251',
    'WIN1252':   'cp1252',
    'WIN1253':   'cp1253',
    'WIN1254':   'cp1254',
    'BIG_5':   'big5',
    'GB_2312':   'gb2312',
    'WIN1255':   'cp1255',
    'WIN1256':   'cp1256',
    'WIN1257':   'cp1257',
    'KOI8-R':   'koi8_r',   # (Firebird 2.0+)
    'KOI8-U':   'koi8_u',   # (Firebird 2.0+)
    'WIN1258':   'cp1258',  # (Firebird 2.0+)
}
