#!/usr/bin/env python
##############################################################################
# Copyright (c) 2010-2013 Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
# Licensed under the New BSD License
# (http://www.freebsd.org/copyright/freebsd-license.html)
#
# Firebird RDBMS (http://www.firebirdsql.org/) proxy tool for debug.
##############################################################################
from __future__ import print_function
import sys
import socket
import binascii
import xdrlib
import ctypes
try:
    import thread
except ImportError:
    import _thread as thread

PYTHON3 = sys.version_info[0] > 2

def _ord(c):
    if PYTHON3:
        if isinstance(c, bytes):
            return ord(c)
        return c
    else:
        return ord(c)

bufsize = 65535

thread_last_op_name = {}
def get_last_op_name():
    return thread_last_op_name.get(thread.get_ident())
def set_last_op_name(op_name):
    thread_last_op_name[thread.get_ident()] = op_name

thread_prepare_trans = {}
def get_prepare_trans():
    return thread_prepare_trans.get(thread.get_ident())
def set_prepare_trans(trans):
    thread_prepare_trans[thread.get_ident()] = trans

thread_prepare_statement = {}
def get_prepare_statement():
    return thread_prepare_statement.get(thread.get_ident())
def set_prepare_statement(trans):
    thread_prepare_statement[thread.get_ident()] = trans

thread_prepare_dialect = {}
def get_prepare_dialect():
    return thread_prepare_dialect.get(thread.get_ident())
def set_prepare_dialect(trans):
    thread_prepare_dialect[thread.get_ident()] = trans

thread_xsqlda_statement = {}
def get_xsqlda_statement():
    return thread_xsqlda_statement.setdefault(thread.get_ident(), {})

def asc_dump(s):
    r = ''
    for c in s:
        if PYTHON3:
            r += chr(c) if (_ord(c) >= 32 and _ord(c) < 128) else '.'
        else:
            r += c if (_ord(c) >= 32 and _ord(c) < 128) else '.'
    if r:
        print('\t[' + r + ']')

def hex_dump(s):
    print('\t', '-' * 55)
    i = 0
    (r, s) = s[:16], s[16:]
    while r:
        print('\t%04x' % (i,), binascii.b2a_hex(r), end='')
        print('  ' * (16 - len(r)), end='')
        for j in range(len(r)):
            if _ord(r[j]) < 32 or _ord(r[j]) > 127:
                r = r[:j] + b'.' + r[j+1:]
        print(r)
        (r, s) = s[:16], s[16:]
        i = i + 16
    print('\t', '-' * 55)

def msg_dump(s):
    hex_dump(s)

isc_gds_error_code = {
  335544321 : 'isc_arith_except',
  335544322 : 'isc_bad_dbkey',
  335544323 : 'isc_bad_db_format',
  335544324 : 'isc_bad_db_handle',
  335544325 : 'isc_bad_dpb_content',
  335544326 : 'isc_bad_dpb_form',
  335544327 : 'isc_bad_req_handle',
  335544328 : 'isc_bad_segstr_handle',
  335544329 : 'isc_bad_segstr_id',
  335544330 : 'isc_bad_tpb_content',
  335544331 : 'isc_bad_tpb_form',
  335544332 : 'isc_bad_trans_handle',
  335544333 : 'isc_bug_check',
  335544334 : 'isc_convert_error',
  335544335 : 'isc_db_corrupt',
  335544336 : 'isc_deadlock',
  335544337 : 'isc_excess_trans',
  335544338 : 'isc_from_no_match',
  335544339 : 'isc_infinap',
  335544340 : 'isc_infona',
  335544341 : 'isc_infunk',
  335544342 : 'isc_integ_fail',
  335544343 : 'isc_invalid_blr',
  335544344 : 'isc_io_error',
  335544345 : 'isc_lock_conflict',
  335544346 : 'isc_metadata_corrupt',
  335544347 : 'isc_not_valid',
  335544348 : 'isc_no_cur_rec',
  335544349 : 'isc_no_dup',
  335544350 : 'isc_no_finish',
  335544351 : 'isc_no_meta_update',
  335544352 : 'isc_no_priv',
  335544353 : 'isc_no_recon',
  335544354 : 'isc_no_record',
  335544355 : 'isc_no_segstr_close',
  335544356 : 'isc_obsolete_metadata',
  335544357 : 'isc_open_trans',
  335544358 : 'isc_port_len',
  335544359 : 'isc_read_only_field',
  335544360 : 'isc_read_only_rel',
  335544361 : 'isc_read_only_trans',
  335544362 : 'isc_read_only_view',
  335544363 : 'isc_req_no_trans',
  335544364 : 'isc_req_sync',
  335544365 : 'isc_req_wrong_db',
  335544366 : 'isc_segment',
  335544367 : 'isc_segstr_eof',
  335544368 : 'isc_segstr_no_op',
  335544369 : 'isc_segstr_no_read',
  335544370 : 'isc_segstr_no_trans',
  335544371 : 'isc_segstr_no_write',
  335544372 : 'isc_segstr_wrong_db',
  335544373 : 'isc_sys_request',
  335544374 : 'isc_stream_eof',
  335544375 : 'isc_unavailable',
  335544376 : 'isc_unres_rel',
  335544377 : 'isc_uns_ext',
  335544378 : 'isc_wish_list',
  335544379 : 'isc_wrong_ods',
  335544380 : 'isc_wronumarg',
  335544381 : 'isc_imp_exc',
  335544382 : 'isc_random',
  335544383 : 'isc_fatal_conflict',
  335544384 : 'isc_badblk',
  335544385 : 'isc_invpoolcl',
  335544386 : 'isc_nopoolids',
  335544387 : 'isc_relbadblk',
  335544388 : 'isc_blktoobig',
  335544389 : 'isc_bufexh',
  335544390 : 'isc_syntaxerr',
  335544391 : 'isc_bufinuse',
  335544392 : 'isc_bdbincon',
  335544393 : 'isc_reqinuse',
  335544394 : 'isc_badodsver',
  335544395 : 'isc_relnotdef',
  335544396 : 'isc_fldnotdef',
  335544397 : 'isc_dirtypage',
  335544398 : 'isc_waifortra',
  335544399 : 'isc_doubleloc',
  335544400 : 'isc_nodnotfnd',
  335544401 : 'isc_dupnodfnd',
  335544402 : 'isc_locnotmar',
  335544403 : 'isc_badpagtyp',
  335544404 : 'isc_corrupt',
  335544405 : 'isc_badpage',
  335544406 : 'isc_badindex',
  335544407 : 'isc_dbbnotzer',
  335544408 : 'isc_tranotzer',
  335544409 : 'isc_trareqmis',
  335544410 : 'isc_badhndcnt',
  335544411 : 'isc_wrotpbver',
  335544412 : 'isc_wroblrver',
  335544413 : 'isc_wrodpbver',
  335544414 : 'isc_blobnotsup',
  335544415 : 'isc_badrelation',
  335544416 : 'isc_nodetach',
  335544417 : 'isc_notremote',
  335544418 : 'isc_trainlim',
  335544419 : 'isc_notinlim',
  335544420 : 'isc_traoutsta',
  335544421 : 'isc_connect_reject',
  335544422 : 'isc_dbfile',
  335544423 : 'isc_orphan',
  335544424 : 'isc_no_lock_mgr',
  335544425 : 'isc_ctxinuse',
  335544426 : 'isc_ctxnotdef',
  335544427 : 'isc_datnotsup',
  335544428 : 'isc_badmsgnum',
  335544429 : 'isc_badparnum',
  335544430 : 'isc_virmemexh',
  335544431 : 'isc_blocking_signal',
  335544432 : 'isc_lockmanerr',
  335544433 : 'isc_journerr',
  335544434 : 'isc_keytoobig',
  335544435 : 'isc_nullsegkey',
  335544436 : 'isc_sqlerr',
  335544437 : 'isc_wrodynver',
  335544438 : 'isc_funnotdef',
  335544439 : 'isc_funmismat',
  335544440 : 'isc_bad_msg_vec',
  335544441 : 'isc_bad_detach',
  335544442 : 'isc_noargacc_read',
  335544443 : 'isc_noargacc_write',
  335544444 : 'isc_read_only',
  335544445 : 'isc_ext_err',
  335544446 : 'isc_non_updatable',
  335544447 : 'isc_no_rollback',
  335544448 : 'isc_bad_sec_info',
  335544449 : 'isc_invalid_sec_info',
  335544450 : 'isc_misc_interpreted',
  335544451 : 'isc_update_conflict',
  335544452 : 'isc_unlicensed',
  335544453 : 'isc_obj_in_use',
  335544454 : 'isc_nofilter',
  335544455 : 'isc_shadow_accessed',
  335544456 : 'isc_invalid_sdl',
  335544457 : 'isc_out_of_bounds',
  335544458 : 'isc_invalid_dimension',
  335544459 : 'isc_rec_in_limbo',
  335544460 : 'isc_shadow_missing',
  335544461 : 'isc_cant_validate',
  335544462 : 'isc_cant_start_journal',
  335544463 : 'isc_gennotdef',
  335544464 : 'isc_cant_start_logging',
  335544465 : 'isc_bad_segstr_type',
  335544466 : 'isc_foreign_key',
  335544467 : 'isc_high_minor',
  335544468 : 'isc_tra_state',
  335544469 : 'isc_trans_invalid',
  335544470 : 'isc_buf_invalid',
  335544471 : 'isc_indexnotdefined',
  335544472 : 'isc_login',
  335544473 : 'isc_invalid_bookmark',
  335544474 : 'isc_bad_lock_level',
  335544475 : 'isc_relation_lock',
  335544476 : 'isc_record_lock',
  335544477 : 'isc_max_idx',
  335544478 : 'isc_jrn_enable',
  335544479 : 'isc_old_failure',
  335544480 : 'isc_old_in_progress',
  335544481 : 'isc_old_no_space',
  335544482 : 'isc_no_wal_no_jrn',
  335544483 : 'isc_num_old_files',
  335544484 : 'isc_wal_file_open',
  335544485 : 'isc_bad_stmt_handle',
  335544486 : 'isc_wal_failure',
  335544487 : 'isc_walw_err',
  335544488 : 'isc_logh_small',
  335544489 : 'isc_logh_inv_version',
  335544490 : 'isc_logh_open_flag',
  335544491 : 'isc_logh_open_flag2',
  335544492 : 'isc_logh_diff_dbname',
  335544493 : 'isc_logf_unexpected_eof',
  335544494 : 'isc_logr_incomplete',
  335544495 : 'isc_logr_header_small',
  335544496 : 'isc_logb_small',
  335544497 : 'isc_wal_illegal_attach',
  335544498 : 'isc_wal_invalid_wpb',
  335544499 : 'isc_wal_err_rollover',
  335544500 : 'isc_no_wal',
  335544501 : 'isc_drop_wal',
  335544502 : 'isc_stream_not_defined',
  335544503 : 'isc_wal_subsys_error',
  335544504 : 'isc_wal_subsys_corrupt',
  335544505 : 'isc_no_archive',
  335544506 : 'isc_shutinprog',
  335544507 : 'isc_range_in_use',
  335544508 : 'isc_range_not_found',
  335544509 : 'isc_charset_not_found',
  335544510 : 'isc_lock_timeout',
  335544511 : 'isc_prcnotdef',
  335544512 : 'isc_prcmismat',
  335544513 : 'isc_wal_bugcheck',
  335544514 : 'isc_wal_cant_expand',
  335544515 : 'isc_codnotdef',
  335544516 : 'isc_xcpnotdef',
  335544517 : 'isc_except',
  335544518 : 'isc_cache_restart',
  335544519 : 'isc_bad_lock_handle',
  335544520 : 'isc_jrn_present',
  335544521 : 'isc_wal_err_rollover2',
  335544522 : 'isc_wal_err_logwrite',
  335544523 : 'isc_wal_err_jrn_comm',
  335544524 : 'isc_wal_err_expansion',
  335544525 : 'isc_wal_err_setup',
  335544526 : 'isc_wal_err_ww_sync',
  335544527 : 'isc_wal_err_ww_start',
  335544528 : 'isc_shutdown',
  335544529 : 'isc_existing_priv_mod',
  335544530 : 'isc_primary_key_ref',
  335544531 : 'isc_primary_key_notnull',
  335544532 : 'isc_ref_cnstrnt_notfound',
  335544533 : 'isc_foreign_key_notfound',
  335544534 : 'isc_ref_cnstrnt_update',
  335544535 : 'isc_check_cnstrnt_update',
  335544536 : 'isc_check_cnstrnt_del',
  335544537 : 'isc_integ_index_seg_del',
  335544538 : 'isc_integ_index_seg_mod',
  335544539 : 'isc_integ_index_del',
  335544540 : 'isc_integ_index_mod',
  335544541 : 'isc_check_trig_del',
  335544542 : 'isc_check_trig_update',
  335544543 : 'isc_cnstrnt_fld_del',
  335544544 : 'isc_cnstrnt_fld_rename',
  335544545 : 'isc_rel_cnstrnt_update',
  335544546 : 'isc_constaint_on_view',
  335544547 : 'isc_invld_cnstrnt_type',
  335544548 : 'isc_primary_key_exists',
  335544549 : 'isc_systrig_update',
  335544550 : 'isc_not_rel_owner',
  335544551 : 'isc_grant_obj_notfound',
  335544552 : 'isc_grant_fld_notfound',
  335544553 : 'isc_grant_nopriv',
  335544554 : 'isc_nonsql_security_rel',
  335544555 : 'isc_nonsql_security_fld',
  335544556 : 'isc_wal_cache_err',
  335544557 : 'isc_shutfail',
  335544558 : 'isc_check_constraint',
  335544559 : 'isc_bad_svc_handle',
  335544560 : 'isc_shutwarn',
  335544561 : 'isc_wrospbver',
  335544562 : 'isc_bad_spb_form',
  335544563 : 'isc_svcnotdef',
  335544564 : 'isc_no_jrn',
  335544565 : 'isc_transliteration_failed',
  335544566 : 'isc_start_cm_for_wal',
  335544567 : 'isc_wal_ovflow_log_required',
  335544568 : 'isc_text_subtype',
  335544569 : 'isc_dsql_error',
  335544570 : 'isc_dsql_command_err',
  335544571 : 'isc_dsql_constant_err',
  335544572 : 'isc_dsql_cursor_err',
  335544573 : 'isc_dsql_datatype_err',
  335544574 : 'isc_dsql_decl_err',
  335544575 : 'isc_dsql_cursor_update_err',
  335544576 : 'isc_dsql_cursor_open_err',
  335544577 : 'isc_dsql_cursor_close_err',
  335544578 : 'isc_dsql_field_err',
  335544579 : 'isc_dsql_internal_err',
  335544580 : 'isc_dsql_relation_err',
  335544581 : 'isc_dsql_procedure_err',
  335544582 : 'isc_dsql_request_err',
  335544583 : 'isc_dsql_sqlda_err',
  335544584 : 'isc_dsql_var_count_err',
  335544585 : 'isc_dsql_stmt_handle',
  335544586 : 'isc_dsql_function_err',
  335544587 : 'isc_dsql_blob_err',
  335544588 : 'isc_collation_not_found',
  335544589 : 'isc_collation_not_for_charset',
  335544590 : 'isc_dsql_dup_option',
  335544591 : 'isc_dsql_tran_err',
  335544592 : 'isc_dsql_invalid_array',
  335544593 : 'isc_dsql_max_arr_dim_exceeded',
  335544594 : 'isc_dsql_arr_range_error',
  335544595 : 'isc_dsql_trigger_err',
  335544596 : 'isc_dsql_subselect_err',
  335544597 : 'isc_dsql_crdb_prepare_err',
  335544598 : 'isc_specify_field_err',
  335544599 : 'isc_num_field_err',
  335544600 : 'isc_col_name_err',
  335544601 : 'isc_where_err',
  335544602 : 'isc_table_view_err',
  335544603 : 'isc_distinct_err',
  335544604 : 'isc_key_field_count_err',
  335544605 : 'isc_subquery_err',
  335544606 : 'isc_expression_eval_err',
  335544607 : 'isc_node_err',
  335544608 : 'isc_command_end_err',
  335544609 : 'isc_index_name',
  335544610 : 'isc_exception_name',
  335544611 : 'isc_field_name',
  335544612 : 'isc_token_err',
  335544613 : 'isc_union_err',
  335544614 : 'isc_dsql_construct_err',
  335544615 : 'isc_field_aggregate_err',
  335544616 : 'isc_field_ref_err',
  335544617 : 'isc_order_by_err',
  335544618 : 'isc_return_mode_err',
  335544619 : 'isc_extern_func_err',
  335544620 : 'isc_alias_conflict_err',
  335544621 : 'isc_procedure_conflict_error',
  335544622 : 'isc_relation_conflict_err',
  335544623 : 'isc_dsql_domain_err',
  335544624 : 'isc_idx_seg_err',
  335544625 : 'isc_node_name_err',
  335544626 : 'isc_table_name',
  335544627 : 'isc_proc_name',
  335544628 : 'isc_idx_create_err',
  335544629 : 'isc_wal_shadow_err',
  335544630 : 'isc_dependency',
  335544631 : 'isc_idx_key_err',
  335544632 : 'isc_dsql_file_length_err',
  335544633 : 'isc_dsql_shadow_number_err',
  335544634 : 'isc_dsql_token_unk_err',
  335544635 : 'isc_dsql_no_relation_alias',
  335544636 : 'isc_indexname',
  335544637 : 'isc_no_stream_plan',
  335544638 : 'isc_stream_twice',
  335544639 : 'isc_stream_not_found',
  335544640 : 'isc_collation_requires_text',
  335544641 : 'isc_dsql_domain_not_found',
  335544642 : 'isc_index_unused',
  335544643 : 'isc_dsql_self_join',
  335544644 : 'isc_stream_bof',
  335544645 : 'isc_stream_crack',
  335544646 : 'isc_db_or_file_exists',
  335544647 : 'isc_invalid_operator',
  335544648 : 'isc_conn_lost',
  335544649 : 'isc_bad_checksum',
  335544650 : 'isc_page_type_err',
  335544651 : 'isc_ext_readonly_err',
  335544652 : 'isc_sing_select_err',
  335544653 : 'isc_psw_attach',
  335544654 : 'isc_psw_start_trans',
  335544655 : 'isc_invalid_direction',
  335544656 : 'isc_dsql_var_conflict',
  335544657 : 'isc_dsql_no_blob_array',
  335544658 : 'isc_dsql_base_table',
  335544659 : 'isc_duplicate_base_table',
  335544660 : 'isc_view_alias',
  335544661 : 'isc_index_root_page_full',
  335544662 : 'isc_dsql_blob_type_unknown',
  335544663 : 'isc_req_max_clones_exceeded',
  335544664 : 'isc_dsql_duplicate_spec',
  335544665 : 'isc_unique_key_violation',
  335544666 : 'isc_srvr_version_too_old',
  335544667 : 'isc_drdb_completed_with_errs',
  335544668 : 'isc_dsql_procedure_use_err',
  335544669 : 'isc_dsql_count_mismatch',
  335544670 : 'isc_blob_idx_err',
  335544671 : 'isc_array_idx_err',
  335544672 : 'isc_key_field_err',
  335544673 : 'isc_no_delete',
  335544674 : 'isc_del_last_field',
  335544675 : 'isc_sort_err',
  335544676 : 'isc_sort_mem_err',
  335544677 : 'isc_version_err',
  335544678 : 'isc_inval_key_posn',
  335544679 : 'isc_no_segments_err',
  335544680 : 'isc_crrp_data_err',
  335544681 : 'isc_rec_size_err',
  335544682 : 'isc_dsql_field_ref',
  335544683 : 'isc_req_depth_exceeded',
  335544684 : 'isc_no_field_access',
  335544685 : 'isc_no_dbkey',
  335544686 : 'isc_jrn_format_err',
  335544687 : 'isc_jrn_file_full',
  335544688 : 'isc_dsql_open_cursor_request',
  335544689 : 'isc_ib_error',
  335544690 : 'isc_cache_redef',
  335544691 : 'isc_cache_too_small',
  335544692 : 'isc_log_redef',
  335544693 : 'isc_log_too_small',
  335544694 : 'isc_partition_too_small',
  335544695 : 'isc_partition_not_supp',
  335544696 : 'isc_log_length_spec',
  335544697 : 'isc_precision_err',
  335544698 : 'isc_scale_nogt',
  335544699 : 'isc_expec_int',
  335544700 : 'isc_expec_long',
  335544701 : 'isc_expec_uint',
  335544702 : 'isc_like_escape_invalid',
  335544703 : 'isc_svcnoexe',
  335544704 : 'isc_net_lookup_err',
  335544705 : 'isc_service_unknown',
  335544706 : 'isc_host_unknown',
  335544707 : 'isc_grant_nopriv_on_base',
  335544708 : 'isc_dyn_fld_ambiguous',
  335544709 : 'isc_dsql_agg_ref_err',
  335544710 : 'isc_complex_view',
  335544711 : 'isc_unprepared_stmt',
  335544712 : 'isc_expec_positive',
  335544713 : 'isc_dsql_sqlda_value_err',
  335544714 : 'isc_invalid_array_id',
  335544715 : 'isc_extfile_uns_op',
  335544716 : 'isc_svc_in_use',
  335544717 : 'isc_err_stack_limit',
  335544718 : 'isc_invalid_key',
  335544719 : 'isc_net_init_error',
  335544720 : 'isc_loadlib_failure',
  335544721 : 'isc_network_error',
  335544722 : 'isc_net_connect_err',
  335544723 : 'isc_net_connect_listen_err',
  335544724 : 'isc_net_event_connect_err',
  335544725 : 'isc_net_event_listen_err',
  335544726 : 'isc_net_read_err',
  335544727 : 'isc_net_write_err',
  335544728 : 'isc_integ_index_deactivate',
  335544729 : 'isc_integ_deactivate_primary',
  335544730 : 'isc_cse_not_supported',
  335544731 : 'isc_tra_must_sweep',
  335544732 : 'isc_unsupported_network_drive',
  335544733 : 'isc_io_create_err',
  335544734 : 'isc_io_open_err',
  335544735 : 'isc_io_close_err',
  335544736 : 'isc_io_read_err',
  335544737 : 'isc_io_write_err',
  335544738 : 'isc_io_delete_err',
  335544739 : 'isc_io_access_err',
  335544740 : 'isc_udf_exception',
  335544741 : 'isc_lost_db_connection',
  335544742 : 'isc_no_write_user_priv',
  335544743 : 'isc_token_too_long',
  335544744 : 'isc_max_att_exceeded',
  335544745 : 'isc_login_same_as_role_name',
  335544746 : 'isc_reftable_requires_pk',
  335544747 : 'isc_usrname_too_long',
  335544748 : 'isc_password_too_long',
  335544749 : 'isc_usrname_required',
  335544750 : 'isc_password_required',
  335544751 : 'isc_bad_protocol',
  335544752 : 'isc_dup_usrname_found',
  335544753 : 'isc_usrname_not_found',
  335544754 : 'isc_error_adding_sec_record',
  335544755 : 'isc_error_modifying_sec_record',
  335544756 : 'isc_error_deleting_sec_record',
  335544757 : 'isc_error_updating_sec_db',
  335544758 : 'isc_sort_rec_size_err',
  335544759 : 'isc_bad_default_value',
  335544760 : 'isc_invalid_clause',
  335544761 : 'isc_too_many_handles',
  335544762 : 'isc_optimizer_blk_exc',
  335544763 : 'isc_invalid_string_constant',
  335544764 : 'isc_transitional_date',
  335544765 : 'isc_read_only_database',
  335544766 : 'isc_must_be_dialect_2_and_up',
  335544767 : 'isc_blob_filter_exception',
  335544768 : 'isc_exception_access_violation',
  335544769 : 'isc_exception_datatype_missalignment',
  335544770 : 'isc_exception_array_bounds_exceeded',
  335544771 : 'isc_exception_float_denormal_operand',
  335544772 : 'isc_exception_float_divide_by_zero',
  335544773 : 'isc_exception_float_inexact_result',
  335544774 : 'isc_exception_float_invalid_operand',
  335544775 : 'isc_exception_float_overflow',
  335544776 : 'isc_exception_float_stack_check',
  335544777 : 'isc_exception_float_underflow',
  335544778 : 'isc_exception_integer_divide_by_zero',
  335544779 : 'isc_exception_integer_overflow',
  335544780 : 'isc_exception_unknown',
  335544781 : 'isc_exception_stack_overflow',
  335544782 : 'isc_exception_sigsegv',
  335544783 : 'isc_exception_sigill',
  335544784 : 'isc_exception_sigbus',
  335544785 : 'isc_exception_sigfpe',
  335544786 : 'isc_ext_file_delete',
  335544787 : 'isc_ext_file_modify',
  335544788 : 'isc_adm_task_denied',
  335544789 : 'isc_extract_input_mismatch',
  335544790 : 'isc_insufficient_svc_privileges',
  335544791 : 'isc_file_in_use',
  335544792 : 'isc_service_att_err',
  335544793 : 'isc_ddl_not_allowed_by_db_sql_dial',
  335544794 : 'isc_cancelled',
  335544795 : 'isc_unexp_spb_form',
  335544796 : 'isc_sql_dialect_datatype_unsupport',
  335544797 : 'isc_svcnouser',
  335544798 : 'isc_depend_on_uncommitted_rel',
  335544799 : 'isc_svc_name_missing',
  335544800 : 'isc_too_many_contexts',
  335544801 : 'isc_datype_notsup',
  335544802 : 'isc_dialect_reset_warning',
  335544803 : 'isc_dialect_not_changed',
  335544804 : 'isc_database_create_failed',
  335544805 : 'isc_inv_dialect_specified',
  335544806 : 'isc_valid_db_dialects',
  335544807 : 'isc_sqlwarn',
  335544808 : 'isc_dtype_renamed',
  335544809 : 'isc_extern_func_dir_error',
  335544810 : 'isc_date_range_exceeded',
  335544811 : 'isc_inv_client_dialect_specified',
  335544812 : 'isc_valid_client_dialects',
  335544813 : 'isc_optimizer_between_err',
  335544814 : 'isc_service_not_supported',
  335544815 : 'isc_generator_name',
  335544816 : 'isc_udf_name',
  335544817 : 'isc_bad_limit_param',
  335544818 : 'isc_bad_skip_param',
  335544819 : 'isc_io_32bit_exceeded_err',
  335544820 : 'isc_invalid_savepoint',
  335544821 : 'isc_dsql_column_pos_err',
  335544822 : 'isc_dsql_agg_where_err',
  335544823 : 'isc_dsql_agg_group_err',
  335544824 : 'isc_dsql_agg_column_err',
  335544825 : 'isc_dsql_agg_having_err',
  335544826 : 'isc_dsql_agg_nested_err',
  335544827 : 'isc_exec_sql_invalid_arg',
  335544828 : 'isc_exec_sql_invalid_req',
  335544829 : 'isc_exec_sql_invalid_var',
  335544830 : 'isc_exec_sql_max_call_exceeded',
  335544831 : 'isc_conf_access_denied',
  335740929 : 'isc_gfix_db_name',
  335740930 : 'isc_gfix_invalid_sw',
  335740932 : 'isc_gfix_incmp_sw',
  335740933 : 'isc_gfix_replay_req',
  335740934 : 'isc_gfix_pgbuf_req',
  335740935 : 'isc_gfix_val_req',
  335740936 : 'isc_gfix_pval_req',
  335740937 : 'isc_gfix_trn_req',
  335740940 : 'isc_gfix_full_req',
  335740941 : 'isc_gfix_usrname_req',
  335740942 : 'isc_gfix_pass_req',
  335740943 : 'isc_gfix_subs_name',
  335740944 : 'isc_gfix_wal_req',
  335740945 : 'isc_gfix_sec_req',
  335740946 : 'isc_gfix_nval_req',
  335740947 : 'isc_gfix_type_shut',
  335740948 : 'isc_gfix_retry',
  335740951 : 'isc_gfix_retry_db',
  335740991 : 'isc_gfix_exceed_max',
  335740992 : 'isc_gfix_corrupt_pool',
  335740993 : 'isc_gfix_mem_exhausted',
  335740994 : 'isc_gfix_bad_pool',
  335740995 : 'isc_gfix_trn_not_valid',
  335741012 : 'isc_gfix_unexp_eoi',
  335741018 : 'isc_gfix_recon_fail',
  335741036 : 'isc_gfix_trn_unknown',
  335741038 : 'isc_gfix_mode_req',
  335741039 : 'isc_gfix_opt_SQL_dialect',
  336003074 : 'isc_dsql_dbkey_from_non_table',
  336003075 : 'isc_dsql_transitional_numeric',
  336003076 : 'isc_dsql_dialect_warning_expr',
  336003077 : 'isc_sql_db_dialect_dtype_unsupport',
  336003079 : 'isc_isc_sql_dialect_conflict_num',
  336003080 : 'isc_dsql_warning_number_ambiguous',
  336003081 : 'isc_dsql_warning_number_ambiguous1',
  336003082 : 'isc_dsql_warn_precision_ambiguous',
  336003083 : 'isc_dsql_warn_precision_ambiguous1',
  336003084 : 'isc_dsql_warn_precision_ambiguous2',
  336068796 : 'isc_dyn_role_does_not_exist',
  336068797 : 'isc_dyn_no_grant_admin_opt',
  336068798 : 'isc_dyn_user_not_role_member',
  336068799 : 'isc_dyn_delete_role_failed',
  336068800 : 'isc_dyn_grant_role_to_user',
  336068801 : 'isc_dyn_inv_sql_role_name',
  336068802 : 'isc_dyn_dup_sql_role',
  336068803 : 'isc_dyn_kywd_spec_for_role',
  336068804 : 'isc_dyn_roles_not_supported',
  336068812 : 'isc_dyn_domain_name_exists',
  336068813 : 'isc_dyn_field_name_exists',
  336068814 : 'isc_dyn_dependency_exists',
  336068815 : 'isc_dyn_dtype_invalid',
  336068816 : 'isc_dyn_char_fld_too_small',
  336068817 : 'isc_dyn_invalid_dtype_conversion',
  336068818 : 'isc_dyn_dtype_conv_invalid',
  336330753 : 'isc_gbak_unknown_switch',
  336330754 : 'isc_gbak_page_size_missing',
  336330755 : 'isc_gbak_page_size_toobig',
  336330756 : 'isc_gbak_redir_ouput_missing',
  336330757 : 'isc_gbak_switches_conflict',
  336330758 : 'isc_gbak_unknown_device',
  336330759 : 'isc_gbak_no_protection',
  336330760 : 'isc_gbak_page_size_not_allowed',
  336330761 : 'isc_gbak_multi_source_dest',
  336330762 : 'isc_gbak_filename_missing',
  336330763 : 'isc_gbak_dup_inout_names',
  336330764 : 'isc_gbak_inv_page_size',
  336330765 : 'isc_gbak_db_specified',
  336330766 : 'isc_gbak_db_exists',
  336330767 : 'isc_gbak_unk_device',
  336330772 : 'isc_gbak_blob_info_failed',
  336330773 : 'isc_gbak_unk_blob_item',
  336330774 : 'isc_gbak_get_seg_failed',
  336330775 : 'isc_gbak_close_blob_failed',
  336330776 : 'isc_gbak_open_blob_failed',
  336330777 : 'isc_gbak_put_blr_gen_id_failed',
  336330778 : 'isc_gbak_unk_type',
  336330779 : 'isc_gbak_comp_req_failed',
  336330780 : 'isc_gbak_start_req_failed',
  336330781 : 'isc_gbak_rec_failed',
  336330782 : 'isc_gbak_rel_req_failed',
  336330783 : 'isc_gbak_db_info_failed',
  336330784 : 'isc_gbak_no_db_desc',
  336330785 : 'isc_gbak_db_create_failed',
  336330786 : 'isc_gbak_decomp_len_error',
  336330787 : 'isc_gbak_tbl_missing',
  336330788 : 'isc_gbak_blob_col_missing',
  336330789 : 'isc_gbak_create_blob_failed',
  336330790 : 'isc_gbak_put_seg_failed',
  336330791 : 'isc_gbak_rec_len_exp',
  336330792 : 'isc_gbak_inv_rec_len',
  336330793 : 'isc_gbak_exp_data_type',
  336330794 : 'isc_gbak_gen_id_failed',
  336330795 : 'isc_gbak_unk_rec_type',
  336330796 : 'isc_gbak_inv_bkup_ver',
  336330797 : 'isc_gbak_missing_bkup_desc',
  336330798 : 'isc_gbak_string_trunc',
  336330799 : 'isc_gbak_cant_rest_record',
  336330800 : 'isc_gbak_send_failed',
  336330801 : 'isc_gbak_no_tbl_name',
  336330802 : 'isc_gbak_unexp_eof',
  336330803 : 'isc_gbak_db_format_too_old',
  336330804 : 'isc_gbak_inv_array_dim',
  336330807 : 'isc_gbak_xdr_len_expected',
  336330817 : 'isc_gbak_open_bkup_error',
  336330818 : 'isc_gbak_open_error',
  336330934 : 'isc_gbak_missing_block_fac',
  336330935 : 'isc_gbak_inv_block_fac',
  336330936 : 'isc_gbak_block_fac_specified',
  336330940 : 'isc_gbak_missing_username',
  336330941 : 'isc_gbak_missing_password',
  336330952 : 'isc_gbak_missing_skipped_bytes',
  336330953 : 'isc_gbak_inv_skipped_bytes',
  336330965 : 'isc_gbak_err_restore_charset',
  336330967 : 'isc_gbak_err_restore_collation',
  336330972 : 'isc_gbak_read_error',
  336330973 : 'isc_gbak_write_error',
  336330985 : 'isc_gbak_db_in_use',
  336330990 : 'isc_gbak_sysmemex',
  336331002 : 'isc_gbak_restore_role_failed',
  336331005 : 'isc_gbak_role_op_missing',
  336331010 : 'isc_gbak_page_buffers_missing',
  336331011 : 'isc_gbak_page_buffers_wrong_param',
  336331012 : 'isc_gbak_page_buffers_restore',
  336331014 : 'isc_gbak_inv_size',
  336331015 : 'isc_gbak_file_outof_sequence',
  336331016 : 'isc_gbak_join_file_missing',
  336331017 : 'isc_gbak_stdin_not_supptd',
  336331018 : 'isc_gbak_stdout_not_supptd',
  336331019 : 'isc_gbak_bkup_corrupt',
  336331020 : 'isc_gbak_unk_db_file_spec',
  336331021 : 'isc_gbak_hdr_write_failed',
  336331022 : 'isc_gbak_disk_space_ex',
  336331023 : 'isc_gbak_size_lt_min',
  336331025 : 'isc_gbak_svc_name_missing',
  336331026 : 'isc_gbak_not_ownr',
  336331031 : 'isc_gbak_mode_req',
  336723983 : 'isc_gsec_cant_open_db',
  336723984 : 'isc_gsec_switches_error',
  336723985 : 'isc_gsec_no_op_spec',
  336723986 : 'isc_gsec_no_usr_name',
  336723987 : 'isc_gsec_err_add',
  336723988 : 'isc_gsec_err_modify',
  336723989 : 'isc_gsec_err_find_mod',
  336723990 : 'isc_gsec_err_rec_not_found',
  336723991 : 'isc_gsec_err_delete',
  336723992 : 'isc_gsec_err_find_del',
  336723996 : 'isc_gsec_err_find_disp',
  336723997 : 'isc_gsec_inv_param',
  336723998 : 'isc_gsec_op_specified',
  336723999 : 'isc_gsec_pw_specified',
  336724000 : 'isc_gsec_uid_specified',
  336724001 : 'isc_gsec_gid_specified',
  336724002 : 'isc_gsec_proj_specified',
  336724003 : 'isc_gsec_org_specified',
  336724004 : 'isc_gsec_fname_specified',
  336724005 : 'isc_gsec_mname_specified',
  336724006 : 'isc_gsec_lname_specified',
  336724008 : 'isc_gsec_inv_switch',
  336724009 : 'isc_gsec_amb_switch',
  336724010 : 'isc_gsec_no_op_specified',
  336724011 : 'isc_gsec_params_not_allowed',
  336724012 : 'isc_gsec_incompat_switch',
  336724044 : 'isc_gsec_inv_username',
  336724045 : 'isc_gsec_inv_pw_length',
  336724046 : 'isc_gsec_db_specified',
  336724047 : 'isc_gsec_db_admin_specified',
  336724048 : 'isc_gsec_db_admin_pw_specified',
  336724049 : 'isc_gsec_sql_role_specified',
  336789504 : 'isc_license_no_file',
  336789523 : 'isc_license_op_specified',
  336789524 : 'isc_license_op_missing',
  336789525 : 'isc_license_inv_switch',
  336789526 : 'isc_license_inv_switch_combo',
  336789527 : 'isc_license_inv_op_combo',
  336789528 : 'isc_license_amb_switch',
  336789529 : 'isc_license_inv_parameter',
  336789530 : 'isc_license_param_specified',
  336789531 : 'isc_license_param_req',
  336789532 : 'isc_license_syntx_error',
  336789534 : 'isc_license_dup_id',
  336789535 : 'isc_license_inv_id_key',
  336789536 : 'isc_license_err_remove',
  336789537 : 'isc_license_err_update',
  336789538 : 'isc_license_err_convert',
  336789539 : 'isc_license_err_unk',
  336789540 : 'isc_license_svc_err_add',
  336789541 : 'isc_license_svc_err_remove',
  336789563 : 'isc_license_eval_exists',
  336920577 : 'isc_gstat_unknown_switch',
  336920578 : 'isc_gstat_retry',
  336920579 : 'isc_gstat_wrong_ods',
  336920580 : 'isc_gstat_unexpected_eof',
  336920605 : 'isc_gstat_open_err',
  336920606 : 'isc_gstat_read_err',
  336920607 : 'isc_gstat_sysmemex',
}

op_names = [
  'op_void', 'op_connect', 'op_exit', 'op_accept', 'op_reject',
  'op_protocol', 'op_disconnect', 'op_credit', 'op_continuation', 
  'op_response', 'op_open_file', 'op_create_file', 'op_close_file', 
  'op_read_page', 'op_write_page', 'op_lock', 'op_convert_lock', 
  'op_release_lock', 'op_blocking', 'op_attach', 'op_create', 'op_detach', 
  'op_compile', 'op_start', 'op_start_and_send', 'op_send', 'op_receive',
  'op_unwind', 'op_release', 'op_transaction', 'op_commit', 'op_rollback',
  'op_prepare', 'op_reconnect', 'op_create_blob', 'op_open_blob',
  'op_get_segment', 'op_put_segment', 'op_cancel_blob', 'op_close_blob',
  'op_info_database', 'op_info_request', 'op_info_transaction', 'op_info_blob',
  'op_batch_segments', 'op_mgr_set_affinity', 'op_mgr_clear_affinity',
  'op_mgr_report', 'op_que_events', 'op_cancel_events', 'op_commit_retaining',
  'op_prepare2', 'op_event', 'op_connect_request', 'op_aux_connect', 'op_ddl',
  'op_open_blob2', 'op_create_blob2', 'op_get_slice', 'op_put_slice',
  'op_slice', 'op_seek_blob', 'op_allocate_statement', 'op_execute',
  'op_execute_immediate', 'op_fetch', 'op_fetch_response', 'op_free_statement',
  'op_prepare_statement', 'op_set_cursor', 'op_info_sql', 'op_dummy',
  'op_response_piggyback', 'op_start_and_receive', 'op_start_send_and_receive',
  'op_execute_immediate2', 'op_execute2', 'op_insert', 'op_sql_response',
  'op_transact', 'op_transact_response', 'op_drop_database',
  'op_service_attach', 'op_service_detach', 'op_service_info',
  'op_service_start', 'op_rollback_retaining',
  # FB3
  'op_update_account_info', 'op_authenticate_user', 'op_partial',
  'op_trusted_auth', 'op_cancel', 'op_cont_auth', 'op_ping', 'op_accept_data',
  'op_abort_aux_connection', 'op_crypt', 'op_crypt_key_callback',
  'op_cond_accept',
]

isc_dpb_names = [
  None, 'isc_dpb_version1', 'isc_dpb_allocation', 'isc_dpb_journal',
  'isc_dpb_page_size', 'isc_dpb_num_buffers', 'isc_dpb_buffer_length',
  'isc_dpb_debug', 'isc_dpb_garbage_collect', 'isc_dpb_verify',
  'isc_dpb_sweep', 'isc_dpb_enable_journal', 'isc_dpb_disable_journal',
  'isc_dpb_dbkey_scope', 'isc_dpb_number_of_users', 'isc_dpb_trace',
  'isc_dpb_no_garbage_collect', 'isc_dpb_damaged', 'isc_dpb_license',
  'isc_dpb_sys_user_name', 'isc_dpb_encrypt_key', 'isc_dpb_activate_shadow',
  'isc_dpb_sweep_interval', 'isc_dpb_delete_shadow', 'isc_dpb_force_write',
  'isc_dpb_begin_log', 'isc_dpb_quit_log', 'isc_dpb_no_reserve',
  'isc_dpb_user_name', 'isc_dpb_password', 'isc_dpb_password_enc',
  'isc_dpb_sys_user_name_enc', 'isc_dpb_interp', 'isc_dpb_online_dump',
  'isc_dpb_old_file_size', 'isc_dpb_old_num_files', 'isc_dpb_old_file',
  'isc_dpb_old_start_page', 'isc_dpb_old_start_seqno', 
  'isc_dpb_old_start_file', 'isc_dpb_drop_walfile', 'isc_dpb_old_dump_id',
  'isc_dpb_wal_backup_dir', 'isc_dpb_wal_chkptlen', 'isc_dpb_wal_numbufs',
  'isc_dpb_wal_bufsize', 'isc_dpb_wal_grp_cmt_wait', 'isc_dpb_lc_messages',
  'isc_dpb_lc_ctype', 'isc_dpb_cache_manager', 'isc_dpb_shutdown',
  'isc_dpb_online', 'isc_dpb_shutdown_delay', 'isc_dpb_reserved',
  'isc_dpb_overwrite', 'isc_dpb_sec_attach', 'isc_dpb_disable_wal',
  'isc_dpb_connect_timeout', 'isc_dpb_dummy_packet_interval',
  'isc_dpb_gbak_attach', 'isc_dpb_sql_role_name', 'isc_dpb_set_page_buffers',
  'isc_dpb_working_directory', 'isc_dpb_sql_dialect',
  'isc_dpb_set_db_readonly', 'isc_dpb_set_db_sql_dialect',
  'isc_dpb_gfix_attach', 'isc_dpb_gstat_attach', 'isc_dpb_set_db_charset',
  'isc_dpb_gsec_attach', 'isc_dpb_address_path', 'isc_dpb_process_id',
  'isc_dpb_no_db_triggers', 'isc_dpb_trusted_auth', 'isc_dpb_process_name',
  'isc_dpb_trusted_role', 'isc_dpb_org_filename', 'isc_dpb_utf8_filename',
  'isc_dpb_ext_call_depth', 'isc_dpb_auth_block', 'isc_dpb_client_version',
  'isc_dpb_remote_protocol', 'isc_dpb_host_name', 'isc_dpb_os_user',
  'isc_dpb_specific_auth_data', 'isc_dpb_auth_plugin_list',
  'isc_dpb_auth_plugin_name', 'isc_dpb_config', 'isc_dpb_nolinger',
]

isc_spb_names = {
  1 : 'isc_spb_version1', 2 : 'isc_spb_current_version',
  3 : 'isc_spb_rpr_validate_db|isc_spb_rpr_sweep_db',
  5 : 'isc_spb_bkp_file', 7 : 'isc_spb_bkp_length', 
  9 : 'isc_spb_res_buffers', 10 : 'isc_spb_res_page_size',
  11 : 'isc_spb_res_length',
  12 : 'isc_action_svc_get_fb_log',
  19 : 'isc_spb_sys_user_name',
  28 : 'isc_spb_user_name', 29 : 'isc_spb_password',
  30 : 'isc_spb_password_enc', 31 : 'isc_spb_sys_user_name_enc',
  105 : 'isc_spb_command_line', 106 : 'isc_spb_dbname', 
  107 : 'isc_spb_verbose', 108 : 'isc_spb_options',
}

isc_req_info_names =[
  None, 'isc_info_end', None, None, 'isc_info_number_messages', 
  'isc_info_max_message', 'isc_info_max_send', 'isc_info_max_receive', 
  'isc_info_state', 'isc_info_message_number', 'isc_info_message_size', 
  'isc_info_request_cost', 'isc_info_access_path', 'isc_info_req_select_count',
  'isc_info_req_insert_count', 'isc_info_req_update_count', 
  'isc_info_req_delete_count',
]

isc_info_names = [
  None, 'isc_info_end', 'isc_info_truncated', 'isc_info_error', 
  'isc_info_db_id', 'isc_info_reads',
  'isc_info_writes', 'isc_info_fetches', 'isc_info_marks', None, None,
  'isc_info_implementation', 'isc_info_isc_version', 'isc_info_base_level',
  'isc_info_page_size', 'isc_info_num_buffers', 'isc_info_limbo',
  'isc_info_current_memory', 'isc_info_max_memory', 'isc_info_window_turns',
  'isc_info_license', 'isc_info_allocation', 'isc_info_attachment_id',
  'isc_info_read_seq_count', 'isc_info_read_idx_count',
  'isc_info_insert_count', 'isc_info_update_count', 'isc_info_delete_count',
  'isc_info_backout_count', 'isc_info_purge_count', 'isc_info_expunge_count',
  'isc_info_sweep_interval', 'isc_info_ods_version',
  'isc_info_ods_minor_version', 'isc_info_no_reserve', 'isc_info_logfile',
  'isc_info_cur_logfile_name', 'isc_info_cur_log_part_offset',
  'isc_info_num_wal_buffers', 'isc_info_wal_buffer_size',
  'isc_info_wal_ckpt_length', 'isc_info_wal_cur_ckpt_interval',
  'isc_info_wal_prv_ckpt_fname', 'isc_info_wal_prv_ckpt_poffset',
  'isc_info_wal_recv_ckpt_fname', 'isc_info_wal_recv_ckpt_poffset', None,
  'isc_info_wal_grpc_wait_usecs', 'isc_info_wal_num_io',
  'isc_info_wal_avg_io_size', 'isc_info_wal_num_commits',
  'isc_info_wal_avg_grpc_size', 'isc_info_forced_writes',
  'isc_info_user_names', 'isc_info_page_errors',
  'isc_info_record_errors', 'isc_info_bpage_errors',
  'isc_info_dpage_errors', 'isc_info_ipage_errors',
  'isc_info_ppage_errors', 'isc_info_tpage_errors',
  'isc_info_set_page_buffers', 'isc_info_db_sql_dialect',
  'isc_info_db_read_only', 'isc_info_db_size_in_pages',
]
for i in range(len(isc_info_names), 101): # 65-100 no use
    isc_info_names.append(None)
isc_info_names += [
  'frb_info_att_charset', 'isc_info_db_class', 'isc_info_firebird_version',
  'isc_info_oldest_transaction', 'isc_info_oldest_active',
  'isc_info_oldest_snapshot', 'isc_info_next_transaction',
  'isc_info_db_provider', 'isc_info_active_transactions',
  'isc_info_active_tran_count', 'isc_info_creation_date',
  'isc_info_db_file_size',
]

isc_tpb_names = [
  None, 'isc_tpb_version1', 'isc_tpb_concurrency', 'isc_tpb_version3',
  'isc_tpb_protected', 'isc_tpb_exclusive', 'isc_tpb_wait', 'isc_tpb_nowait',
  'isc_tpb_read', 'isc_tpb_write', 'isc_tpb_lock_read', 'isc_tpb_lock_write',
  'isc_tpb_verb_time', 'isc_tpb_commit_time', 'isc_tpb_ignore_limbo',
  'isc_tpb_read_committed', 'isc_tpb_autocommit', 'isc_tpb_rec_version',
  'isc_tpb_no_rec_version', 'isc_tpb_restart_requests', 'isc_tpb_no_auto_undo',
]
    
isc_info_sql_names = [
  None, 'isc_info_end', 'isc_info_truncated', 'isc_info_error', 
  'isc_info_sql_select', 'isc_info_sql_bind',
  'isc_info_sql_num_variables', 'isc_info_sql_describe_vars',
  'isc_info_sql_describe_end', 'isc_info_sql_sqlda_seq',
  'isc_info_sql_message_seq', 'isc_info_sql_type', 'isc_info_sql_sub_type',
  'isc_info_sql_scale', 'isc_info_sql_length', 'isc_info_sql_null_ind',
  'isc_info_sql_field', 'isc_info_sql_relation', 'isc_info_sql_owner',
  'isc_info_sql_alias', 'isc_info_sql_sqlda_start', 'isc_info_sql_stmt_type',
  'isc_info_sql_get_plan', 'isc_info_sql_records', 'isc_info_sql_batch_fetch',
]

isc_info_sql_stmt_names = [
  None, 'isc_info_sql_stmt_select', 'isc_info_sql_stmt_insert',
  'isc_info_sql_stmt_update', 'isc_info_sql_stmt_delete',
  'isc_info_sql_stmt_ddl', 'isc_info_sql_stmt_get_segment',
  'isc_info_sql_stmt_put_segment', 'isc_info_sql_stmt_exec_procedure',
  'isc_info_sql_stmt_start_trans', 'isc_info_sql_stmt_commit',
  'isc_info_sql_stmt_rollback', 'isc_info_sql_stmt_select_for_upd',
  'isc_info_sql_stmt_set_generator', 'isc_info_sql_stmt_savepoint',
]


isc_status_names = [
  'isc_arg_end', 'isc_arg_gds', 'isc_arg_string', 'isc_arg_cstring',
  'isc_arg_number', 'isc_arg_interpreted', 'isc_arg_vms', 'isc_arg_unix',
  'isc_arg_domain', 'isc_arg_dos', 'isc_arg_mpexl', 'isc_arg_mpexl_ipc',
  None, None, None, 
  'isc_arg_next_mach', 'isc_arg_netware', 'isc_arg_win32', 'isc_arg_warning',
  'isc_arg_sql_state',
]

type_names = {
  452:'SQL_TEXT', 448:'SQL_VARYING', 500:'SQL_SHORT', 496:'SQL_LONG',
  482:'SQL_FLOAT', 480:'SQL_DOUBLE', 530:'SQL_D_FLOAT', 510:'SQL_TIMESTAMP',
  520:'SQL_BLOB', 540:'SQL_ARRAY', 550:'SQL_QUAD', 560:'SQL_TIME',
  570:'SQL_DATE', 580:'SQL_INT64',
  32764:'SQL_BOOLEAN', 32766: 'SQL_NULL',
}

CNCT_names = [
  None, 'CNCT_user', 'CNCT_passwd', None, 'CNCT_host', 'CNCT_group',
  'CNCT_user_verification', 'CNCT_specific_data', 'CNCT_plugin_name',
  'CNCT_login', 'CNCT_plugin_list', 'CNCT_client_crypt',
]

class XSQLVar(object):
    def __init__(self):
        self.sqltype = None
        self.sqlscale = None
        self.sqlsubtype = None
        self.sqllen = None
        self.sqlnullind = None
        self.sqlname = ''
        self.relname = ''
        self.ownname = ''
        self.aliasname = ''
        self.coder = None
        self.raw_value = None
        self.null_flag = None

    def get_type_name(self):
        return type_names[self.sqltype & ~1]

    def io_length(self):
        dtype = self.get_type_name()
        if dtype == 'SQL_TEXT':
            return self.sqllen
        elif dtype == 'SQL_VARYING':
            return -1   # First 4 bytes 
        elif (dtype == 'SQL_SHORT' or dtype == 'SQL_LONG'
                or dtype == 'SQL_FLOAT' or dtype == 'SQL_TIME'
                or dtype == 'SQL_DATE'):
            return 4
        elif (dtype == 'SQL_DOUBLE' or dtype == 'SQL_TIMESTAMP' 
                or dtype == 'SQL_BLOB' or dtype == 'SQL_ARRAY' 
                or dtype == 'SQL_QUAD' or dtype == 'SQL_INT64'):
            return 8
        elif dtype == 'SQL_BOOLEAN':
            return 1

    def __str__(self):
        s  = '[' + str(self.sqltype) + ',' + str(self.sqlscale) + ',' \
                + str(self.sqlsubtype) + ',' + str(self.sqllen)  + ',' \
                + str(self.sqlnullind) + ',' \
                + str(self.sqlname) + ',' + str(self.relname) + ',' \
                + str(self.ownname) + ',' + str(self.aliasname) + ']'
        if self.raw_value != None:
            if self.null_flag:
                s += 'NULL'
            else:
                s += binascii.b2a_hex(self.raw_value)
        return s

def _need_nbytes_align(sock, nbytes): # Get nbytes with 4 bytes word alignment
    n = nbytes
    if n % 4:
        n += 4 - nbytes % 4  # 4 bytes word alignment
    r = b''
    while n:
        bs = sock.recv(n)
        r += bs
        n -= len(bs)
    return r

def _bytes_to_bint32(bs, i):    # Read as big endian int32
    v = ((_ord(bs[i]) << 24) | (_ord(bs[i+1]) << 16) 
            | (_ord(bs[i+2]) << 8) | (_ord(bs[i+3]) << 0))
    return ctypes.c_int(v).value

def _bytes_to_bint(bs, i, len): # Read as big endian
    val = 0
    n = 0
    while n < len:
        val += _ord(bs[i+n]) << (8 * (len - n -1))
        n += 1
    if _ord(bs[i]) & 128: # First byte MSB eq 1 means negative.
        val = ctypes.c_int(val).value
    return val

def _bytes_to_int(bs, i, len): # Read as little endian
    val = 0
    n = 0
    while n < len:
        val += _ord(bs[i+n]) << (8 * n)
        n += 1
    if _ord(bs[i+n-1]) & 128: # Last byte MSB eq 1 means negative.
        val = ctypes.c_int(val).value
    return val

def _bint_to_bytes(val, nbytes): # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8*(nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes -i -1] == 256:
                b[nbytes -i -1] = 0
                b[nbytes -i -2] += 1
    if PYTHON3:
        return bytes(b)
    else:
        return ''.join([chr(c) for c in b])

def _int_to_bytes(val, nbytes):  # Convert int value to little endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8 * n)) & 0xff)
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[0] += 1
        for i in range(nbytes):
            if b[i] == 256:
                b[i] = 0
                b[i+1] += 1
    return ''.join([chr(c) for c in b])


def _calc_blr(xsqlda):  # calc from sqlda to BLR format data.
    ln = len(xsqlda) * 2
    blr = [5, 2, 4, 0, ln & 255, ln >> 8]
    for x in xsqlda:
        if x.get_type_name() == 'SQL_VARYING':
            blr += [37, x.sqllen & 255, x.sqllen >> 8]
        elif x.get_type_name() == 'SQL_TEXT':
            blr += [14, x.sqllen & 255, x.sqllen >> 8]
        elif x.get_type_name() == 'SQL_DOUBLE':
            blr += [27]
        elif x.get_type_name() == 'SQL_FLOAT':
            blr += [10]
        elif x.get_type_name() == 'SQL_D_FLOAT':
            blr += [11]
        elif x.get_type_name() == 'SQL_DATE':
            blr += [12]
        elif x.get_type_name() == 'SQL_TIME':
            blr += [13]
        elif x.get_type_name() == 'SQL_TIMESTAMP':
            blr += [35]
        elif x.get_type_name() == 'SQL_BLOB':
            blr += [9, 0]
        elif x.get_type_name() == 'SQL_ARRAY':
            blr += [9, 0]
        elif x.get_type_name() == 'SQL_LONG':
            blr += [8, x.sqlscale]
        elif x.get_type_name() == 'SQL_SHORT':
            blr += [7, x.sqlscale]
        elif x.get_type_name() == 'SQL_INT64':
            blr += [16, x.sqlscale]
        elif x.get_type_name() == 'SQL_QUAD':
            blr += [9, x.sqlscale]
        elif x.get_type_name() == 'SQL_BOOLEAN':
            blr += [23]
        blr += [7, 0]   # [blr_short, 0]
    blr += [255, 76]    # [blr_end, blr_eoc]

    # x.sqlscale value is sometimes negative, so b convert to unsigned char
    if PYTHON3:
        blr = bytes([(256 + b if b < 0 else b) for b in blr])
    else:
        blr = ''.join([chr(256 + b if b < 0 else b) for b in blr])
    return blr

def _parse_param(blr, bs = None):   # Parse (bytes data with) BLR format
    assert [_ord(blr[0]), _ord(blr[1]), _ord(blr[2]), _ord(blr[3])] == [5, 2, 4, 0]
    param_len = _bytes_to_int(blr, 4, 2)
    print('\t_parse_param len =', param_len)
    i = 6
    n = 0
    r = []
    while n < param_len:
        t = _ord(blr[i])
        scale = 0
        if t == 14:
            dtype =  'SQL_TEXT'
            io_length = _bytes_to_int(blr, i+1, 2)
            i += 3
        elif t == 27:
            dtype = 'SQL_DOUBLE'
            io_length = 8
            i += 1
        elif t == 10:
            dtype = 'SQL_FLOAT'
            io_length = 4
            i += 1
        elif t == 12:
            dtype = 'SQL_DATE'
            io_length = 4
            i += 1
        elif t == 13:
            dtype = 'SQL_TIME'
            io_length = 4
            i += 1
        elif t == 35:
            dtype = 'SQL_TIMESTAMP'
            io_length = 8
            i += 1
        elif t == 9:    # SQL_BLOB or SQL_ARRAY or SQL_QUAD
            dtype =  'SQL_BLOB'
            io_length = 8
            scale = _ord(blr[i+1])
            i += 2
        elif t == 8:
            dtype = 'SQL_LONG'
            io_length = 4
            scale = _ord(blr[i+1])
            i += 2
        elif t == 7:
            dtype = 'SQL_SHORT'
            io_length = 4
            scale = _ord(blr[i+1])
            i += 2
        elif t == 16:
            dtype = 'SQL_INT64'
            io_length = 8
            scale = _ord(blr[i+1])
            i += 2
        elif t == 23:
            dtype = 'SQL_BOOLEAN'
            io_length = 1
            i += 1
        else:
            print('Unknown data type', t, i)
            assert False
        n += 1
        pad_length = ((4-io_length) & 3)
        print('\t', dtype, io_length, '+', pad_length, 'scale =', scale)
        if bs:
            print('\t[', binascii.b2a_hex(bs[:io_length]), ']')
            bs = bs[io_length + pad_length:]

    assert [_ord(blr[i]), _ord(blr[i+1])] == [255, 76]    # [blr_end, blr_eoc]

def _parse_trunc_sql_info(start, bytes, statement):
    print('\n\t<-------- start byte index =', start)
    index = 0
    i = start
    l = _bytes_to_int(bytes, i, 2)
    col_len = _bytes_to_int(bytes, i + 2, l)
    i += 2 + l
    print('\tcol_len=', col_len)
    xsqlda = get_xsqlda_statement()[statement]
    if not xsqlda:
        xsqlda = [None] * col_len

    item = isc_info_sql_names[_ord(bytes[i])]
    while item != 'isc_info_end':
        if item == 'isc_info_sql_sqlda_seq':
            l = _bytes_to_int(bytes, i + 1, 2)
            index = _bytes_to_int(bytes, i + 3, l)
            xsqlda[index-1] = XSQLVar()
            print('\t', item, index)
            i = i + 3 + l
        elif item == 'isc_info_sql_type':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqltype = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, xsqlda[index-1].sqltype, end=' ')
            print('dtype=', xsqlda[index-1].sqltype & ~1)
            i = i + 3 + l
        elif item == 'isc_info_sql_sub_type':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqlsubtype = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, xsqlda[index-1].sqlsubtype)
            i = i + 3 + l
        elif item == 'isc_info_sql_scale':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqlscale = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, xsqlda[index-1].sqlscale)
            i = i + 3 + l
        elif item == 'isc_info_sql_length':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqllen = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, xsqlda[index-1].sqllen)
            i = i + 3 + l
        elif item == 'isc_info_sql_null_ind':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqlnullind = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, xsqlda[index-1].sqlnullind)
            i = i + 3 + l
        elif item == 'isc_info_sql_field':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].sqlname = bytes[i + 3: i + 3 + l]
            print('\t', item, xsqlda[index-1].sqlname)
            i = i + 3 + l
        elif item == 'isc_info_sql_relation':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].relname = bytes[i + 3: i + 3 + l]
            print('\t', item, xsqlda[index-1].relname)
            i = i + 3 + l
        elif item == 'isc_info_sql_owner':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].ownname = bytes[i + 3: i + 3 + l]
            print('\t', item, xsqlda[index-1].ownname)
            i = i + 3 + l
        elif item == 'isc_info_sql_alias':
            l = _bytes_to_int(bytes, i + 1, 2)
            xsqlda[index-1].aliasname = bytes[i + 3: i + 3 + l]
            print('\t', item, xsqlda[index-1].aliasname)
            i = i + 3 + l
        elif item == 'isc_info_truncated':
            print('\t', item)
            break
        elif item == 'isc_info_sql_describe_end':
            print('\t', item)
            i = i + 1
        elif item == 'isc_info_sql_num_variables':
            l = _bytes_to_int(bytes, i + 1, 2)
            num_variables = _bytes_to_int(bytes, i + 3, l)
            print('\t', item, num_variables)
            i = i + 3 + l
        elif item == 'isc_info_sql_get_plan':
            l = _bytes_to_int(bytes, i + 1, 2)
            plan = bytes[i + 3: i + 3 + l]
            print('\t', item, plan)
            i = i + 3 + l
        elif item == 'isc_info_sql_bind':
            i += 1
        else:
            print('\t', item, 'Invalid item <%02x> ! i=%d' % (_ord(bytes[i]), i))
            i = i + 1
        item = isc_info_sql_names[_ord(bytes[i])]
    get_xsqlda_statement()[statement] = xsqlda
    print('\t-------->')
    return index

def _database_parameter_block(bytes):
    i = 0
    while i < len(bytes):
        n = _ord(bytes[i])
        print('\t', n, end='')
        if n == 110:
            s = 'isc_spb_process_id'
        elif n == 112:
            s = 'isc_spb_process_name'
        else:
            s = isc_dpb_names[n]
        print('\t', s, end='')
        if s in ['isc_dpb_lc_ctype', 'isc_dpb_user_name', 'isc_dpb_password',
            'isc_dpb_password_enc', 'isc_dpb_sql_role_name', 
            'isc_dpb_old_start_file', 'isc_dpb_set_db_charset',
            'isc_dpb_working_directory', 'isc_dpb_gbak_attach',
            'isc_dpb_process_name', 'isc_spb_process_name',
            'isc_dpb_utf8_filename', 'isc_dpb_client_version',
            'isc_dpb_remote_protocol', 'isc_dpb_host_name',
            'isc_dpb_os_user', 'isc_dpb_auth_plugin_name',
            'isc_dpb_auth_plugin_list', 'isc_dpb_specific_auth_data',
            ]:
            l = _ord(bytes[i+1])
            print('[', bytes[i+2:i+2+l], ']', end='')
            i = i + 2 + l
        elif s in ['isc_dpb_dummy_packet_interval', 'isc_dpb_sql_dialect', 
            'isc_dpb_sweep', 'isc_dpb_connect_timeout', 'isc_dpb_page_size', 
            'isc_dpb_force_write', 'isc_dpb_overwrite', 'isc_dpb_process_id',
            'isc_spb_process_id', 'isc_dpb_ext_call_depth',
            ]:
            l = _ord(bytes[i+1])
            print('', _bytes_to_int(bytes, i+2, l), end='')
            i = i + 2 + l
        else:
            i = i + 1
        print()

def _service_parameter_block(bs):
    i = 0
    while i < len(bs):
        print('\t', _ord(bs[i]), end='')
        s = isc_spb_names[_ord(bs[i])]
        print('\t', s, end='')
        if s in ['isc_spb_bkp_file', 'isc_spb_command_line', 
            'isc_spb_dbname']:
            l = _bytes_to_int(bs, i+1, 2)
            print('[' + bs[i+3:i+3+l] + ']')
            i += 3 + l
        elif s in ['isc_spb_bkp_length',
                'isc_spb_res_buffers', 'isc_spb_res_page_size']:
            print (_bytes_to_int(bs, i+1, 4))
            i = i + 5
        elif s in ['isc_spb_options']:
            print('[' + binascii.b2a_hex(bs[i+1:i+5]) + ']')
            i = i + 5
        else:
            print()
            i += 1

def parse_sql_info(b, statement):
    if len(b) == 0:     # Error occured.
        return
    i = 0
    while isc_info_sql_names[_ord(b[i])] != 'isc_info_end':
        if ((isc_info_sql_names[_ord(b[i])] == 'isc_info_sql_select' or
             isc_info_sql_names[_ord(b[i])] == 'isc_info_sql_bind') and
           isc_info_sql_names[_ord(b[i+1])] == 'isc_info_sql_describe_vars'):
            index = _parse_trunc_sql_info(i + 2, b, statement)
            if index:
                print('\tmore info index=', index)
            break
        print('\t' + isc_info_sql_names[_ord(b[i])], end=' ')
        if isc_info_sql_names[_ord(b[i])] == 'isc_info_sql_records':
            i += 3
            while isc_req_info_names[_ord(b[i])] != 'isc_info_end':
                print(isc_req_info_names[_ord(b[i])], end=' ')
                l = _bytes_to_int(b, i + 1, 2)
                print(_bytes_to_int(b, i + 3, l), end=' ')
                i += 3 + l
            print()
            break
        l = _bytes_to_int(b, i + 1, 2)
        n = _bytes_to_int(b, i + 3, l)
        if isc_info_sql_names[_ord(b[i])] == 'isc_info_sql_stmt_type':
            print(isc_info_sql_stmt_names[n])
        else:
            print(n)
        i += 3 + l

def op_start_send_and_receive(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tInc<%x>Trans<%x>' % (up.unpack_uint(), up.unpack_uint()))
    message_number = up.unpack_int()
    number_of_messages = up.unpack_int()
    print('\t<%d,%d>' % (message_number, number_of_messages))
    i = up.get_position()
    i += ((4-i) & 3)
    print('\t', binascii.b2a_hex(msg[i:]))
    return msg

def op_response(sock):
    head = sock.recv(16)
    print('\thandle<', binascii.b2a_hex(head[0:4]), '>id<', binascii.b2a_hex(head[4:12]), '>', end='')
    nbytes = _bytes_to_bint32(head, 12)
    bs = _need_nbytes_align(sock, nbytes)
    print('Data len=%d' % (nbytes))
    hex_dump(bs)
    if get_last_op_name() == 'op_info_database':
        i = 0
        while i < len(bs):
            s = isc_info_names[_ord(bs[i])]
            print('\t' + s, end=' ')
            if s == 'isc_info_end':
                print()
                break
            if s in ['isc_info_db_sql_dialect', 'isc_info_firebird_version',
                'isc_info_isc_version',
                'isc_info_implementation', 'isc_info_db_class',
                'isc_info_base_level', 'isc_info_ods_version', 
                'isc_info_ods_minor_version', 'isc_info_db_id',
                'isc_info_expunge_count', 'isc_info_page_size',
                'isc_info_sweep_interval', 'isc_info_user_names',
                'frb_info_att_charset',
                ]:
                l = _bytes_to_int(bs, i+1, 2)
                print('[', binascii.b2a_hex(bs[i+3:i+3+l]), ']', end=' ')
                if s == 'isc_info_firebird_version':
                    print('', bs[i+5:i+3+l], end=' ')
                i = i + 3 + l
            else:
                i = i + 1
            print()
    elif get_last_op_name() == 'op_prepare_statement':
        parse_sql_info(bs, get_prepare_statement())
    elif get_last_op_name() == 'op_info_sql':
        parse_sql_info(bs, get_prepare_statement())
    elif get_last_op_name() == 'op_connect_request':
        server_port = _bytes_to_bint(bs, 2, 2)
        server_ip = '.'.join([str(_ord(bs[i])) for i in (4, 5, 6, 7)])
        print('\tport:', server_port)
        print('\tip address:', server_ip)
        # override new ip address in packet
        if True:
            port = server_port + 1
            bs_new_ip = _bint_to_bytes(port, 2)
            bs = bs[:2] + bs_new_ip + bs[4:]
            print('\tnew->')
            hex_dump(bs)
            thread.start_new_thread(recv_forever, (server_ip, server_port, port))

    # http://www.ibphoenix.com/main.nfs?a=ibphoenix&page=ibp_60_upd_sv_fs
    sv = sock.recv(bufsize)
    i = 0
    print('\tStatus vector[', binascii.b2a_hex(sv), ']')
    asc_dump(sv)
    print('\t', end=' ')
    while i < len(sv):
        s = isc_status_names[_bytes_to_bint32(sv, i)]
        i += 4
        print(s, end=' ')
        if s == 'isc_arg_gds':
            err_code = _bytes_to_bint32(sv,i)
            print( isc_gds_error_code.get(err_code, err_code), end=' ')
            i += 4 
        elif s == 'isc_arg_number':
            print(_bytes_to_bint32(sv, i), end=' ')
            i += 4 
        elif s in ['isc_arg_string', 'isc_arg_interpreted', 'isc_arg_sql_state']:
            nbytes = _bytes_to_bint32(sv, i)
            i += 4
            print('<', sv[i:i + nbytes], '>', end=' ')
            i += nbytes
            if nbytes % 4:
                i += 4 - nbytes % 4  # 4 bs word alignment
        if s == 'isc_arg_end':
            i += 1
            break
    print()
    if i < len(sv):
        i += ((4-i) & 3)
        print('\tpiggyback[', binascii.b2a_hex(sv[i:]), ']', len(sv) - i)
        asc_dump(sv[i:])
    return head + bs + sv

op_response_piggyback = op_response

def op_sql_response(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tcount=%d' % (up.unpack_int()))
    print('\t', binascii.b2a_hex(msg[up.get_position():]))
    return msg

def op_fetch(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    statement = up.unpack_uint()
    set_prepare_statement(statement)
    print('\tStatement<%x>' % (statement))
    blr = up.unpack_bytes()
    assert blr == _calc_blr(get_xsqlda_statement()[statement])
    print('\tBLR[', binascii.b2a_hex(blr), ']')
    print('\tMessage No.<%d> size<%d>' % (up.unpack_int(), up.unpack_int()))
    up.done()
    return msg

def op_fetch_response(sock):
    msg = sock.recv(8)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    status = up.unpack_int()
    count = up.unpack_int()
    print('\tStatus<%d> count=<%d>' % (status, count))
    up.done()
    print('\tstatement=<%d>' % (get_prepare_statement()))
    xsqlda = get_xsqlda_statement()[get_prepare_statement()]
    for x in xsqlda:
        print('\t', x)
    if status == 100:
        print('\tNo more data')
    elif status == 0: # Has rows
        while True:
            for x in xsqlda:
                if x.io_length() < 0:
                    bytes = sock.recv(4)
                    l = _bytes_to_bint32(bytes, 0)
                    msg += bytes
                else:
                    l = x.io_length()
                x.raw_value = sock.recv(l)
                msg += x.raw_value
                msg += sock.recv((4-l) & 3)    # padding
                bytes = sock.recv(4)
                x.null_flag = False if bytes == '\0\0\0\0' else True
                msg += bytes
                print('\t', x)
            bytes = sock.recv(12)
            msg += bytes
            print('\t{', _bytes_to_bint32(bytes, 0), end='')
            print(_bytes_to_bint32(bytes, 4), end=',')
            print(_bytes_to_bint32(bytes, 8), '}')
            if _bytes_to_bint32(bytes, 8) == 0:
                break
    return msg

def op_info_database(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tDatabase<%x>' % (up.unpack_uint()))
    assert up.unpack_int() == 0 # Incarnation of object
    bs = up.unpack_bytes() # AbstractJavaGDSImpl.java/describe_database_info
    print('\t[', binascii.b2a_hex(bs), ']=[', end='')
    for b in bs:
        print('', isc_info_names[_ord(b)], end='')
    print(']')
    print('\tbuffer len=%d' % up.unpack_int())
    up.done()
    return msg

def op_connect(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    assert op_names[up.unpack_int()] == 'op_attach'
    print('\tconnect_version', up.unpack_int())
    print('\tArchitecture type', up.unpack_int())  # Architecture type (Generic = 1)
    print('\tPath<%s>' % (up.unpack_string()))
    pcount = up.unpack_int()
    print('\tProtocol version understood count=', pcount )
    uid = up.unpack_bytes()
    print('\tuid=[', binascii.b2a_hex(uid), ']')
    i = 0
    while i < len(uid):
        name = CNCT_names[ord(uid[i])]
        n = ord(uid[i+1])
        v = uid[i+2:i+2+n]
        if name in ('CNCT_specific_data', 'CNCT_client_crypt'):
            v = binascii.b2a_hex(v)
        print('\t\t', name, n, v)
        i += n + 2

    print('\tProtocol version', up.unpack_int())
    print('\tArchitecture type', up.unpack_int())
    print('\tMinimum type',  up.unpack_int())   # Minimum type (2)
    print('\tMaxiumum type',  up.unpack_int())  # Maximum type (3 to 5)
    print('\tPreference weight', up.unpack_int())
    while pcount > 1:
        print('\tmore protocol=', up.unpack_int(), up.unpack_int(), end='')
        print(up.unpack_int(), up.unpack_int(), up.unpack_int())
        pcount -= 1
    up.done()
    return msg

def op_accept(sock):
    msg = sock.recv(12)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tProtocol<%d>Archtecture<%d>MinimumType<%d>' % (
            up.unpack_int(), up.unpack_int(), up.unpack_int()))
    up.done()
    return msg

def op_accept_data(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tProtocol<%d>Archtecture<%d>MinimumType<%d>' % (
            up.unpack_int(), up.unpack_int(), up.unpack_int()))
    bs = up.unpack_bytes()
    print('\tdata=[', binascii.b2a_hex(bs), ']')
    bs = up.unpack_bytes()
    print('\tplugin=[', bs, ']')
    print('\tAuthenticated<%d>' % (up.unpack_int(), ))
    bs = up.unpack_bytes()
    print('\tkeys=[', bs, ']')
    up.done()
    return msg

op_cond_accept = op_accept_data

def op_cont_auth(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    bs = up.unpack_bytes()
    print('\tdata=[', binascii.b2a_hex(bs), ']')
    bs = up.unpack_bytes()
    print('\tname=[', bs, ']')
    bs = up.unpack_bytes()
    print('\tlist=[', bs, ']')
    bs = up.unpack_bytes()
    print('\tkeys=[', binascii.b2a_hex(bs), ']')

    up.done()
    return msg

def op_cancel(sock):
    msg = sock.recv(4)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tkind=', up.unpack_uint())
    up.done()
    return msg

def op_crypt(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tplugin[%s]' % (up.unpack_string(), ))
    print('\tkey[%s]' % (binascii.b2a_hex((up.unpack_bytes())),))
    up.done()
    return msg

def op_attach(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    assert up.unpack_uint() == 0    # Database Object ID (0)
    print('\tPath<%s>' % (up.unpack_string()))
    bytes = up.unpack_bytes()
    _database_parameter_block(bytes)
    up.done()
    return msg

def op_detach(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tDatabase<%x>' % (up.unpack_uint()))
    up.done()
    return msg

def op_transaction(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tDatabase<%x>' % (up.unpack_uint()), end='')
    bs = up.unpack_bytes()
    print('\t[', binascii.b2a_hex(bs), ']=[', end='')
    for b in bs:
        print('', isc_tpb_names[_ord(b)], end='')
    print(']')
    up.done()
    return msg

def op_commit(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tTrans<%x>' % (up.unpack_uint()))
    up.done()
    return msg

op_rollback = op_commit

def op_prepare_statement(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    set_prepare_trans(up.unpack_uint())
    statement = up.unpack_int()
    set_prepare_statement(statement)
    get_xsqlda_statement()[statement] = None
    set_prepare_dialect(up.unpack_int())
    print('\tTrans<%x>Statement<%x>dialect<%d>' % (get_prepare_trans(),
            get_prepare_statement(), get_prepare_dialect()))
    print('\t', up.unpack_string())
    bs = up.unpack_bytes() # AbstractJavaGDSImpl.java/sql_prepare_info
    print('\t[', binascii.b2a_hex(bs), ']=[', end='')
    for b in bs:
        print(isc_info_sql_names[_ord(b)], end=',')
    print(']')
    print('\tbuffer_len=', up.unpack_int())
    up.done()
    return msg

def op_info_sql(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tStatement<%x>' % (up.unpack_uint()))
    assert up.unpack_int() == 0
    bs = up.unpack_bytes()
    print('\t[' + binascii.b2a_hex(bs) + ']=[', end='')
    i = 0
    while i < len(bs):
        s = isc_info_sql_names[_ord(bs[i])]
        print(s, end='')
        if s == 'isc_info_end':
            break
        if s in ['isc_info_truncated']:
            print(_bytes_to_int(bs, i+1, 2), end='')
            i = i + 3
        else:
            i = i + 1
    print(']')
    print('\tbuffer_len=', up.unpack_int())
    up.done()
    return msg

def op_allocate_statement(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    print('\tDatabase<%x>' % (_bytes_to_bint32(msg, 0),), end='')
    print('[', binascii.b2a_hex(msg[24:]), ']')
    asc_dump(msg[24:])
    return msg

def op_free_statement(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tStatement<%x>' % (up.unpack_uint()), end='')
    f = up.unpack_int()
    if f == 1:
        print('DSQL_close')
    elif f == 2:
        print('DSQL_drop')
    else:
        print('Unknown!')
    # unknown data remains
#    up.done()
    return msg

def op_execute(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tStatement<%x>Trans<%x>' % (up.unpack_uint(), up.unpack_uint()))
    blr = up.unpack_bytes()
    print('\tparam BLR[', binascii.b2a_hex(blr), ']')
    message_number = up.unpack_int()
    number_of_messages = up.unpack_int()
    print('\t<%d,%d>' % (message_number, number_of_messages))
    if number_of_messages:
        print('\tparam value[', binascii.b2a_hex(msg[up.get_position():]), ']')
        _parse_param(blr, msg[up.get_position():])
    return msg

def op_execute2(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tStatement<%x>Trans<%x>' % (up.unpack_uint(), up.unpack_uint()))
    blr = up.unpack_bytes()
    print('\tinput params BLR[', binascii.b2a_hex(blr), ']')
    message_number = up.unpack_int()
    messages = up.unpack_bytes()
    print('\t<%d,%s>' % (message_number, messages))

    out_blr = up.unpack_bytes()
    print('\toutput params BLR[', binascii.b2a_hex(out_blr), ']')

    output_message_number = up.unpack_int()
    print('\toutput_message_number<%d>' % (message_number))

    return msg

def op_execute_immediate(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    i = 0
    trans_handle = _bytes_to_bint(msg, i, 4)
    i += 4
    db_handle = _bytes_to_bint(msg, i, 4)
    i += 4
    sql_len = _bytes_to_bint(msg, i, 2)
    i += 2
    sql = msg[i:i+sql_len]
    i += sql_len
    dialect = _bytes_to_bint(msg, i, 2)
    i += 2
    in_blr_len = _bytes_to_bint(msg, i, 2)
    i += 2
    in_blr = msg[i:i+in_blr_len]
    i += in_blr_len
    in_msg_len = _bytes_to_bint(msg, i, 2)
    i += 2
    in_msg = msg[i:i+in_msg_len]
    i += in_msg_len
    out_msg_len = _bytes_to_bint(msg, i, 2)
    i += 2
    out_msg = msg[i:i+out_msg_len]
    i += out_msg_len
    possible_requests = _bytes_to_bint(msg, i, 4)

    print('\tdb_handle=', db_handle)
    print('\ttrans_handle=', trans_handle)
    print('\tsql=[', sql, ']')
    print('\tdiarect=', dialect)
    print('\tin_msg=[%s]' % (in_msg, ))
    print('\tout_msg=[%s]' % (out_msg, ))
    print('\tpossible_requests=', possible_requests)

    return msg

op_execute_immediate2 = op_execute_immediate

def op_open_blob(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tTrans<%x>BlobID<%04x%04x>' % (
                    up.unpack_uint(), up.unpack_uint(), up.unpack_uint()))
    up.done()
    return msg

def op_open_blob2(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    buf = up.unpack_bytes()
    print('\tbuf[' + binascii.b2a_hex(buf) + ']')
    print('\tTrans<%x>BlobID<%04x%04x>' % (
                    up.unpack_uint(), up.unpack_uint(), up.unpack_uint()))
    up.done()
    return msg

def op_batch_segments(sock):
    msg = sock.recv(12)
    msg_dump(msg)
    blob_id = _bytes_to_bint32(msg, 0)
    segment_size = _bytes_to_bint32(msg, 4)
    segment_size2 = _bytes_to_bint32(msg, 8)
    assert segment_size == segment_size2
    print('\tsegment_blob=', segment_size)
    buf = b''
    while segment_size:
        b = sock.recv(segment_size)
        buf += b
        segment_size -= len(b)
    print('\tlen=', len(buf))
    return msg + buf

op_put_segment = op_batch_segments

def op_close_blob(sock):
    msg = sock.recv(4)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tBlobHandle<%x>' % (up.unpack_uint()))
    up.done()
    return msg

def op_get_segment(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tBlobHandle<%x>len<%d>' % (up.unpack_uint(), up.unpack_int()))
    assert up.unpack_int() == 0     # Data segment (0)
    up.done()
    return msg

def op_service_attach(sock):
    msg = sock.recv(bufsize)
    up = xdrlib.Unpacker(msg)
    assert up.unpack_int() == 0 # object id
    print('\tservice=[' + up.unpack_string() +']')
    param = up.unpack_bytes()
    print('\tparam=[' +  binascii.b2a_hex(param) + ']')
    _database_parameter_block(param)
    up.done()
    return msg

def op_service_info(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tobject id<%08x>' % (up.unpack_int(),), end='')
    assert up.unpack_int() == 0 # object
    print('param=[' + binascii.b2a_hex(up.unpack_bytes()) +']', end='')
    print('information items=[' + binascii.b2a_hex(up.unpack_bytes()) +']', end='')
    print('buflen=', up.unpack_int())
    up.done()
    return msg

def op_service_detach(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\thandle=', up.unpack_int())
    up.done()
    return msg

def op_service_start(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\thandle=', up.unpack_int())
    assert up.unpack_int() == 0 # object
    param = up.unpack_bytes()
    print('\tparam=[' +  binascii.b2a_hex(param) + ']')
    _service_parameter_block(param)
    up.done()
    return msg

def op_release(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\thandle=', up.unpack_int())
    up.done()
    return msg

def op_compile(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    assert up.unpack_int() == 0 # Object ID
    hex_dump(up.unpack_bytes())
    up.done()
    return msg

def op_create(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    assert up.unpack_int() == 0 # Object ID
    print('\tPath<%s>' % (up.unpack_string()))
    param = up.unpack_bytes()
    _database_parameter_block(param)
    up.done()
    return msg

def op_que_events(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tdb_handle=', up.unpack_int())
    prs = up.unpack_string()    # param raw strings
    print('\tprs=[', binascii.b2a_hex(prs), ']')
    param_strings = []
    assert _ord(prs[0]) == 1
    i = 1
    while i < len(prs):
        ln = _ord(prs[i])
        s = prs[i+1:i+1+ln]
        param_strings.append(s)
        i += 5+ln

    print('\tparam=', param_strings)
    print('\tast=', up.unpack_int())
    print('\tevent_args=', up.unpack_int())
    print('\tevent_rid=', up.unpack_int())
    return msg

def op_cancel_events(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\tdb_handle=', up.unpack_int())
    print('\tevent_id<%x>' % (up.unpack_uint()))
    up.done()
    return msg

def op_connect_request(sock):
    msg = sock.recv(bufsize)
    msg_dump(msg)
    up = xdrlib.Unpacker(msg)
    print('\ttype=',up.unpack_int())
    print('\tdb_handle=', up.unpack_int())
    assert up.unpack_int() == 0
    up.done()
    return msg

def op_dummy(sock):
    return None

#-----------------------------------------------------------------------------
# recive and dump bytes
def recv_forever(server_name, server_port, listen_port):
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cs.bind(('', listen_port))
    cs.listen(1)
    client_socket, addr = cs.accept()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_name, server_port))

    while True:
        r = server_socket.recv(1)
        if r:
            print('%02x' % (_ord(r),), end='')
            client_socket.send(r)
        else:
            print('recv thread exit')
            thread.exit()

#-----------------------------------------------------------------------------
# proxy tcp socket side by side
def proxy_socket(client_socket, server_name, server_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_name, server_port))
    server_socket.setblocking(0)
    client_socket.setblocking(0)
    while True:
        try:
            s = client_socket.recv(1)
            if s:
                server_socket.send(s)
                print('>%02x' % (_ord(s),))
        except socket.error:
            pass
        try:
            r = server_socket.recv(1)
            if r:
                print('<%02x' % (_ord(r),))
                client_socket.send(r)
        except socket.error:
            pass

def proxy_socket_forever(server_name, server_port, listen_port):
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cs.bind(('', listen_port))
    cs.listen(5)
    while True:
        sock, addr = cs.accept()
        thread.start_new_thread(proxy_socket, (sock, server_name, server_port))

#-----------------------------------------------------------------------------
# proxy wire protocol
def process_wire(client_socket, server_name, server_port):
    # Socket to Firebird server.
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.connect((server_name, server_port))
    
    while True:
        client_head = client_socket.recv(4)
        unpacker = xdrlib.Unpacker(client_head)
        op_req_code = unpacker.unpack_int()
        op_req_name = op_names[op_req_code]
        print(thread.get_ident(), '<--', op_req_code, op_req_name)
        if op_req_name in globals():
            client_msg = globals()[op_req_name](client_socket)
        else:
            client_msg = client_socket.recv(bufsize)
            print('\t[[', binascii.b2a_hex(client_msg), ']]')
        server_socket.send(client_head)
        if client_msg:
            server_socket.send(client_msg)
        if op_req_name == 'op_cancel':
            continue
        set_last_op_name(op_req_name)
        op_res_name = ''
        while op_res_name == '' or op_res_name == 'op_dummy':
            server_head = server_socket.recv(4)
            unpacker = xdrlib.Unpacker(server_head)
            op_res_code = unpacker.unpack_int()
            op_res_name = op_names[op_res_code]
            print(thread.get_ident(), '-->', op_res_code, op_res_name)
            if op_res_name in globals():
                server_msg = globals()[op_res_name](server_socket)
            else:
                server_msg = server_socket.recv(bufsize)
                print('\t', binascii.b2a_hex(server_msg))
            client_socket.send(server_head)
            if server_msg:
                client_socket.send(server_msg)
        if get_last_op_name() in (
            'op_service_detach', 'op_detach'):
            break

def proxy_wire_forever(server_name, server_port, listen_host, listen_port):
    # Socket from client.
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cs.bind((listen_host, listen_port))
    cs.listen(5)
    while True:
        sock, addr = cs.accept()
        thread.start_new_thread(process_wire, (sock, server_name, server_port))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage : ' + sys.argv[0] + ' fb_server[:fb_port] [listen_host:]listen_port')
        sys.exit()
    
    server = sys.argv[1].split(':')
    server_name = server[0]
    if len(server) == 1:
        server_port = 3050
    else:
        server_port = int(server[1])

    listen = sys.argv[2].split(':')
    if len(listen) == 1:
        listen_host = 'localhost'
        listen_port = int(listen[0])
    else:
        listen_host = listen[0]
        listen_port = int(listen[1])

    proxy_wire_forever(server_name, server_port, listen_host, listen_port)

