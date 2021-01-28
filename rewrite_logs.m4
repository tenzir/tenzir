m4_define(m4_dquote, DQUOTE)
m4_define(m4_shift2, `m4_shift(m4_shift($@))')
m4_define(m4_shift3, `m4_shift(m4_shift(m4_shift($@)))')
m4_define(m4_cat, `$1$2')
m4_define(m4_strcat, `m4_cat(m4_substr($1,0,m4_decr(m4_len($1))),m4_substr($2,1))')
m4_dnl -- _m4_reorder_args($1 = format_string, $2 = format_args, $3 = current, tail...)
m4_define(_m4_reorder_args, `m4_ifelse(
    $3,,
        $1 __COMMA__ $2,
    m4_substr($3, 0, 1),",
        `_m4_reorder_args(m4_strcat($1,$3),$2, m4_shift3($@))',
        `_m4_reorder_args(m4_strcat($1," {} "),$2 __COMMA__ $3, m4_shift3($@))')')

m4_define(VAST_TRACE, VAST_LOG_SPD_TRACE( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_TRACE_ANON, VAST_LOG_SPD_TRACE( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )

m4_define(VAST_DEBUG, VAST_LOG_SPD_DEBUG( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_DEBUG_ANON, VAST_LOG_SPD_DEBUG( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )

m4_define(VAST_VERBOSE, VAST_LOG_SPD_VERBOSE( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_VERBOSE_ANON, VAST_LOG_SPD_VERBOSE( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )

m4_define(VAST_INFO, VAST_LOG_SPD_INFO( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_INFO_ANON, VAST_LOG_SPD_INFO( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )

m4_define(VAST_WARNING, VAST_LOG_SPD_WARN( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_WARNING_ANON, VAST_LOG_SPD_WARN( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )

m4_define(VAST_ERROR, VAST_LOG_SPD_ERROR( `_m4_reorder_args("{} ", detail::id_or_name(`$1'), m4_shift($@))' ) )
m4_define(VAST_ERROR_ANON, VAST_LOG_SPD_ERROR( `_m4_reorder_args("",,`$1',m4_shift($@))' ) )
