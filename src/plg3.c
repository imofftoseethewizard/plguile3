#include <libguile.h>

#include <postgres.h>

// Postgres include files, in alphabetical order
#include <access/genam.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/relation.h>
#include <catalog/namespace.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_enum.h>
#include "catalog/pg_language.h"
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_range.h>
#include <catalog/pg_type.h>
#include <commands/event_trigger.h>
#include <common/hashfn.h>
#include <executor/spi.h>
#include <executor/tuptable.h>
#include <fmgr.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <parser/parse_func.h>
#include <tcop/dest.h>
#include <tsearch/ts_type.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/elog.h>
#include <utils/fmgroids.h>
#include <utils/geo_decls.h>
#include <utils/hsearch.h>
#include <utils/inet.h>
#include <utils/jsonfuncs.h>
#include <utils/jsonpath.h>
#include <utils/lsyscache.h>
#include <utils/multirangetypes.h>
#include <utils/pg_crc.h>
#include <utils/rangetypes.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <utils/tuplestore.h>
#include <utils/typcache.h>
#include <utils/uuid.h>
#include <utils/varbit.h>
#include <utils/xml.h>

#define NS_PER_USEC 1000
#define SECS_PER_MONTH (DAYS_PER_MONTH * SECS_PER_DAY)

// Most datum-scm conversion functions accept but do not use the oid parameter. This is a
// convenience for calling one of these where the appropriate type oid is not available.
#define OID_NOT_USED ((Oid)0)

// Defines const char *src_plg3_scm.
#include "plg3.scm.h"

PG_MODULE_MAGIC;

PGDLLEXPORT Datum plg3_call(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum plg3_call_inline(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum plg3_compile(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(plg3_call);
PG_FUNCTION_INFO_V1(plg3_call_inline);
PG_FUNCTION_INFO_V1(plg3_compile);

void _PG_init(void);
void _PG_fini(void);

static Oid owner_oid = InvalidOid;

typedef struct {
	Oid subtype_oid;
	Oid range_type_oid;
	Oid multirange_type_oid;
} RangeCacheEntry;

typedef SCM (*ToScmFunc)(Datum, Oid);
typedef Datum (*ToDatumFunc)(SCM, Oid);

typedef struct TypeConvCacheEntry {
	Oid type_oid;    // OID of a PostgreSQL type
	ToScmFunc to_scm;   // Function pointer to convert Datum to SCM
	ToDatumFunc to_datum; // Function pointer to convert SCM to Datum
} TypeConvCacheEntry;

typedef struct Receiver {
	DestReceiver dest;
	SCM proc;
	SCM result;
	SCM tail;
} Receiver;

typedef struct call_limits {
	float4 time_seconds;
	uint64 allocation_bytes;
} CallLimits;

static SCM make_boxed_datum(Oid type_oid, Datum x);

static SCM datum_array_to_scm(Datum x, Oid type_oid);
static SCM datum_bit_string_to_scm(Datum x, Oid type_oid);
static SCM datum_bool_to_scm(Datum x, Oid type_oid);
static SCM datum_box_to_scm(Datum x, Oid type_oid);
static SCM datum_bytea_to_scm(Datum x, Oid type_oid);
static SCM datum_circle_to_scm(Datum x, Oid type_oid);
static SCM datum_composite_to_scm(Datum x, Oid type_oid);
static SCM datum_date_to_scm(Datum x, Oid type_oid);
static SCM datum_enum_to_scm(Datum x, Oid type_oid);
static SCM datum_float4_to_scm(Datum x, Oid type_oid);
static SCM datum_float8_to_scm(Datum x, Oid type_oid);
static SCM datum_inet_to_scm(Datum x, Oid type_oid);
static SCM datum_int2_to_scm(Datum x, Oid type_oid);
static SCM datum_int4_to_scm(Datum x, Oid type_oid);
static SCM datum_int8_to_scm(Datum x, Oid type_oid);
static SCM datum_interval_to_scm(Datum x, Oid type_oid);
static SCM datum_json_to_scm(Datum x, Oid type_oid);
static SCM datum_jsonb_to_scm(Datum x, Oid type_oid);
static SCM datum_jsonpath_to_scm(Datum x, Oid type_oid);
static SCM datum_line_to_scm(Datum x, Oid type_oid);
static SCM datum_lseg_to_scm(Datum x, Oid type_oid);
static SCM datum_macaddr8_to_scm(Datum x, Oid type_oid);
static SCM datum_macaddr_to_scm(Datum x, Oid type_oid);
static SCM datum_multirange_to_scm(Datum x, Oid type_oid);
static SCM datum_numeric_to_scm(Datum x, Oid type_oid);
static SCM datum_path_to_scm(Datum x, Oid type_oid);
static SCM datum_point_to_scm(Datum x, Oid type_oid);
static SCM datum_polygon_to_scm(Datum x, Oid type_oid);
static SCM datum_range_to_scm(Datum x, Oid type_oid);
static SCM datum_text_to_scm(Datum x, Oid type_oid);
static SCM datum_time_to_scm(Datum x, Oid type_oid);
static SCM datum_timestamptz_to_scm(Datum x, Oid type_oid);
static SCM datum_timetz_to_scm(Datum x, Oid type_oid);
static SCM datum_tsquery_to_scm(Datum x, Oid type_oid);
static SCM datum_tsvector_to_scm(Datum x, Oid type_oid);
static SCM datum_uuid_to_scm(Datum x, Oid type_oid);
static SCM datum_void_to_scm(Datum x, Oid type_oid);
static SCM datum_xml_to_scm(Datum x, Oid type_oid);

static Datum scm_to_datum_array(SCM x, Oid type_oid);
static Datum scm_to_datum_bit_string(SCM x, Oid type_oid);
static Datum scm_to_datum_bool(SCM x, Oid type_oid);
static Datum scm_to_datum_box(SCM x, Oid type_oid);
static Datum scm_to_datum_bytea(SCM x, Oid type_oid);
static Datum scm_to_datum_circle(SCM x, Oid type_oid);
static Datum scm_to_datum_date(SCM x, Oid type_oid);
static Datum scm_to_datum_enum(SCM x, Oid type_oid);
static Datum scm_to_datum_float4(SCM x, Oid type_oid);
static Datum scm_to_datum_float8(SCM x, Oid type_oid);
static Datum scm_to_datum_inet(SCM x, Oid type_oid);
static Datum scm_to_datum_int2(SCM x, Oid type_oid);
static Datum scm_to_datum_int4(SCM x, Oid type_oid);
static Datum scm_to_datum_int8(SCM x, Oid type_oid);
static Datum scm_to_datum_interval(SCM x, Oid type_oid);
static Datum scm_to_datum_json(SCM x, Oid type_oid);
static Datum scm_to_datum_jsonb(SCM x, Oid type_oid);
static Datum scm_to_datum_jsonpath(SCM x, Oid type_oid);
static Datum scm_to_datum_line(SCM x, Oid type_oid);
static Datum scm_to_datum_lseg(SCM x, Oid type_oid);
static Datum scm_to_datum_macaddr(SCM x, Oid type_oid);
static Datum scm_to_datum_macaddr8(SCM x, Oid type_oid);
static Datum scm_to_datum_multirange(SCM x, Oid type_oid);
static Datum scm_to_datum_numeric(SCM x, Oid type_oid);
static Datum scm_to_datum_path(SCM x, Oid type_oid);
static Datum scm_to_datum_point(SCM x, Oid type_oid);
static Datum scm_to_datum_polygon(SCM x, Oid type_oid);
static Datum scm_to_datum_range(SCM x, Oid type_oid);
static Datum scm_to_datum_record(SCM x, Oid type_oid);
static Datum scm_to_datum_text(SCM x, Oid type_oid);
static Datum scm_to_datum_time(SCM x, Oid type_oid);
static Datum scm_to_datum_timestamptz(SCM x, Oid type_oid);
static Datum scm_to_datum_timetz(SCM x, Oid type_oid);
static Datum scm_to_datum_tsquery(SCM x, Oid type_oid);
static Datum scm_to_datum_tsvector(SCM x, Oid type_oid);
static Datum scm_to_datum_uuid(SCM x, Oid type_oid);
static Datum scm_to_datum_void(SCM x, Oid type_oid);
static Datum scm_to_datum_xml(SCM x, Oid type_oid);

static Datum datum_date_to_timestamptz(Datum x);
static char *scm_to_string(SCM x);
static SCM scm_c_list_ref(SCM obj, size_t k);

static Oid get_boxed_datum_type(SCM x);
static Datum get_boxed_datum_value(SCM x);
static bool is_bit_string(SCM x);
static bool is_box(SCM x);
static bool is_boxed_datum(SCM x);
static bool is_circle(SCM x);
static bool is_cursor(SCM x);
static bool is_date(SCM x);
static bool is_decimal(SCM x);
static bool is_inet(SCM x);
static bool is_int2(SCM x);
static bool is_int4(SCM x);
static bool is_int8(SCM x);
static bool is_jsonb(SCM x);
static bool is_jsonpath(SCM x);
static bool is_line(SCM x);
static bool is_lseg(SCM x);
static bool is_macaddr(SCM x);
static bool is_macaddr8(SCM x);
static bool is_multirange(SCM x);
static bool is_path(SCM x);
static bool is_point(SCM x);
static bool is_polygon(SCM x);
static bool is_range(SCM x);
static bool is_time(SCM x);
static bool is_tsquery(SCM x);
static bool is_tsvector(SCM x);

static Oid get_guile3_owner_oid(void);
static SCM load_base_module(void);
static SCM base_module_loader(void *data);
static SCM load_base_module_error_handler(void *data, SCM key, SCM args);
static void role_remove_func_oid(SCM old_owner, SCM func);
static SCM base_module_evaluator(void *data);
static SCM eval_string_in_base_module(const char *text);
static SCM base_module_evaluator_error_handler(void *data, SCM key, SCM args);
static void define_primitive(const char *name, int req, int opt, int rst, scm_t_subr fcn);
static SCM untrusted_eval(SCM expr, SCM env);
static SCM find_or_create_module_for_role(Oid role_oid);
static SCM call_1_executor(void *data);
static SCM call_1(SCM func, SCM arg);
static SCM call_2_executor(void *data);
static SCM call_2(SCM func, SCM arg1, SCM arg2);
static SCM call_3_executor(void *data);
static SCM call_3(SCM func, SCM arg1, SCM arg2, SCM arg3);
static SCM call_4_executor(void *data);
static SCM call_4(SCM func, SCM arg1, SCM arg2, SCM arg3, SCM arg4);
static SCM call_8_executor(void *data);
static SCM call_8(SCM func, SCM arg1, SCM arg2, SCM arg3, SCM arg4, SCM arg5, SCM arg6, SCM arg7, SCM arg8);
static SCM call_error_handler(void *data, SCM key, SCM args);
static void insert_range_cache_entry(Oid subtype_oid, Oid range_type_oid, Oid multirange_type_oid);
static void insert_type_cache_entry(Oid type_oid, ToScmFunc to_scm, ToDatumFunc to_datum);
static SCM datum_to_scm(Datum datum, Oid type_oid);
static Datum scm_to_datum(SCM scm, Oid type_oid);
static Datum scm_to_setof_datum(SCM x, Oid type_oid, MemoryContext ctx, ReturnSetInfo *rsinfo);
static Datum call_event_trigger(FunctionCallInfo fcinfo);
static Datum call_trigger(FunctionCallInfo fcinfo);
static Datum call_ordinary(FunctionCallInfo fcinfo);
static SCM prepare_event_trigger_arguments(EventTriggerData *event_trigger_data);
static SCM prepare_ordinary_arguments(FunctionCallInfo fcinfo);
static SCM find_or_compile_proc(Oid func_oid);
static SCM prepare_trigger_arguments(TriggerData *trigger_data);
static SCM compile_proc(HeapTuple proc_tuple, SCM module);
static Datum convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo);
static Datum convert_boxed_datum_to_datum(SCM scm, Oid target_type_oid);
static Datum scm_to_composite_datum(SCM result, TupleDesc tuple_desc);
static Datum scm_to_setof_composite_datum(SCM result, TupleDesc tuple_desc, MemoryContext ctx, ReturnSetInfo *rsinfo);
static Datum scm_to_setof_record_datum(SCM result, MemoryContext ctx, ReturnSetInfo *rsinfo);
static SCM heap_tuple_to_scm(HeapTuple heap_tuple, TupleDesc tuple_desc);
static HeapTuple scm_record_to_heap_tuple(SCM x, TupleDesc tuple_desc);
static SCM datum_heap_tuple_to_scm(Datum x, TupleDesc tuple_desc);
static Oid infer_scm_type_oid(SCM x);
static Oid infer_array_type_oid(SCM x);
static Oid infer_range_type_oid(SCM x);
static Oid infer_multirange_type_oid(SCM x);
static Oid unify_type_oid(Oid t1, Oid t2);
static Oid unify_range_subtype_oid(Oid t1, Oid t2);
static Oid find_enum_datum_type(SCM type_name);
static bool find_range_type_oid(Oid subtype_oid, Oid *range_type_oid, Oid *multirange_type_oid);
static TupleDesc record_tuple_desc(SCM x);
static TupleDesc table_tuple_desc(SCM x);
static TupleDesc build_tuple_desc(SCM attr_names, SCM type_desc_list);
static bool is_record(SCM x);
static bool is_table(SCM x);
static bool is_set_returning(HeapTuple proc_tuple);
static bool is_event_trigger_handler(HeapTuple proc_tuple);
static bool is_trigger_handler(HeapTuple proc_tuple);
static SCM type_desc_expr(Oid type_oid);
static SCM jsonb_to_scm_expr(Jsonb *jsb);
static SCM jsb_scalar_to_scm(JsonbValue *jsb);
static uint16 get_tsposition_index(SCM position);
static uint16 get_tsposition_weight(SCM position);
static size_t calculate_tsvector_buffer_size(SCM x);
static SCM tsquery_items_to_scm(QueryItem **qi_iter, char *operands);
static void tsquery_expr_size(SCM expr, int *count, size_t *bytes);
static void scm_to_tsquery_items(SCM expr, QueryItem **qi_iter, char *operands, size_t *offset);
static Jsonb *jsonb_root_expr_to_jsonb(SCM expr);
static void jsonb_expr_to_jsbv(SCM expr, JsonbValue *jsbv);
static void jsonb_array_expr_to_jsbv(SCM expr, JsonbValue *jsbv);
static void jsonb_object_expr_to_jsbv(SCM expr, JsonbValue *jsbv);
static void jsonb_raw_scalar_expr_to_jsbv(SCM expr, JsonbValue *jsbv);
static void jsonb_scalar_expr_to_jsbv(SCM expr, JsonbValue *jsbv);
static SCM jsp_to_scm(JsonPathItem *v, SCM expr);
static SCM jsp_item_to_scm(JsonPathItem *v, SCM expr);
static SCM jsp_operation_symbol(JsonPathItemType type);
static Numeric jsp_get_numeric(JsonPathItem *v);
static char *jsp_get_string(JsonPathItem *v);
static SCM jsp_bounds_to_scm(uint32 value);
static SCM jsp_regex_flags_to_scm(uint32 flags);
static void jsp_get_arg(JsonPathItem *v, JsonPathItem *a);
static void jsp_get_left_arg(JsonPathItem *v, JsonPathItem *a);
static void jsp_get_right_arg(JsonPathItem *v, JsonPathItem *a);
static bool jsp_get_array_subscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem *to, int i);
static void jsp_init(JsonPathItem *v, JsonPath *js);
static bool jsp_next(JsonPathItem *v);
static void jsp_init_by_buffer(JsonPathItem *v, char *base, int32 pos);
static int scm_expr_to_jsp(StringInfo buf, SCM expr);
static JsonPathItemType jsp_expr_type(SCM expr);
static void scm_chained_expr_to_jsp(StringInfo buf, SCM expr);
static void align_buffer(StringInfo buf);
static int32 reserve_jsp_offset(StringInfo buf);
static int32 reserve_jsp_offsets(StringInfo buf, uint32 count);
static void write_jsp_any_bounds(StringInfo buf, SCM expr);
static void write_jsp_arg(StringInfo buf, int32 base, int32 offset, SCM expr);
static void write_jsp_bool(StringInfo buf, SCM expr);
static int32 write_jsp_header(StringInfo buf, JsonPathItemType type);
static void write_jsp_like_regex_flags(StringInfo buf, SCM flags);
static void write_jsp_numeric(StringInfo buf, SCM expr);
static void write_jsp_string(StringInfo buf, SCM s);
static void write_jsp_uint32(StringInfo buf, SCM expr);
static SCM range_flags_to_scm(char flags);
static SCM range_bound_to_scm(const RangeBound *bound, Oid subtype_oid);
static char scm_range_flags_to_char(SCM range);
static SCM stop_command_execution(void);
static SCM spi_execute_with_receiver(SCM receiver_proc, SCM command, SCM args, SCM count);
static char *scm_string_to_pstr(SCM s);
static void throw_wrong_argument_type(const char *param_name, const char *type, SCM v);
static void throw_runtime_error(const char *format, ...);
static void throw_execute_error(ErrorData *edata);
static void handle_execute_error(void);
static void dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool dest_receive(TupleTableSlot *slot, DestReceiver *self);
static void dest_shutdown(DestReceiver *self);
static void dest_destroy(DestReceiver *self);
static SCM spi_cursor_open(SCM command, SCM args, SCM count, SCM hold, SCM name, SCM scroll);
static SCM spi_cursor_fetch(SCM cursor, SCM direction, SCM count);
static SCM spi_cursor_move(SCM cursor, SCM direction, SCM count);
static FetchDirection scm_to_fetch_direction(SCM direction);
static long scm_to_fetch_count(SCM count);
static char set_current_volatility(char v);
static SCM apply_in_sandbox(SCM proc, SCM args);
static SCM apply_with_limits(SCM proc, SCM args, SCM time_limit, SCM allocation_limit);
static SCM eval_with_limits(SCM expr, SCM module, SCM time_limit, SCM allocation_limit);
static SCM apply_0_with_handlers(SCM proc, SCM args, int internal_backtrace_frame_count);
static SCM apply_0_with_handlers_inner(void *data);
static SCM apply_0_with_handlers_error_handler(void *data, SCM key, SCM args);
static SCM apply_0_with_handlers_pre_unwind_handler(void *data, SCM key, SCM args);
static SCM find_cached_proc(HeapTuple proc_tuple);
static SCM compile_and_cache_proc(HeapTuple proc_tuple);
static SCM hash_proc_source(HeapTuple proc_tuple);
static CallLimits *get_call_limits(CallLimits *limits);
static void fetch_preamble_ids(Oid role_oid, int64 *default_preamble_id, int64 *role_preamble_id);
static void flush_module_cache_for_role(Oid role_oid);
static int64 get_cached_preamble_id(Oid role_oid);
static SCM get_cached_module(Oid role_oid);
static SCM make_sandbox_module(int64 preamble_id);
static void cache_module(Oid role_oid, int64 preamble_id, SCM module);
static void cache_proc(Oid func_oid, Oid owner_oid, SCM src_hash, SCM proc);

static Oid date_oid;
static Oid float4_oid;
static Oid float8_oid;
static Oid int2_oid;
static Oid int4_oid;
static Oid int4range_oid;
static Oid int8_oid;
static Oid numeric_oid;
static Oid timestamp_oid;
static Oid timestamptz_oid;

static SCM apply_with_limits_proc;
static SCM bit_string_data_proc;
static SCM bit_string_length_proc;
static SCM box_a_proc;
static SCM box_b_proc;
static SCM boxed_datum_type_proc;
static SCM boxed_datum_value_proc;
static SCM circle_center_proc;
static SCM circle_radius_proc;
static SCM cursor_name_proc;
static SCM date_day_proc;
static SCM date_hour_proc;
static SCM date_minute_proc;
static SCM date_month_proc;
static SCM date_nanosecond_proc;
static SCM date_second_proc;
static SCM date_year_proc;
static SCM date_zone_offset_proc;
static SCM decimal_to_inexact_proc;
static SCM decimal_to_string_proc;
static SCM eval_with_limits_proc;
static SCM flush_function_cache_proc;
static SCM inet_address_proc;
static SCM inet_bits_proc;
static SCM inet_family_proc;
static SCM is_bit_string_proc;
static SCM is_box_proc;
static SCM is_boxed_datum_proc;
static SCM is_circle_proc;
static SCM is_cursor_proc;
static SCM is_date_proc;
static SCM is_decimal_proc;
static SCM is_inet_proc;
static SCM is_int2_proc;
static SCM is_int4_proc;
static SCM is_int8_proc;
static SCM is_jsonb_proc;
static SCM is_jsonpath_proc;
static SCM is_line_proc;
static SCM is_lseg_proc;
static SCM is_macaddr8_proc;
static SCM is_macaddr_proc;
static SCM is_multirange_proc;
static SCM is_path_proc;
static SCM is_point_proc;
static SCM is_polygon_proc;
static SCM is_range_proc;
static SCM is_record_proc;
static SCM is_table_proc;
static SCM is_time_proc;
static SCM is_tsquery_proc;
static SCM is_tsvector_proc;
static SCM jsonb_expr_proc;
static SCM jsonpath_expr_proc;
static SCM jsonpath_is_strict_proc;
static SCM line_a_proc;
static SCM line_b_proc;
static SCM line_c_proc;
static SCM lseg_a_proc;
static SCM lseg_b_proc;
static SCM macaddr8_data_proc;
static SCM macaddr_data_proc;
static SCM make_bit_string_proc;
static SCM make_box_proc;
static SCM make_boxed_datum_proc;
static SCM make_circle_proc;
static SCM make_cursor_proc;
static SCM make_date_proc;
static SCM make_inet_proc;
static SCM make_jsonb_proc;
static SCM make_jsonpath_proc;
static SCM make_line_proc;
static SCM make_lseg_proc;
static SCM make_macaddr8_proc;
static SCM make_macaddr_proc;
static SCM make_multirange_proc;
static SCM make_path_proc;
static SCM make_point_proc;
static SCM make_polygon_proc;
static SCM make_range_proc;
static SCM make_record_proc;
static SCM make_sandbox_module_proc;
static SCM make_table_proc;
static SCM make_time_proc;
static SCM make_tslexeme_proc;
static SCM make_tsposition_proc;
static SCM make_tsquery_proc;
static SCM make_tsvector_proc;
static SCM multirange_ranges_proc;
static SCM normalize_tsvector_proc;
static SCM path_is_closed_proc;
static SCM path_points_proc;
static SCM point_x_proc;
static SCM point_y_proc;
static SCM polygon_boundbox_proc;
static SCM polygon_points_proc;
static SCM range_flags_proc;
static SCM range_lower_proc;
static SCM range_upper_proc;
static SCM record_attr_names_proc;
static SCM record_attrs_proc;
static SCM record_types_proc;
static SCM role_add_func_oid_proc;
static SCM role_to_func_oids_proc;
static SCM string_to_decimal_proc;
static SCM table_attr_names_proc;
static SCM table_rows_proc;
static SCM table_types_proc;
static SCM time_duration_symbol;
static SCM time_monotonic_symbol;
static SCM time_nanosecond_proc;
static SCM time_second_proc;
static SCM trusted_bindings;
static SCM tslexeme_lexeme_proc;
static SCM tslexeme_positions_proc;
static SCM tsposition_index_proc;
static SCM tsposition_weight_proc;
static SCM tsquery_expr_proc;
static SCM tsvector_lexemes_proc;
static SCM untrusted_eval_proc;
static SCM validate_tsquery_proc;
//static SCM validate_jsonb_proc;
static SCM validate_jsonpath_proc;

// Symbols for triggers
static SCM after_symbol;
static SCM before_symbol;
static SCM delete_symbol;
static SCM insert_symbol;
static SCM row_symbol;
static SCM statement_symbol;
static SCM truncate_symbol;
static SCM update_symbol;

// Symbols for range type flags
static SCM empty_symbol;
static SCM lower_inclusive_symbol;
static SCM lower_infinite_symbol;
static SCM upper_inclusive_symbol;
static SCM upper_infinite_symbol;

// Symbols for tsquery
static SCM and_symbol;
static SCM not_symbol;
static SCM or_symbol;
static SCM phrase_symbol;
static SCM value_symbol;

// Symbols for jsonpath
static SCM abs_symbol;
static SCM add_symbol;
static SCM any_array_symbol;
static SCM any_key_symbol;
static SCM any_symbol;
static SCM ceiling_symbol;
static SCM current_symbol;
static SCM datetime_symbol;
static SCM div_symbol;
static SCM dotall_symbol;
static SCM double_symbol;
static SCM equal_symbol;
static SCM exists_symbol;
static SCM filter_symbol;
static SCM floor_symbol;
static SCM greater_or_equal_symbol;
static SCM greater_symbol;
static SCM icase_symbol;
static SCM index_array_symbol;
static SCM is_unknown_symbol;
static SCM key_symbol;
static SCM keyvalue_symbol;
static SCM last_symbol;
static SCM less_or_equal_symbol;
static SCM less_symbol;
static SCM like_regex_symbol;
static SCM negate_symbol;
static SCM mline_symbol;
static SCM mod_symbol;
static SCM mul_symbol;
static SCM nop_symbol;
static SCM not_equal_symbol;
static SCM quote_symbol;
static SCM root_symbol;
static SCM size_symbol;
static SCM starts_with_symbol;
//static SCM strict_symbol;
static SCM sub_symbol;
static SCM type_symbol;
static SCM var_symbol;
static SCM wspace_symbol;

// symbols for jsonb
static SCM null_symbol;

// symbols for cursors
static SCM absolute_symbol;
static SCM all_symbol;
static SCM backward_symbol;
static SCM forward_symbol;
static SCM relative_symbol;

static SCM jsp_op_types_hash;

static SCM stop_marker;

static SCM func_cache;
static SCM module_cache;

static HTAB *range_cache;
static HTAB *type_cache;

static SCM unbox_datum(SCM x);
static SCM spi_execute(SCM command, SCM args, SCM count);

static SCM plg3_base_module = SCM_UNDEFINED;

void _PG_init(void)
{
	HASHCTL range_info;
	HASHCTL type_info;

	// TODO: check that `SHOW server_encoding` is 'UTF8'

	memset(&range_info, 0, sizeof(range_info));
	range_info.keysize = sizeof(Oid);
	range_info.entrysize = sizeof(RangeCacheEntry);
	range_cache = hash_create("plg3 range cache", 128, &range_info, HASH_ELEM | HASH_BLOBS);

	memset(&type_info, 0, sizeof(type_info));
	type_info.keysize = sizeof(Oid);
	type_info.entrysize = sizeof(TypeConvCacheEntry);
	type_cache = hash_create("plg3 type cache", 128, &type_info, HASH_ELEM | HASH_BLOBS);

	date_oid        = TypenameGetTypid("date");
	int2_oid        = TypenameGetTypid("int2");
	int4_oid        = TypenameGetTypid("int4");
	int4range_oid   = TypenameGetTypid("int4range");
	int8_oid        = TypenameGetTypid("int8");
	float4_oid      = TypenameGetTypid("float4");
	float8_oid      = TypenameGetTypid("float8");
	numeric_oid     = TypenameGetTypid("numeric");
	timestamp_oid   = TypenameGetTypid("timestamp");
	timestamptz_oid = TypenameGetTypid("timestamptz");

	insert_range_cache_entry(date_oid,        TypenameGetTypid("daterange"), TypenameGetTypid("datemultirange"));
	insert_range_cache_entry(int2_oid,        TypenameGetTypid("int4range"), TypenameGetTypid("int4multirange"));
	insert_range_cache_entry(int4_oid,        TypenameGetTypid("int4range"), TypenameGetTypid("int4multirange"));
	insert_range_cache_entry(int8_oid,        TypenameGetTypid("int8range"), TypenameGetTypid("int8multirange"));
	insert_range_cache_entry(numeric_oid,     TypenameGetTypid("numrange"),  TypenameGetTypid("nummultirange"));
	insert_range_cache_entry(timestamp_oid,   TypenameGetTypid("tsrange"),   TypenameGetTypid("tsmultirange"));
	insert_range_cache_entry(timestamptz_oid, TypenameGetTypid("tstzrange"), TypenameGetTypid("tstzmultirange"));

	/* Fill type cache with to_scm and to_datum functions for known types */
	insert_type_cache_entry(TypenameGetTypid("bit"),            datum_bit_string_to_scm,     scm_to_datum_bit_string);
	insert_type_cache_entry(TypenameGetTypid("bool"),           datum_bool_to_scm,           scm_to_datum_bool);
	insert_type_cache_entry(TypenameGetTypid("box"),            datum_box_to_scm,            scm_to_datum_box);
	insert_type_cache_entry(TypenameGetTypid("bpchar"),         datum_text_to_scm,           scm_to_datum_text);
	insert_type_cache_entry(TypenameGetTypid("bytea"),          datum_bytea_to_scm,          scm_to_datum_bytea);
	insert_type_cache_entry(TypenameGetTypid("char"),           datum_text_to_scm,           scm_to_datum_text);
	insert_type_cache_entry(TypenameGetTypid("cidr"),           datum_inet_to_scm,           scm_to_datum_inet);
	insert_type_cache_entry(TypenameGetTypid("circle"),         datum_circle_to_scm,         scm_to_datum_circle);
	insert_type_cache_entry(TypenameGetTypid("date"),           datum_date_to_scm,           scm_to_datum_date);
	insert_type_cache_entry(TypenameGetTypid("datemultirange"), datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("daterange"),      datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("float4"),         datum_float4_to_scm,         scm_to_datum_float4);
	insert_type_cache_entry(TypenameGetTypid("float8"),         datum_float8_to_scm,         scm_to_datum_float8);
	insert_type_cache_entry(TypenameGetTypid("inet"),           datum_inet_to_scm,           scm_to_datum_inet);
	insert_type_cache_entry(TypenameGetTypid("int2"),           datum_int2_to_scm,           scm_to_datum_int2);
	insert_type_cache_entry(TypenameGetTypid("int4"),           datum_int4_to_scm,           scm_to_datum_int4);
	insert_type_cache_entry(TypenameGetTypid("int4multirange"), datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("int4range"),      datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("int8"),           datum_int8_to_scm,           scm_to_datum_int8);
	insert_type_cache_entry(TypenameGetTypid("int8multirange"), datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("int8range"),      datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("interval"),       datum_interval_to_scm,       scm_to_datum_interval);
	insert_type_cache_entry(TypenameGetTypid("json"),           datum_json_to_scm,           scm_to_datum_json);
	insert_type_cache_entry(TypenameGetTypid("jsonb"),          datum_jsonb_to_scm,          scm_to_datum_jsonb);
	insert_type_cache_entry(TypenameGetTypid("jsonpath"),       datum_jsonpath_to_scm,       scm_to_datum_jsonpath);
	insert_type_cache_entry(TypenameGetTypid("line"),           datum_line_to_scm,           scm_to_datum_line);
	insert_type_cache_entry(TypenameGetTypid("lseg"),           datum_lseg_to_scm,           scm_to_datum_lseg);
	insert_type_cache_entry(TypenameGetTypid("macaddr"),        datum_macaddr_to_scm,        scm_to_datum_macaddr);
	insert_type_cache_entry(TypenameGetTypid("macaddr8"),       datum_macaddr8_to_scm,       scm_to_datum_macaddr8);
	insert_type_cache_entry(TypenameGetTypid("money"),          datum_int8_to_scm,           scm_to_datum_int8);
	insert_type_cache_entry(TypenameGetTypid("numeric"),        datum_numeric_to_scm,        scm_to_datum_numeric);
	insert_type_cache_entry(TypenameGetTypid("nummultirange"),  datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("numrange"),       datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("path"),           datum_path_to_scm,           scm_to_datum_path);
	insert_type_cache_entry(TypenameGetTypid("point"),          datum_point_to_scm,          scm_to_datum_point);
	insert_type_cache_entry(TypenameGetTypid("polygon"),        datum_polygon_to_scm,        scm_to_datum_polygon);
	insert_type_cache_entry(TypenameGetTypid("record"),         datum_composite_to_scm,      scm_to_datum_record);
	insert_type_cache_entry(TypenameGetTypid("text"),           datum_text_to_scm,           scm_to_datum_text);
	insert_type_cache_entry(TypenameGetTypid("time"),           datum_time_to_scm,           scm_to_datum_time);
	insert_type_cache_entry(TypenameGetTypid("timestamp"),      datum_timestamptz_to_scm,    scm_to_datum_timestamptz);
	insert_type_cache_entry(TypenameGetTypid("timestamptz"),    datum_timestamptz_to_scm,    scm_to_datum_timestamptz);
	insert_type_cache_entry(TypenameGetTypid("timetz"),         datum_timetz_to_scm,         scm_to_datum_timetz);
	insert_type_cache_entry(TypenameGetTypid("tsmultirange"),   datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("tsquery"),        datum_tsquery_to_scm,        scm_to_datum_tsquery);
	insert_type_cache_entry(TypenameGetTypid("tsrange"),        datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("tstzmultirange"), datum_multirange_to_scm,     scm_to_datum_multirange);
	insert_type_cache_entry(TypenameGetTypid("tstzrange"),      datum_range_to_scm,          scm_to_datum_range);
	insert_type_cache_entry(TypenameGetTypid("tsvector"),       datum_tsvector_to_scm,       scm_to_datum_tsvector);
	insert_type_cache_entry(TypenameGetTypid("uuid"),           datum_uuid_to_scm,           scm_to_datum_uuid);
	insert_type_cache_entry(TypenameGetTypid("varbit"),         datum_bit_string_to_scm,     scm_to_datum_bit_string);
	insert_type_cache_entry(TypenameGetTypid("varchar"),        datum_text_to_scm,           scm_to_datum_text);
	insert_type_cache_entry(TypenameGetTypid("void"),           datum_void_to_scm,           scm_to_datum_void);
	insert_type_cache_entry(TypenameGetTypid("xml"),            datum_xml_to_scm,            scm_to_datum_xml);

	/* Initialize the Guile interpreter */
	scm_init_guile();

	//elog(NOTICE, "evaluating plg3.scm");
	plg3_base_module = load_base_module();
	//elog(NOTICE, "done");

	func_cache = scm_c_make_hash_table(16);
	scm_gc_protect_object(func_cache);

	module_cache = scm_c_make_hash_table(16);
	scm_gc_protect_object(module_cache);

	define_primitive("%cursor-open",           6, 0, 0, (SCM (*)()) spi_cursor_open);
	define_primitive("%execute",               3, 0, 0, (SCM (*)()) spi_execute);
	define_primitive("%execute-with-receiver", 4, 0, 0, (SCM (*)()) spi_execute_with_receiver);
	define_primitive("%fetch",                 3, 0, 0, (SCM (*)()) spi_cursor_fetch);
	define_primitive("%move",                  3, 0, 0, (SCM (*)()) spi_cursor_move);
	define_primitive("stop-command-execution", 0, 0, 0, (SCM (*)()) stop_command_execution);
	define_primitive("unbox-datum",            1, 0, 0, (SCM (*)()) unbox_datum);

	/* Define names in our scheme module for the type oids we work with. */

	// scm_c_define("bit-type-oid",         scm_from_int(TypenameGetTypid("bit")));
	// scm_c_define("bool-type-oid",        scm_from_int(TypenameGetTypid("bool")));
	// scm_c_define("box-type-oid",         scm_from_int(TypenameGetTypid("box")));
	// scm_c_define("bpchar-type-oid",      scm_from_int(TypenameGetTypid("bpchar")));
	// scm_c_define("bytea-type-oid",       scm_from_int(TypenameGetTypid("bytea")));
	// scm_c_define("char-type-oid",        scm_from_int(TypenameGetTypid("char")));
	// scm_c_define("cidr-type-oid",        scm_from_int(TypenameGetTypid("cidr")));
	// scm_c_define("circle-type-oid",      scm_from_int(TypenameGetTypid("circle")));
	// scm_c_define("date-type-oid",        scm_from_int(TypenameGetTypid("date")));
	// scm_c_define("float4-type-oid",      scm_from_int(TypenameGetTypid("float4")));
	// scm_c_define("float8-type-oid",      scm_from_int(TypenameGetTypid("float8")));
	// scm_c_define("inet-type-oid",        scm_from_int(TypenameGetTypid("inet")));
	// scm_c_define("int2-type-oid",        scm_from_int(TypenameGetTypid("int2")));
	// scm_c_define("int4-type-oid",        scm_from_int(TypenameGetTypid("int4")));
	// scm_c_define("int8-type-oid",        scm_from_int(TypenameGetTypid("int8")));
	// scm_c_define("interval-type-oid",    scm_from_int(TypenameGetTypid("interval")));
	// scm_c_define("json-type-oid",        scm_from_int(TypenameGetTypid("json")));
	// scm_c_define("jsonb-type-oid",       scm_from_int(TypenameGetTypid("jsonb")));
	// scm_c_define("line-type-oid",        scm_from_int(TypenameGetTypid("line")));
	// scm_c_define("lseg-type-oid",        scm_from_int(TypenameGetTypid("lseg")));
	// scm_c_define("macaddr-type-oid",     scm_from_int(TypenameGetTypid("macaddr")));
	// scm_c_define("macaddr8-type-oid",    scm_from_int(TypenameGetTypid("macaddr8")));
	// scm_c_define("money-type-oid",       scm_from_int(TypenameGetTypid("money")));
	// scm_c_define("numeric-type-oid",     scm_from_int(TypenameGetTypid("numeric")));
	// scm_c_define("path-type-oid",        scm_from_int(TypenameGetTypid("path")));
	// scm_c_define("point-type-oid",       scm_from_int(TypenameGetTypid("point")));
	// scm_c_define("polygon-type-oid",     scm_from_int(TypenameGetTypid("polygon")));
	// scm_c_define("text-type-oid",        scm_from_int(TypenameGetTypid("text")));
	// scm_c_define("time-type-oid",        scm_from_int(TypenameGetTypid("time")));
	// scm_c_define("timetz-type-oid",      scm_from_int(TypenameGetTypid("timetz")));
	// scm_c_define("timestamp-type-oid",   scm_from_int(TypenameGetTypid("timestamp")));
	// scm_c_define("timestamptz-type-oid", scm_from_int(TypenameGetTypid("timestamptz")));
	// scm_c_define("uuid-type-oid",        scm_from_int(TypenameGetTypid("uuid")));
	// scm_c_define("varbit-type-oid",      scm_from_int(TypenameGetTypid("varbit")));
	// scm_c_define("varchar-type-oid",     scm_from_int(TypenameGetTypid("varchar")));
	// scm_c_define("void-type-oid",        scm_from_int(TypenameGetTypid("void")));
	// scm_c_define("xml-type-oid",         scm_from_int(TypenameGetTypid("xml")));

	/* Procedures defined by define-record-type are inlinable, meaning that instead of being
	   procedures, they are actually syntax transformers.  In non-call contexts, they refer to
	   the original procedure, but in call contexts, they interpolate the body of the
	   procedure. The upshot to this is that using scm_c_lookup gets the syntax transfomer
	   procedure, not the inlined procedure. To fix this, we just eval the name, inducing the
	   syntax transformer to give us proc we need.
	*/

	apply_with_limits_proc      = eval_string_in_base_module("apply-with-limits");
	bit_string_data_proc        = eval_string_in_base_module("bit-string-data");
	bit_string_length_proc      = eval_string_in_base_module("bit-string-length");
	box_a_proc                  = eval_string_in_base_module("box-a");
	box_b_proc                  = eval_string_in_base_module("box-b");
	boxed_datum_type_proc       = eval_string_in_base_module("boxed-datum-type");
	boxed_datum_value_proc      = eval_string_in_base_module("boxed-datum-value");
	circle_center_proc          = eval_string_in_base_module("circle-center");
	circle_radius_proc          = eval_string_in_base_module("circle-radius");
	cursor_name_proc            = eval_string_in_base_module("cursor-name");
	date_day_proc               = eval_string_in_base_module("date-day");
	date_hour_proc              = eval_string_in_base_module("date-hour");
	date_minute_proc            = eval_string_in_base_module("date-minute");
	date_month_proc             = eval_string_in_base_module("date-month");
	date_nanosecond_proc        = eval_string_in_base_module("date-nanosecond");
	date_second_proc            = eval_string_in_base_module("date-second");
	date_year_proc              = eval_string_in_base_module("date-year");
	date_zone_offset_proc       = eval_string_in_base_module("date-zone-offset");
	eval_with_limits_proc       = eval_string_in_base_module("eval-with-limits");
	flush_function_cache_proc   = eval_string_in_base_module("flush-function-cache");
	inet_address_proc           = eval_string_in_base_module("inet-address");
	inet_bits_proc              = eval_string_in_base_module("inet-bits");
	inet_family_proc            = eval_string_in_base_module("inet-family");
	is_bit_string_proc          = eval_string_in_base_module("bit-string?");
	is_box_proc                 = eval_string_in_base_module("box?");
	is_boxed_datum_proc         = eval_string_in_base_module("boxed-datum?");
	is_circle_proc              = eval_string_in_base_module("circle?");
	is_cursor_proc              = eval_string_in_base_module("cursor?");
	is_date_proc                = eval_string_in_base_module("date?");
	is_decimal_proc             = eval_string_in_base_module("decimal?");
	is_inet_proc                = eval_string_in_base_module("inet?");
	is_jsonb_proc               = eval_string_in_base_module("jsonb?");
	is_jsonpath_proc            = eval_string_in_base_module("jsonpath?");
	is_line_proc                = eval_string_in_base_module("line?");
	is_lseg_proc                = eval_string_in_base_module("lseg?");
	is_macaddr8_proc            = eval_string_in_base_module("macaddr8?");
	is_macaddr_proc             = eval_string_in_base_module("macaddr?");
	is_multirange_proc          = eval_string_in_base_module("multirange?");
	is_path_proc                = eval_string_in_base_module("path?");
	is_point_proc               = eval_string_in_base_module("point?");
	is_polygon_proc             = eval_string_in_base_module("polygon?");
	is_range_proc               = eval_string_in_base_module("range?");
	is_record_proc              = eval_string_in_base_module("record?");
	is_table_proc               = eval_string_in_base_module("table?");
	is_time_proc                = eval_string_in_base_module("time?");
	is_tsquery_proc             = eval_string_in_base_module("tsquery?");
	is_tsvector_proc            = eval_string_in_base_module("tsvector?");
	jsonb_expr_proc             = eval_string_in_base_module("jsonb-expr");
	jsonpath_expr_proc          = eval_string_in_base_module("jsonpath-expr");
	jsonpath_is_strict_proc     = eval_string_in_base_module("jsonpath-strict?");
	line_a_proc                 = eval_string_in_base_module("line-a");
	line_b_proc                 = eval_string_in_base_module("line-b");
	line_c_proc                 = eval_string_in_base_module("line-c");
	lseg_a_proc                 = eval_string_in_base_module("lseg-a");
	lseg_b_proc                 = eval_string_in_base_module("lseg-b");
	macaddr8_data_proc          = eval_string_in_base_module("macaddr8-data");
	macaddr_data_proc           = eval_string_in_base_module("macaddr-data");
	make_bit_string_proc        = eval_string_in_base_module("make-bit-string");
	make_box_proc               = eval_string_in_base_module("make-box");
	make_boxed_datum_proc       = eval_string_in_base_module("make-boxed-datum");
	make_circle_proc            = eval_string_in_base_module("make-circle");
	make_cursor_proc            = eval_string_in_base_module("make-cursor");
	make_date_proc              = eval_string_in_base_module("make-date");
	make_inet_proc              = eval_string_in_base_module("make-inet");
	make_jsonb_proc             = eval_string_in_base_module("make-jsonb");
	make_jsonpath_proc          = eval_string_in_base_module("make-jsonpath");
	make_line_proc              = eval_string_in_base_module("make-line");
	make_lseg_proc              = eval_string_in_base_module("make-lseg");
	make_multirange_proc        = eval_string_in_base_module("make-multirange");
	make_macaddr8_proc          = eval_string_in_base_module("make-macaddr8");
	make_macaddr_proc           = eval_string_in_base_module("make-macaddr");
	make_path_proc              = eval_string_in_base_module("make-path");
	make_point_proc             = eval_string_in_base_module("make-point");
	make_polygon_proc           = eval_string_in_base_module("make-polygon");
	make_range_proc             = eval_string_in_base_module("make-range");
	make_record_proc            = eval_string_in_base_module("make-record");
	make_sandbox_module_proc    = eval_string_in_base_module("make-sandbox-module");
	make_table_proc             = eval_string_in_base_module("make-table");
	make_time_proc              = eval_string_in_base_module("make-time");
	make_tslexeme_proc          = eval_string_in_base_module("make-tslexeme");
	make_tsposition_proc        = eval_string_in_base_module("make-tsposition");
	make_tsquery_proc           = eval_string_in_base_module("make-tsquery");
	make_tsvector_proc          = eval_string_in_base_module("make-tsvector");
	multirange_ranges_proc      = eval_string_in_base_module("multirange-ranges");
	normalize_tsvector_proc     = eval_string_in_base_module("normalize-tsvector");
	path_is_closed_proc         = eval_string_in_base_module("path-closed?");
	path_points_proc            = eval_string_in_base_module("path-points");
	point_x_proc                = eval_string_in_base_module("point-x");
	point_y_proc                = eval_string_in_base_module("point-y");
	polygon_boundbox_proc       = eval_string_in_base_module("polygon-boundbox");
	polygon_points_proc         = eval_string_in_base_module("polygon-points");
	range_flags_proc            = eval_string_in_base_module("range-flags");
	range_lower_proc            = eval_string_in_base_module("range-lower");
	range_upper_proc            = eval_string_in_base_module("range-upper");
	record_attr_names_proc      = eval_string_in_base_module("record-attr-names");
	record_attrs_proc           = eval_string_in_base_module("record-attrs");
	record_types_proc           = eval_string_in_base_module("record-types");
	role_add_func_oid_proc      = eval_string_in_base_module("role-add-func-oid!");
	role_to_func_oids_proc      = eval_string_in_base_module("role->func-oids");
	table_attr_names_proc       = eval_string_in_base_module("table-attr-names");
	table_rows_proc             = eval_string_in_base_module("table-rows");
	table_types_proc            = eval_string_in_base_module("table-types");
	time_duration_symbol        = eval_string_in_base_module("time-duration");
	time_monotonic_symbol       = eval_string_in_base_module("time-monotonic");
	time_nanosecond_proc        = eval_string_in_base_module("time-nanosecond");
	time_second_proc            = eval_string_in_base_module("time-second");
	trusted_bindings            = eval_string_in_base_module("trusted-bindings");
	tslexeme_lexeme_proc        = eval_string_in_base_module("tslexeme-lexeme");
	tslexeme_positions_proc     = eval_string_in_base_module("tslexeme-positions");
	tsposition_index_proc       = eval_string_in_base_module("tsposition-index");
	tsposition_weight_proc      = eval_string_in_base_module("tsposition-weight");
	tsquery_expr_proc           = eval_string_in_base_module("tsquery-expr");
	tsvector_lexemes_proc       = eval_string_in_base_module("tsvector-lexemes");
	untrusted_eval_proc         = eval_string_in_base_module("untrusted-eval");
	validate_jsonpath_proc      = eval_string_in_base_module("validate-jsonpath");
	validate_tsquery_proc       = eval_string_in_base_module("validate-tsquery");

	decimal_to_string_proc  = eval_string_in_base_module("decimal->string");
	decimal_to_inexact_proc = eval_string_in_base_module("decimal->inexact");
	is_int2_proc            = eval_string_in_base_module("int2-compatible?");
	is_int4_proc            = eval_string_in_base_module("int4-compatible?");
	is_int8_proc            = eval_string_in_base_module("int8-compatible?");
	string_to_decimal_proc  = eval_string_in_base_module("string->decimal");

	after_symbol     = scm_from_utf8_symbol("after");
	before_symbol    = scm_from_utf8_symbol("before");
	delete_symbol    = scm_from_utf8_symbol("delete");
	insert_symbol    = scm_from_utf8_symbol("insert");
	row_symbol       = scm_from_utf8_symbol("row");
	statement_symbol = scm_from_utf8_symbol("statement");
	truncate_symbol  = scm_from_utf8_symbol("truncate");
	update_symbol    = scm_from_utf8_symbol("update");

	empty_symbol           = scm_from_utf8_symbol("empty");
	lower_inclusive_symbol = scm_from_utf8_symbol("lower-inclusive");
	lower_infinite_symbol  = scm_from_utf8_symbol("lower-infinite");
	upper_inclusive_symbol = scm_from_utf8_symbol("upper-inclusive");
	upper_infinite_symbol  = scm_from_utf8_symbol("upper-infinite");

	and_symbol    = scm_from_utf8_symbol("and");
	not_symbol    = scm_from_utf8_symbol("not");
	or_symbol     = scm_from_utf8_symbol("or");
	phrase_symbol = scm_from_utf8_symbol("phrase");
	value_symbol  = scm_from_utf8_symbol("value");

	abs_symbol              = scm_from_utf8_symbol("abs");
	add_symbol              = scm_from_utf8_symbol("+");
	any_array_symbol        = scm_from_utf8_symbol("any-array");
	any_key_symbol          = scm_from_utf8_symbol("any-key");
	any_symbol              = scm_from_utf8_symbol("any");
	ceiling_symbol          = scm_from_utf8_symbol("ceiling");
	current_symbol          = scm_from_utf8_symbol("@");
	datetime_symbol         = scm_from_utf8_symbol("datetime");
	div_symbol              = scm_from_utf8_symbol("/");
	dotall_symbol           = scm_from_utf8_symbol("dot-matches-newline");
	double_symbol           = scm_from_utf8_symbol("double");
	equal_symbol            = scm_from_utf8_symbol("=");
	exists_symbol           = scm_from_utf8_symbol("exists");
	filter_symbol           = scm_from_utf8_symbol("filter");
	floor_symbol            = scm_from_utf8_symbol("floor");
	greater_symbol          = scm_from_utf8_symbol(">");
	greater_or_equal_symbol = scm_from_utf8_symbol(">=");
	icase_symbol            = scm_from_utf8_symbol("ignore-case");
	index_array_symbol      = scm_from_utf8_symbol("index-array");
	is_unknown_symbol       = scm_from_utf8_symbol("unknown?");
	key_symbol              = scm_from_utf8_symbol("key");
	keyvalue_symbol         = scm_from_utf8_symbol("keyvalue");
	last_symbol             = scm_from_utf8_symbol("last");
	less_symbol             = scm_from_utf8_symbol("<");
	less_or_equal_symbol    = scm_from_utf8_symbol("<=");
	like_regex_symbol       = scm_from_utf8_symbol("like-regex");
	negate_symbol           = scm_from_utf8_symbol("negate");
	mline_symbol            = scm_from_utf8_symbol("multi-line");
	mod_symbol              = scm_from_utf8_symbol("%");
	mul_symbol              = scm_from_utf8_symbol("*");
	nop_symbol              = scm_from_utf8_symbol("nop");
	not_equal_symbol        = scm_from_utf8_symbol("!=");
	quote_symbol            = scm_from_utf8_symbol("literal");
	root_symbol             = scm_from_utf8_symbol("root");
	size_symbol             = scm_from_utf8_symbol("size");
	starts_with_symbol      = scm_from_utf8_symbol("starts-with");
	sub_symbol              = scm_from_utf8_symbol("-");
	type_symbol             = scm_from_utf8_symbol("type");
	var_symbol              = scm_from_utf8_symbol("var");
	wspace_symbol           = scm_from_utf8_symbol("whitespace");

	null_symbol           = scm_from_utf8_symbol("null");

	absolute_symbol = scm_from_utf8_symbol("absolute");
	all_symbol = scm_from_utf8_symbol("all");
	backward_symbol = scm_from_utf8_symbol("backward");
	forward_symbol = scm_from_utf8_symbol("forward");
	relative_symbol = scm_from_utf8_symbol("relative");

	jsp_op_types_hash = scm_c_make_hash_table(39); // will have 39 symbols in it

	scm_hash_set_x(jsp_op_types_hash, abs_symbol,              scm_from_int(jpiAbs));
	scm_hash_set_x(jsp_op_types_hash, add_symbol,              scm_from_int(jpiAdd));
	scm_hash_set_x(jsp_op_types_hash, and_symbol,              scm_from_int(jpiAnd));
	scm_hash_set_x(jsp_op_types_hash, any_array_symbol,        scm_from_int(jpiAnyArray));
	scm_hash_set_x(jsp_op_types_hash, any_key_symbol,          scm_from_int(jpiAnyKey));
	scm_hash_set_x(jsp_op_types_hash, any_symbol,              scm_from_int(jpiAny));
	scm_hash_set_x(jsp_op_types_hash, ceiling_symbol,          scm_from_int(jpiCeiling));
	scm_hash_set_x(jsp_op_types_hash, current_symbol,          scm_from_int(jpiCurrent));
	scm_hash_set_x(jsp_op_types_hash, datetime_symbol,         scm_from_int(jpiDatetime));
	scm_hash_set_x(jsp_op_types_hash, div_symbol,              scm_from_int(jpiDiv));
	scm_hash_set_x(jsp_op_types_hash, double_symbol,           scm_from_int(jpiDouble));
	scm_hash_set_x(jsp_op_types_hash, equal_symbol,            scm_from_int(jpiEqual));
	scm_hash_set_x(jsp_op_types_hash, exists_symbol,           scm_from_int(jpiExists));
	scm_hash_set_x(jsp_op_types_hash, filter_symbol,           scm_from_int(jpiFilter));
	scm_hash_set_x(jsp_op_types_hash, floor_symbol,            scm_from_int(jpiFloor));
	scm_hash_set_x(jsp_op_types_hash, greater_or_equal_symbol, scm_from_int(jpiGreaterOrEqual));
	scm_hash_set_x(jsp_op_types_hash, greater_symbol,          scm_from_int(jpiGreater));
	scm_hash_set_x(jsp_op_types_hash, index_array_symbol,      scm_from_int(jpiIndexArray));
	scm_hash_set_x(jsp_op_types_hash, is_unknown_symbol,       scm_from_int(jpiIsUnknown));
	scm_hash_set_x(jsp_op_types_hash, key_symbol,              scm_from_int(jpiKey));
	scm_hash_set_x(jsp_op_types_hash, keyvalue_symbol,         scm_from_int(jpiKeyValue));
	scm_hash_set_x(jsp_op_types_hash, last_symbol,             scm_from_int(jpiLast));
	scm_hash_set_x(jsp_op_types_hash, less_or_equal_symbol,    scm_from_int(jpiLessOrEqual));
	scm_hash_set_x(jsp_op_types_hash, less_symbol,             scm_from_int(jpiLess));
	scm_hash_set_x(jsp_op_types_hash, like_regex_symbol,       scm_from_int(jpiLikeRegex));
	scm_hash_set_x(jsp_op_types_hash, negate_symbol,           scm_from_int(jpiMinus));
	scm_hash_set_x(jsp_op_types_hash, mod_symbol,              scm_from_int(jpiMod));
	scm_hash_set_x(jsp_op_types_hash, mul_symbol,              scm_from_int(jpiMul));
	scm_hash_set_x(jsp_op_types_hash, not_equal_symbol,        scm_from_int(jpiNotEqual));
	scm_hash_set_x(jsp_op_types_hash, nop_symbol,              scm_from_int(jpiPlus));
	scm_hash_set_x(jsp_op_types_hash, not_symbol,              scm_from_int(jpiNot));
	scm_hash_set_x(jsp_op_types_hash, or_symbol,               scm_from_int(jpiOr));
	scm_hash_set_x(jsp_op_types_hash, root_symbol,             scm_from_int(jpiRoot));
	scm_hash_set_x(jsp_op_types_hash, size_symbol,             scm_from_int(jpiSize));
	scm_hash_set_x(jsp_op_types_hash, starts_with_symbol,      scm_from_int(jpiStartsWith));
	scm_hash_set_x(jsp_op_types_hash, sub_symbol,              scm_from_int(jpiSub));
	scm_hash_set_x(jsp_op_types_hash, type_symbol,             scm_from_int(jpiType));
	scm_hash_set_x(jsp_op_types_hash, var_symbol,              scm_from_int(jpiVariable));

	// unique object used to signal desired end of processing
	// during execute-with-receiver
	stop_marker = scm_cons(SCM_EOL, SCM_EOL);
	scm_gc_protect_object(stop_marker);

	//elog(NOTICE, "initialization complete");
}

static inline Oid get_language_owner()
{
	return owner_oid != InvalidOid ? owner_oid : get_guile3_owner_oid();
}

Oid get_guile3_owner_oid(void)
{
	HeapTuple lang_tuple;
	bool is_null;

	lang_tuple = SearchSysCache1(LANGNAME, CStringGetDatum("guile3"));

	if (lang_tuple == NULL)
		elog(ERROR, "language guile3 not found");

	owner_oid = SysCacheGetAttr(LANGOID, lang_tuple, Anum_pg_language_lanowner, &is_null);

	if (is_null)
		elog(ERROR, "language guile3 has no owner");

	ReleaseSysCache(lang_tuple);

	return owner_oid;
}

SCM load_base_module(void)
{
    return scm_internal_catch(
        SCM_BOOL_T,
        base_module_loader, NULL,
        load_base_module_error_handler, NULL);
}

SCM base_module_loader(void *data)
{
	SCM text = scm_from_locale_string((const char *)src_plg3_scm);
	scm_eval_string_in_module(text, scm_current_module());
	return scm_c_resolve_module("plg3 base");
}

SCM load_base_module_error_handler(void *data, SCM key, SCM args)
{
	elog(ERROR, "Unable to load plg3 base module: %s %s", scm_to_string(key),
	     scm_to_string(args));
}

void define_primitive(const char *name, int req, int opt, int rst, scm_t_subr fcn)
{
	scm_c_module_define(plg3_base_module, name, scm_c_make_gsubr(name, req, opt, rst, fcn));
}

SCM call_1(SCM func, SCM arg1)
{
    SCM args[2] = {func, arg1};

    return scm_internal_catch(
        SCM_BOOL_T,
        call_1_executor, args,
        call_error_handler, args);
}

SCM call_1_executor(void *data)
{
    SCM *args = (SCM *)data;
    return scm_call_1(args[0], args[1]);
}

SCM call_2(SCM func, SCM arg1, SCM arg2)
{
	SCM args[3] = {func, arg1, arg2};

    return scm_internal_catch(
        SCM_BOOL_T,
        call_2_executor, args,
        call_error_handler, args);
}

SCM call_2_executor(void *data)
{
    SCM *args = (SCM *)data;
    return scm_call_2(args[0], args[1], args[2]);
}

SCM call_3(SCM func, SCM arg1, SCM arg2, SCM arg3)
{
	SCM args[4] = {func, arg1, arg2, arg3};

    return scm_internal_catch(
        SCM_BOOL_T,
        call_3_executor, args,
        call_error_handler, args);
}

SCM call_3_executor(void *data)
{
    SCM *args = (SCM *)data;
    return scm_call_3(args[0], args[1], args[2], args[3]);
}

SCM call_4(SCM func, SCM arg1, SCM arg2, SCM arg3, SCM arg4)
{
	SCM args[5] = {func, arg1, arg2, arg3, arg4};

    return scm_internal_catch(
        SCM_BOOL_T,
        call_4_executor, args,
        call_error_handler, args);
}

SCM call_4_executor(void *data)
{
    SCM *args = (SCM *)data;
    return scm_call_4(args[0], args[1], args[2], args[3], args[4]);
}

SCM call_8(SCM func, SCM arg1, SCM arg2, SCM arg3, SCM arg4, SCM arg5, SCM arg6, SCM arg7, SCM arg8)
{
	SCM args[9] = {func, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8};

    return scm_internal_catch(
        SCM_BOOL_T,
        call_8_executor, args,
        call_error_handler, args);
}

SCM call_8_executor(void *data)
{
    SCM *args = (SCM *)data;
    return scm_call_8(
	    args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
}

SCM call_error_handler(void *data, SCM key, SCM args)
{
	elog(ERROR, "Unable to call %s base module: %s %s", scm_to_string(*(SCM *)data),
	     scm_to_string(key), scm_to_string(args));
}

void _PG_fini(void)
{
	scm_gc_unprotect_object(func_cache);

	/* Clean up hash tables */
	hash_destroy(range_cache);
	hash_destroy(type_cache);
}

Datum plg3_call(PG_FUNCTION_ARGS)
{
	if (CALLED_AS_TRIGGER(fcinfo))
		return call_trigger(fcinfo);

	if (CALLED_AS_EVENT_TRIGGER(fcinfo))
		return call_event_trigger(fcinfo);

	return call_ordinary(fcinfo);
}

Datum call_event_trigger(FunctionCallInfo fcinfo)
{
	EventTriggerData *event_trigger_data = (EventTriggerData *)fcinfo->context;

	SCM proc = find_or_compile_proc(fcinfo->flinfo->fn_oid);
	SCM args = prepare_event_trigger_arguments(event_trigger_data);

	apply_in_sandbox(proc, args);

	return PointerGetDatum(NULL);
}

SCM prepare_event_trigger_arguments(EventTriggerData *event_trigger_data)
{
	SCM event = scm_from_locale_string(event_trigger_data->event);
	SCM parse_tree = SCM_EOL;
	SCM tag = scm_from_locale_string(GetCommandTagName(event_trigger_data->tag));

	return scm_list_3(event, parse_tree, tag);
}

Datum call_trigger(FunctionCallInfo fcinfo)
{
	TriggerData	*trigger_data = (TriggerData *)fcinfo->context;
	TriggerEvent event = trigger_data->tg_event;

	SCM proc = find_or_compile_proc(fcinfo->flinfo->fn_oid);
	SCM args = prepare_trigger_arguments(trigger_data);

	SCM scm_result = apply_in_sandbox(proc, args);

	if (scm_result != SCM_BOOL_F && TRIGGER_FIRED_FOR_ROW(event)) {

		TupleDesc tuple_desc = TRIGGER_FIRED_BY_UPDATE(event)
			? trigger_data->tg_newslot->tts_tupleDescriptor
			: trigger_data->tg_trigslot->tts_tupleDescriptor;

		return PointerGetDatum(scm_record_to_heap_tuple(scm_result, tuple_desc));
	}

	return PointerGetDatum(NULL);
}

SCM find_or_compile_proc(Oid func_oid)
{
	HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

	SCM proc = find_cached_proc(proc_tuple);

	if (proc == SCM_BOOL_F)
		proc = compile_and_cache_proc(proc_tuple);

	ReleaseSysCache(proc_tuple);

	return proc;
}

SCM find_cached_proc(HeapTuple proc_tuple)
{
	Form_pg_proc proc = (Form_pg_proc)GETSTRUCT(proc_tuple);

	Oid func_oid = proc->oid;
	SCM func = scm_from_int(func_oid);

	Oid owner_oid = proc->proowner;
	SCM owner = scm_from_int(owner_oid);

	SCM src_hash = hash_proc_source(proc_tuple);

	SCM entry, old_owner;

	// On changes to the evaluation environment, the function cache
	// for this role will be cleared.
	find_or_create_module_for_role(owner_oid);

	entry = scm_hash_ref(func_cache, func, SCM_BOOL_F);

	if (entry == SCM_BOOL_F)
		return SCM_BOOL_F;

	old_owner = scm_cadr(entry);

	if (old_owner != owner) {
		// An ALTER FUNCTION ... OWNER TO ... statement was executed.
		role_remove_func_oid(old_owner, func);
		scm_hash_remove_x(func_cache, func);
		return SCM_BOOL_F;
	}

	if (src_hash != scm_cddr(entry)) {
		// An ALTER FUNCTION statement was executed, changing the
		// function's source.
		scm_hash_remove_x(func_cache, func);
		return SCM_BOOL_F;
	}

	return scm_car(entry);
}

void role_remove_func_oid(SCM old_owner, SCM func)
{
	static SCM role_remove_func_oid_proc = SCM_BOOL_F;

	if (role_remove_func_oid_proc == SCM_BOOL_F)
		role_remove_func_oid_proc = eval_string_in_base_module("role-remove-func-oid!");

	call_2(role_remove_func_oid_proc, old_owner, func);
}

SCM eval_string_in_base_module(const char *text)
{
	return scm_internal_catch(
		SCM_BOOL_T,
		(scm_t_catch_body)base_module_evaluator,
		(void *)scm_from_locale_string(text),
		base_module_evaluator_error_handler,
		(void *)text);
}

SCM base_module_evaluator_error_handler(void *data, SCM key, SCM args)
{
	elog(ERROR, "internal error: failed to evaluate \"%s\" in plg3 base module: %s %s",
	     (const char *)data, scm_to_string(key), scm_to_string(args));
}

SCM base_module_evaluator(void *data)
{
	return scm_eval_string_in_module((SCM)data, plg3_base_module);
}

SCM prepare_trigger_arguments(TriggerData *trigger_data)
{
	Relation rel = trigger_data->tg_relation;
	TriggerEvent event = trigger_data->tg_event;
	TupleDesc tuple_desc = CreateTupleDescCopy(trigger_data->tg_trigslot->tts_tupleDescriptor);
	TupleDesc new_tuple_desc = TRIGGER_FIRED_BY_UPDATE(event) ? CreateTupleDescCopy(trigger_data->tg_newslot->tts_tupleDescriptor) : NULL;

	SCM new = SCM_EOL;
	SCM old = SCM_EOL;
	SCM tg_name = scm_from_locale_string(trigger_data->tg_trigger->tgname);
	SCM tg_when = TRIGGER_FIRED_BEFORE(event) ? before_symbol : after_symbol;
	SCM tg_level = TRIGGER_FIRED_FOR_ROW(event) ? row_symbol : statement_symbol;
	SCM tg_op = SCM_EOL;
	SCM tg_relid = scm_from_int(RelationGetRelid(rel));
	SCM tg_relname = scm_from_locale_string(RelationGetRelationName(rel));
	SCM tg_table_name = scm_from_locale_string(RelationGetRelationName(rel));
	SCM tg_table_schema = scm_from_locale_string(get_namespace_name(RelationGetNamespace(rel)));
	SCM tg_argv = scm_c_make_vector(trigger_data->tg_trigger->tgnargs, SCM_BOOL_F);

	SCM result;

	for (int i = 0; i < trigger_data->tg_trigger->tgnargs; i++)
		scm_c_vector_set_x(
			tg_argv, i, scm_from_locale_string(trigger_data->tg_trigger->tgargs[i]));

	tg_op =
		TRIGGER_FIRED_BY_INSERT(event)     ? insert_symbol
		: TRIGGER_FIRED_BY_DELETE(event)   ? delete_symbol
		: TRIGGER_FIRED_BY_UPDATE(event)   ? update_symbol
		: TRIGGER_FIRED_BY_TRUNCATE(event) ? truncate_symbol
		: SCM_BOOL_F;

	if (TRIGGER_FIRED_BY_INSERT(event))
		new = heap_tuple_to_scm(trigger_data->tg_trigtuple, tuple_desc);

	else if (TRIGGER_FIRED_BY_DELETE(event))
		old = heap_tuple_to_scm(trigger_data->tg_trigtuple, tuple_desc);

	else if (TRIGGER_FIRED_BY_UPDATE(event)) {
		new = heap_tuple_to_scm(trigger_data->tg_newtuple, new_tuple_desc);
		old = heap_tuple_to_scm(trigger_data->tg_trigtuple, tuple_desc);
	}

	// scm_list_n crashed with an equivalent usage
	result = scm_cons(tg_argv, SCM_EOL);
	result = scm_cons(tg_table_schema, result);
	result = scm_cons(tg_table_name, result);
	result = scm_cons(tg_relname, result);
	result = scm_cons(tg_relid, result);
	result = scm_cons(tg_op, result);
	result = scm_cons(tg_level, result);
	result = scm_cons(tg_when, result);
	result = scm_cons(tg_name, result);
	result = scm_cons(old, result);
	result = scm_cons(new, result);

	return result;
}

SCM heap_tuple_to_scm(HeapTuple heap_tuple, TupleDesc tuple_desc)
{
	return datum_heap_tuple_to_scm(HeapTupleGetDatum(heap_tuple), tuple_desc);
}

HeapTuple scm_record_to_heap_tuple(SCM x, TupleDesc tuple_desc)
{
	HeapTuple result;

	Datum *ret_values;
	bool *ret_is_null;

	SCM attrs = call_1(record_attrs_proc, x);

	ret_values = (Datum *) palloc0(sizeof(Datum) * tuple_desc->natts);
	ret_is_null = (bool *) palloc0(sizeof(bool) * tuple_desc->natts);

	for (int i = 0; i < tuple_desc->natts; ++i) {

		Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
		SCM v = scm_c_vector_ref(attrs, i);

		if (v == SCM_EOL)
			ret_is_null[i] = true;
		else
			ret_values[i] = scm_to_datum(v, attr->atttypid);
	}

	result = heap_form_tuple(tuple_desc, ret_values, ret_is_null);

	pfree(ret_values);
	pfree(ret_is_null);

	return result;
}

Datum call_ordinary(FunctionCallInfo fcinfo)
{
	Oid func_oid = fcinfo->flinfo->fn_oid;

	SCM proc = find_or_compile_proc(func_oid);
	SCM args = prepare_ordinary_arguments(fcinfo);

	char prior_volatility = set_current_volatility(func_volatile(func_oid));

	SCM scm_result = apply_in_sandbox(proc, args);

	HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

	Datum result = convert_result_to_datum(scm_result, proc_tuple, fcinfo);

	set_current_volatility(prior_volatility);

	ReleaseSysCache(proc_tuple);

	return result;
}

static char current_volatility = 'v';

char set_current_volatility(char v)
{
	char prior_volatility = current_volatility;
	current_volatility = v;
	return prior_volatility;
}

SCM apply_in_sandbox(SCM proc, SCM args)
{
	CallLimits limits;
	SCM time_limit, allocation_limit;

	get_call_limits(&limits);

	time_limit = scm_from_double(limits.time_seconds);
	allocation_limit = scm_from_int64(limits.allocation_bytes);

	return apply_with_limits(proc, args, time_limit, allocation_limit);
}

SCM apply_with_limits(SCM proc, SCM args, SCM time_limit, SCM allocation_limit)
{
	return apply_0_with_handlers(
		apply_with_limits_proc,
		scm_list_4(proc, args, time_limit, allocation_limit),
		9);
}

SCM eval_with_limits(SCM expr, SCM module, SCM time_limit, SCM allocation_limit)
{
	return apply_0_with_handlers(
		eval_with_limits_proc,
		scm_list_4(expr, module, time_limit, allocation_limit),
		8);
}

typedef struct {
	SCM proc;
	SCM args;
	int internal_frame_count;
	// Used to communicate between the pre-unwind handler, which
	// captures the backtrace, and the error handler, which displays
	// it.
	SCM backtrace;
} ApplyWithHandlersData;

SCM apply_0_with_handlers(SCM proc, SCM args, int internal_backtrace_frame_count)
{
	ApplyWithHandlersData data = {
		proc,
		args,
		internal_backtrace_frame_count,
		SCM_BOOL_F
	};

    return scm_c_catch(
        SCM_BOOL_T,
        apply_0_with_handlers_inner,
        (void *)&data,
        apply_0_with_handlers_error_handler, &data,
        apply_0_with_handlers_pre_unwind_handler, &data);
}

SCM apply_0_with_handlers_inner(void *data)
{
	ApplyWithHandlersData *d = (ApplyWithHandlersData *)data;

	return scm_apply_0(d->proc, d->args);
}

SCM apply_0_with_handlers_error_handler(void *data, SCM key, SCM args)
{
	ApplyWithHandlersData *d = (ApplyWithHandlersData *)data;
	SCM backtrace = d->backtrace;

	if (backtrace == SCM_BOOL_F)
		elog(ERROR, "%s: %s", scm_to_string(key), scm_to_string(args));
	else
		elog(ERROR, "%s: %s\n%s", scm_to_string(key), scm_to_string(args),
		     scm_string_to_pstr(backtrace));
}

SCM apply_0_with_handlers_pre_unwind_handler(void *data, SCM key, SCM args)
{
	ApplyWithHandlersData *d = (ApplyWithHandlersData *)data;

	// The bottom of the stack contains frames for calls of protective
	// procedures: with-exception-handler, call-with-time-limit, etc.
	// Omit these from any backtrace shown to the user.

	SCM stack = scm_make_stack(SCM_BOOL_T, SCM_EOL);
	int frame_count = scm_to_int(scm_stack_length(stack)) - d->internal_frame_count;

	if (frame_count > 0) {

		SCM port = scm_open_output_string();

		scm_display_backtrace(stack, port, SCM_INUM1, scm_from_int(frame_count));

		d->backtrace = scm_get_output_string(port);
		scm_close_port(port);
	}

	return SCM_BOOL_F;
}

CallLimits *get_call_limits(CallLimits *limits)
{
	const char *command =
		"select time, allocation from plguile3.call_limit where role_id in (0, $1) order by role_id";

	Oid arg_type = OIDOID;
	Datum role_datum = ObjectIdGetDatum(GetUserId());

	int ret;

	Oid prev_user_id;
	int prev_sec_con;

	ret = SPI_connect();

	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "spi_connect_error: %d", ret);

	GetUserIdAndSecContext(&prev_user_id, &prev_sec_con);
	SetUserIdAndSecContext(get_language_owner(), SECURITY_LOCAL_USERID_CHANGE);

	ret = SPI_execute_with_args(command, 1, &arg_type, &role_datum, NULL, true, 2);

	SetUserIdAndSecContext(prev_user_id, prev_sec_con);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "spi_select_error: %d", ret);

	// Defaults to effectively unlimited.
	limits->time_seconds     = 1e9;   // almost 32 years
	limits->allocation_bytes = 1L<<50; // one exabyte

	if (SPI_tuptable && SPI_processed) {

		TupleDesc tuple_desc = SPI_tuptable->tupdesc;

		for (long row = 0; row < SPI_tuptable->numvals; row++) {

			HeapTuple tuple = SPI_tuptable->vals[row];
			bool is_null;
			Datum time_limit, allocation_limit;

			time_limit = SPI_getbinval(tuple, tuple_desc, 1, &is_null);

			if (!is_null)
				limits->time_seconds = DatumGetFloat4(time_limit);

			allocation_limit = SPI_getbinval(tuple, tuple_desc, 2, &is_null);

			if (!is_null)
				limits->allocation_bytes = DatumGetInt64(allocation_limit);
		}
	}

	SPI_finish();

	return limits;
}

SCM prepare_ordinary_arguments(FunctionCallInfo fcinfo)
{
	SCM arg_list = SCM_EOL;

	for (int i = PG_NARGS()-1; i >= 0; i--) {

		Datum arg = PG_GETARG_DATUM(i);
		Oid arg_type = get_fn_expr_argtype(fcinfo->flinfo, i);

		SCM scm_arg = datum_to_scm(arg, arg_type);

		arg_list = scm_cons(scm_arg, arg_list);
	}

	return arg_list;
}

Datum convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo)
{
	TypeFuncClass typefunc_class;
	Oid rettype_oid;
	TupleDesc tuple_desc;

	typefunc_class = get_call_result_type(fcinfo, &rettype_oid, &tuple_desc);

	if (is_set_returning(proc_tuple)) {

		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

		switch (typefunc_class) {

		case TYPEFUNC_SCALAR:
			return scm_to_setof_datum(result, rettype_oid, per_query_ctx, rsinfo);

		case TYPEFUNC_COMPOSITE:
			return scm_to_setof_composite_datum(result, tuple_desc, per_query_ctx, rsinfo);

		case TYPEFUNC_RECORD:   /* indeterminate rowtype result      */
			return scm_to_setof_record_datum(result, per_query_ctx, rsinfo);

		case TYPEFUNC_COMPOSITE_DOMAIN: /* domain over determinable rowtype result */
		case TYPEFUNC_OTHER:   /* bogus type, eg pseudotype      */
			elog(ERROR, "convert_result_to_datum: setof not implemented");
			return (Datum) 0;

		default:
			elog(ERROR, "convert_result_to_datum: unknown TypeFuncClass value: %d", typefunc_class);
			return (Datum) 0;
		}
	}

	switch (typefunc_class) {

	case TYPEFUNC_SCALAR:
		return scm_to_datum(result, rettype_oid);

	case TYPEFUNC_COMPOSITE:
		return scm_to_composite_datum(result, tuple_desc);

	case TYPEFUNC_RECORD:   /* indeterminate rowtype result      */
		return scm_to_datum_record(result, rettype_oid);

	case TYPEFUNC_COMPOSITE_DOMAIN: /* domain over determinable rowtype result */
	case TYPEFUNC_OTHER:   /* bogus type, eg pseudotype      */
		elog(ERROR, "convert_result_to_datum: not implemented");
		return (Datum) 0;

	default:
		elog(ERROR, "convert_result_to_datum: unknown TypeFuncClass value: %d", typefunc_class);
		return (Datum) 0;
	}
}

bool is_set_returning(HeapTuple proc_tuple)
{
	Form_pg_proc proc;

	proc = (Form_pg_proc)GETSTRUCT(proc_tuple);
	return proc->proretset;
}

bool is_trigger_handler(HeapTuple proc_tuple)
{
	Form_pg_proc proc;

	proc = (Form_pg_proc)GETSTRUCT(proc_tuple);
	return proc->prorettype == TRIGGEROID;
}

bool is_event_trigger_handler(HeapTuple proc_tuple)
{
	Form_pg_proc proc;

	proc = (Form_pg_proc)GETSTRUCT(proc_tuple);
	return proc->prorettype == EVENT_TRIGGEROID;
}

Datum scm_to_composite_datum(SCM result, TupleDesc tuple_desc)
{
	HeapTuple ret_heap_tuple;

	Datum *ret_values;
	bool *ret_is_null;

	Datum result_datum;

	if (scm_c_nvalues(result) != tuple_desc->natts)
		elog(ERROR, "scm_to_datum: num values %zu does not equal num out params %d", scm_c_nvalues(result), tuple_desc->natts);

	ret_values = (Datum *) palloc0(sizeof(Datum) * tuple_desc->natts);
	ret_is_null = (bool *) palloc0(sizeof(bool) * tuple_desc->natts);

	for (int i = 0; i < tuple_desc->natts; ++i) {

		Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
		SCM v = scm_c_value_ref(result, i);

		// TODO: handle attr->atttypmod
		if (v == SCM_EOL)
			ret_is_null[i] = true;
		else
			ret_values[i] = scm_to_datum(v, attr->atttypid);
	}

	ret_heap_tuple = heap_form_tuple(tuple_desc, ret_values, ret_is_null);
	result_datum = HeapTupleGetDatum(ret_heap_tuple);

	pfree(ret_values);
	pfree(ret_is_null);

	return result_datum;
}

Datum scm_to_setof_composite_datum(SCM x, TupleDesc tuple_desc, MemoryContext ctx, ReturnSetInfo *rsinfo)
{
    SCM rows;
    Datum result;
    Tuplestorestate *tupstore;
    MemoryContext prior_ctx;
    Datum *attrs;
    bool *is_null;

    // Put tuplestore in context of the result, not the context of the function call. It will
    // otherwise be reclaimed and cause a crash when Postgres attempts to use the result.
    prior_ctx = MemoryContextSwitchTo(ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);

    if (is_table(x))
        rows = call_1(table_rows_proc, x);
    else
        rows = x;

    attrs = (Datum *) palloc(sizeof(Datum) * tuple_desc->natts);
    is_null = (bool *) palloc(sizeof(bool) * tuple_desc->natts);

    while (rows != SCM_EOL) {
        SCM item = scm_car(rows);

        if (is_record(item))
	        item = call_1(record_attrs_proc, item);

        if (!scm_is_vector(item))
	        elog(ERROR, "scm_to_setof_composite_datum: vector expected, not %s", scm_to_string(item));

        if (scm_c_vector_length(item) != tuple_desc->natts)
	        elog(ERROR, "scm_to_setof_composite_datum: num values %zu does not equal record size %d", scm_c_vector_length(item), tuple_desc->natts);

        for (int i = 0; i < tuple_desc->natts; ++i) {

	        Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
	        SCM v = scm_c_vector_ref(item, i);

	        // TODO: handle attr->atttypmod
	        if (v == SCM_EOL) {
		        is_null[i] = true;
		        attrs[i] = PointerGetDatum(NULL);
	        }
	        else {
		        is_null[i] = false;
		        attrs[i] = scm_to_datum(v, attr->atttypid);
	        }
        }

        // Put the row into the tuplestore
        tuplestore_putvalues(tupstore, tuple_desc, attrs, is_null);

        rows = scm_cdr(rows);
    }

    pfree(attrs);
    pfree(is_null);

    // Prepare for tuplestore consumption (optional)
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    // Create a result tuplestore to be returned
    result = PointerGetDatum(tupstore);

    MemoryContextSwitchTo(prior_ctx);

    return result;
}

Datum scm_to_setof_record_datum(SCM x, MemoryContext ctx, ReturnSetInfo *rsinfo)
{
    SCM rows;
    SCM default_types = SCM_BOOL_F;
    SCM prior_item_types = SCM_BOOL_F;
    Datum result;
	TupleDesc default_tuple_desc = NULL;
	TupleDesc prior_item_tuple_desc = NULL;
    Tuplestorestate *tupstore;
    MemoryContext prior_ctx;

    // Put tuplestore in context of the result, not the context of the function call. It will
    // otherwise be reclaimed and cause a crash when Postgres attempts to use the result.
    prior_ctx = MemoryContextSwitchTo(ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);

    if (!is_table(x))
        rows = x;

    else {
	    default_types = call_1(table_types_proc, x);
        rows = call_1(table_rows_proc, x);
        default_tuple_desc = table_tuple_desc(x);
    }

    while (rows != SCM_EOL) {
	    SCM rec_attrs;
        SCM item = scm_car(rows);
        SCM types = SCM_BOOL_F;
        TupleDesc tuple_desc;
        Datum *attrs;
        bool *is_null;

        if (is_record(item)) {
	        rec_attrs = call_1(record_attrs_proc, item);
	        types = call_1(record_types_proc, item);

	        if (scm_is_eq(types, default_types))
		        tuple_desc = default_tuple_desc;

	        else if (scm_is_eq(types, prior_item_types))
		        tuple_desc = prior_item_tuple_desc;

	        else {
		        tuple_desc = record_tuple_desc(item);
		        prior_item_types = types;
		        prior_item_tuple_desc = tuple_desc;
	        }
        }
        else {
	        rec_attrs = item;
	        types = default_types;
	        tuple_desc = default_tuple_desc;
        }

        if (types == SCM_BOOL_F)
	        elog(ERROR, "scm_to_setof_record_datum: unable to determine types for item %s", scm_to_string(item));

        if (!scm_is_vector(rec_attrs))
	        elog(ERROR, "scm_to_setof_record_datum: vector expected, not %s", scm_to_string(rec_attrs));

        if (scm_c_vector_length(rec_attrs) != tuple_desc->natts)
	        elog(ERROR, "scm_to_setof_record_datum: num attributes %zu does not equal record size %d", scm_c_vector_length(rec_attrs), tuple_desc->natts);

        attrs = (Datum *) palloc(sizeof(Datum) * tuple_desc->natts);
        is_null = (bool *) palloc(sizeof(bool) * tuple_desc->natts);

        for (int i = 0; i < tuple_desc->natts; ++i) {

	        Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
	        SCM v = scm_c_vector_ref(rec_attrs, i);

	        // TODO: handle attr->atttypmod
	        if (v == SCM_EOL) {
		        is_null[i] = true;
		        attrs[i] = PointerGetDatum(NULL);
	        }
	        else {
		        is_null[i] = false;
		        attrs[i] = scm_to_datum(v, attr->atttypid);
	        }
        }

        // Put the row into the tuplestore
        tuplestore_putvalues(tupstore, tuple_desc, attrs, is_null);

        pfree(attrs);
        pfree(is_null);

        rows = scm_cdr(rows);
    }

    // Prepare for tuplestore consumption (optional)
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    // Create a result tuplestore to be returned
    result = PointerGetDatum(tupstore);

    MemoryContextSwitchTo(prior_ctx);

    return result;
}

TupleDesc record_tuple_desc(SCM x)
{
	SCM attr_names = call_1(record_attr_names_proc, x);
	SCM type_desc_list = call_1(record_types_proc, x);

	return build_tuple_desc(attr_names, type_desc_list);
}

TupleDesc table_tuple_desc(SCM x)
{
	SCM attr_names = call_1(table_attr_names_proc, x);
	SCM type_desc_list = call_1(table_types_proc, x);

	return build_tuple_desc(attr_names, type_desc_list);
}

TupleDesc build_tuple_desc(SCM attr_names, SCM type_desc_list)
{
	SCM head;
	long len = scm_ilength(type_desc_list);
	TupleDesc tupdesc;

	if (len <= 0)
		elog(ERROR, "Type description must be a list of symbols: %s", scm_to_string(type_desc_list));

	head = type_desc_list;

	while (head != SCM_EOL) {
		SCM type_name = scm_car(head);
		if (!scm_is_symbol(type_name))
			elog(ERROR, "Type description must be a list of symbols: %s", scm_to_string(type_desc_list));
		head = scm_cdr(head);
	}

	tupdesc = CreateTemplateTupleDesc(len);
	head = type_desc_list;

	for (long i = 0; i < len; i++) {
		SCM type_name = scm_car(head);
		char *type_name_cstr = scm_to_locale_string(scm_symbol_to_string(type_name));
		Oid typeOid = TypenameGetTypid(type_name_cstr);

		SCM attr_name;
		char *attr_name_cstr;

		if (attr_names == SCM_EOL || attr_names == SCM_BOOL_F)
			attr_name_cstr = type_name_cstr;

		else {
			attr_name = scm_car(attr_names);
			attr_names = scm_cdr(attr_names);
			attr_name_cstr = scm_to_locale_string(scm_symbol_to_string(attr_name));
		}

		TupleDescInitEntry(tupdesc, (AttrNumber)i+1, attr_name_cstr, typeOid, -1, 0);

		head = scm_cdr(head);
		free(type_name_cstr);
	}

	// Bless the TupleDesc so that it's valid for the rest of the transaction
	BlessTupleDesc(tupdesc);

	return tupdesc;
}

SCM make_boxed_datum(Oid type_oid, Datum x)
{
	return call_2(make_boxed_datum_proc, scm_from_int32(type_oid), scm_from_int64(x));
}

Datum plg3_call_inline(PG_FUNCTION_ARGS)
{
	InlineCodeBlock *codeblock = (InlineCodeBlock *) DatumGetPointer(PG_GETARG_DATUM(0));
	char *source_text = codeblock->source_text;

	CallLimits limits;
	SCM time_limit, allocation_limit;

	SCM module = find_or_create_module_for_role(GetUserId());
	SCM port = scm_open_input_string(scm_from_locale_string(source_text));
	SCM x;

	get_call_limits(&limits);

	time_limit = scm_from_double(limits.time_seconds);
	allocation_limit = scm_from_int64(limits.allocation_bytes);

	while (scm_eof_object_p(x = scm_read(port)) == SCM_BOOL_F)
		eval_with_limits(x, module, time_limit, allocation_limit);

	return (Datum)0;
}

SCM find_or_create_module_for_role(Oid role_oid)
{
	int64 default_preamble_id, preamble_id, role_preamble_id;
	SCM module;

	fetch_preamble_ids(role_oid, &default_preamble_id, &role_preamble_id);

	preamble_id = role_preamble_id ? role_preamble_id : default_preamble_id;

	if (!preamble_id)
		flush_module_cache_for_role(role_oid);
	else {
		int64 cached_preamble_id = get_cached_preamble_id(role_oid);
		if (preamble_id != cached_preamble_id)
			flush_module_cache_for_role(role_oid);
	}

	module = get_cached_module(role_oid);

	if (module == SCM_BOOL_F) {
		module = make_sandbox_module(preamble_id);
		cache_module(role_oid, preamble_id, module);
	}

	return module;
}

void fetch_preamble_ids(Oid role_oid, int64 *default_preamble_id, int64 *role_preamble_id)
{
	const char *command =
		"select role_id, preamble_id from plguile3.eval_env where role_id in (0, $1)";

	int ret;
	Oid arg_type = OIDOID;
	Datum role_datum = ObjectIdGetDatum(role_oid);
	Oid prev_user_id;
	int prev_sec_con;

	ret = SPI_connect();

	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "spi_connect_error: %d", ret);

	GetUserIdAndSecContext(&prev_user_id, &prev_sec_con);
	SetUserIdAndSecContext(get_language_owner(), SECURITY_LOCAL_USERID_CHANGE);

	ret = SPI_execute_with_args(command, 1, &arg_type, &role_datum, NULL, true, 2);

	SetUserIdAndSecContext(prev_user_id, prev_sec_con);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "spi_select_error: %d", ret);

	*default_preamble_id = *role_preamble_id = 0;

	if (SPI_tuptable != NULL) {

		TupleDesc tuple_desc = SPI_tuptable->tupdesc;

		for (long row = 0; row < SPI_tuptable->numvals; row++) {

			HeapTuple tuple = SPI_tuptable->vals[row];

			bool is_null;
			int64 preamble_id = DatumGetInt64(SPI_getbinval(tuple, tuple_desc, 2, &is_null));

			if (DatumGetObjectId(SPI_getbinval(tuple, tuple_desc, 1, &is_null)))
				*role_preamble_id = preamble_id;
			else
				*default_preamble_id = preamble_id;
		}
	}

	SPI_finish();
}

void flush_module_cache_for_role(Oid role_oid)
{
	SCM role = scm_from_int(role_oid);
	SCM old_func_cache = func_cache;

	scm_hash_remove_x(module_cache, role);

	func_cache = call_2(
		flush_function_cache_proc, func_cache, call_1(role_to_func_oids_proc, role));

	scm_gc_protect_object(func_cache);
	scm_gc_unprotect_object(old_func_cache);
}

int64 get_cached_preamble_id(Oid role_oid)
{
	SCM role = scm_from_int(role_oid);
	SCM obj = scm_hash_ref(module_cache, role, SCM_BOOL_F);

	if (obj == SCM_BOOL_F)
		return (int64)0;

	return scm_to_int64(scm_car(obj));
}

SCM get_cached_module(Oid role_oid)
{
	SCM role = scm_from_int(role_oid);
	SCM obj = scm_hash_ref(module_cache, role, SCM_BOOL_F);

	if (obj == SCM_BOOL_F)
		return SCM_BOOL_F;

	return scm_cdr(obj);
}

SCM make_sandbox_module(int64 preamble_id)
{
	SCM module = call_1(make_sandbox_module_proc, trusted_bindings);

	if (preamble_id) {

		const char *command = "select src from plguile3.preamble where id = $1";

		int ret;
		Oid arg_type = INT8OID;
		Datum preamble = Int64GetDatum(preamble_id);

		Oid prev_user_id;
		int prev_sec_con;

		ret = SPI_connect();

		if (ret != SPI_OK_CONNECT)
			elog(ERROR, "spi_connect_error: %d", ret);

		GetUserIdAndSecContext(&prev_user_id, &prev_sec_con);
		SetUserIdAndSecContext(get_language_owner(), SECURITY_LOCAL_USERID_CHANGE);

		ret = SPI_execute_with_args(command, 1, &arg_type, &preamble, NULL, true, 1);

		SetUserIdAndSecContext(prev_user_id, prev_sec_con);

		if (ret != SPI_OK_SELECT)
			elog(ERROR, "spi_select_error: %d", ret);

		if (SPI_tuptable && SPI_processed) {

			TupleDesc tuple_desc = SPI_tuptable->tupdesc;
			HeapTuple tuple = SPI_tuptable->vals[0];
			bool is_null;
			char *src = TextDatumGetCString(SPI_getbinval(tuple, tuple_desc, 1, &is_null));

			if (!is_null) {

				SCM port = scm_open_input_string(scm_from_locale_string(src));
				SCM x;

				while (scm_eof_object_p(x = scm_read(port)) == SCM_BOOL_F)
					untrusted_eval(x, module);
			}
		}

		SPI_finish();
	}

	return module;
}

void cache_module(Oid role_oid, int64 preamble_id, SCM module)
{
	SCM role = scm_from_int(role_oid);
	SCM preamble = scm_from_int64(preamble_id);
	scm_hash_set_x(module_cache, role, scm_cons(preamble, module));
}

Datum plg3_compile(PG_FUNCTION_ARGS)
{
	Oid func_oid = PG_GETARG_OID(0);
	HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

	if (!HeapTupleIsValid(proc_tuple))
		elog(ERROR, "plg3_compile: Failed to fetch function details.");

	compile_and_cache_proc(proc_tuple);

	ReleaseSysCache(proc_tuple);

	return (Datum) 1;
}

SCM compile_and_cache_proc(HeapTuple proc_tuple)
{
	Form_pg_proc pg_proc = (Form_pg_proc)GETSTRUCT(proc_tuple);
	Oid owner_oid = pg_proc->proowner;
	SCM module = find_or_create_module_for_role(owner_oid);
	SCM proc = compile_proc(proc_tuple, module);

	cache_proc(pg_proc->oid, owner_oid, hash_proc_source(proc_tuple), proc);

	return proc;
}

SCM hash_proc_source(HeapTuple proc_tuple)
{
	bool is_null;
	Datum src_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &is_null);
	char *src = TextDatumGetCString(src_datum);
	return scm_from_uint64(hash_bytes_extended((unsigned char *)src, strlen(src), 0));
}

void cache_proc(Oid func_oid, Oid owner_oid, SCM src_hash, SCM proc)
{
	SCM func = scm_from_int(func_oid);
	SCM owner = scm_from_int(owner_oid);

	SCM entry = scm_hash_ref(func_cache, func, SCM_BOOL_F);

	if (entry != SCM_BOOL_F) {

		SCM old_owner = scm_cadr(entry);

		if (owner != old_owner)
			role_remove_func_oid(old_owner, func);
	}

	call_2(role_add_func_oid_proc, owner, func);
	scm_hash_set_x(func_cache, func, scm_cons(proc, scm_cons(owner, src_hash)));
}

SCM compile_proc(HeapTuple proc_tuple, SCM module)
{
	Datum prosrc_datum;
	char *prosrc;
	bool is_null;

	Datum argnames_datum, argmodes_datum;

	ArrayType *argnames_array, *argmodes_array;
	int num_args, num_modes, i;

	char *argmodes;

	StringInfoData buf;
	SCM scm_proc;
	SCM port;

	initStringInfo(&buf);
	appendStringInfo(&buf, "(lambda (");

	if (is_event_trigger_handler(proc_tuple))
		appendStringInfo(&buf, " event parse-tree tag");

	else if (is_trigger_handler(proc_tuple))
		appendStringInfo(&buf, " new old tg-name tg-when tg-level tg-op tg-relid tg-relname tg-table-name tg-table-schema tg-argv");

	else {
		argnames_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargnames, &is_null);

		if (!is_null) {
			argnames_array = DatumGetArrayTypeP(argnames_datum);
			num_args = ARR_DIMS(argnames_array)[0];

			argmodes_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargmodes, &is_null);

			if (is_null) {
				argmodes = NULL;
			}
			else {
				argmodes_array = DatumGetArrayTypeP(argmodes_datum);
				argmodes = (char *) ARR_DATA_PTR(argmodes_array);
				num_modes = ArrayGetNItems(ARR_NDIM(argmodes_array), ARR_DIMS(argmodes_array));

				if (num_modes != num_args) {
					elog(ERROR, "compile_proc: num arg modes %d and num args %d differ", num_modes, num_args);
				}
			}

			for (i = 1; i <= num_args; i++) {
				if (argmodes == NULL || argmodes[i-1] != 'o') {
					Datum name_datum = array_get_element(argnames_datum, 1, &i, -1, -1, false, 'i', &is_null);
					if (!is_null) {
						char *name = TextDatumGetCString(name_datum);
						// TODO check that the parameter name is a proper scheme identifier
						appendStringInfo(&buf, " %s", name);
					}
					else {
						elog(NOTICE, "compile_proc: %d: name_datum is null", i);
					}
				}
			}
		}
	}

	prosrc_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &is_null);
	prosrc = TextDatumGetCString(prosrc_datum);

	if (is_null)
		elog(ERROR, "compile_proc: source datum is null.");

	appendStringInfo(&buf, ")\n%s)", prosrc);

	port = scm_open_input_string(scm_from_locale_string(buf.data));

	scm_proc = untrusted_eval(scm_read(port), module);

	return scm_proc;
}

SCM untrusted_eval(SCM expr, SCM module)
{
	return call_2(untrusted_eval_proc, expr, module);
}

SCM unbox_datum(SCM x)
{
	Oid type_oid = get_boxed_datum_type(x);
	Datum value = get_boxed_datum_value(x);

	return datum_to_scm(value, type_oid);
}

SCM datum_to_scm(Datum datum, Oid type_oid)
{
	bool found;
	TypeConvCacheEntry *entry;
	Oid base_type_oid, element_type_oid;

	entry = (TypeConvCacheEntry *)hash_search(type_cache, &type_oid, HASH_FIND, &found);

	if (found && entry->to_scm)
		return entry->to_scm(datum, type_oid);

	switch (get_typtype(type_oid)) {

	case TYPTYPE_COMPOSITE:
		insert_type_cache_entry(type_oid, datum_composite_to_scm, scm_to_datum_record);
		return datum_composite_to_scm(datum, type_oid);

	case TYPTYPE_DOMAIN:
		base_type_oid = getBaseType(type_oid);

		if (base_type_oid == type_oid)
			break;

		entry = (TypeConvCacheEntry *)hash_search(type_cache, &base_type_oid, HASH_FIND, &found);
		if (found && entry->to_scm)
			insert_type_cache_entry(type_oid, entry->to_scm, entry->to_datum);

		return datum_to_scm(datum, base_type_oid);

	case TYPTYPE_ENUM:
		insert_type_cache_entry(type_oid, datum_enum_to_scm, scm_to_datum_enum);
		return datum_enum_to_scm(datum, type_oid);

	case TYPTYPE_MULTIRANGE:
		insert_type_cache_entry(type_oid, datum_multirange_to_scm, scm_to_datum_multirange);
		return datum_multirange_to_scm(datum, type_oid);

	case TYPTYPE_RANGE:
		insert_type_cache_entry(type_oid, datum_range_to_scm, scm_to_datum_range);
		return datum_range_to_scm(datum, type_oid);

	default:
		element_type_oid = get_element_type(type_oid);

		if (OidIsValid(element_type_oid))
			return datum_array_to_scm(datum, element_type_oid);
	}

	elog(NOTICE, "datum_to_scm: conversion function for type OID %u not found", type_oid);
	return make_boxed_datum(type_oid, datum);
}

Datum scm_to_datum(SCM scm, Oid type_oid)
{
	bool found;
	TypeConvCacheEntry *entry;

	Oid base_type_oid, element_type_oid;

	if (is_boxed_datum(scm))
		return convert_boxed_datum_to_datum(scm, type_oid);

	entry = (TypeConvCacheEntry *)hash_search(type_cache, &type_oid, HASH_FIND, &found);

	if (found && entry->to_datum)
		return entry->to_datum(scm, type_oid);

	element_type_oid = get_element_type(type_oid);

	if (OidIsValid(element_type_oid))
		return scm_to_datum_array(scm, element_type_oid);

	// Domain types
	base_type_oid = getBaseType(type_oid);

	if (base_type_oid != type_oid)
		return scm_to_datum(scm, base_type_oid);

	elog(ERROR, "scm_to_datum: conversion function for type OID %u not found", type_oid);
	// Unreachable
	return (Datum)0;
}

Datum scm_to_setof_datum(SCM x, Oid type_oid, MemoryContext ctx, ReturnSetInfo *rsinfo)
{
    SCM rows;
    Datum result;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext prior_ctx;

    // Put tuplestore in context of the result, not the context of the function call. It will
    // otherwise be reclaimed and cause a crash when Postgres attempts to use the result.
    prior_ctx = MemoryContextSwitchTo(ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);

    tupdesc = CreateTemplateTupleDesc(1);
    TupleDescInitEntry(tupdesc, 1, "", type_oid, -1, 0);

    if (is_table(x))
        rows = call_1(table_rows_proc, x);
    else
        rows = x;

    while (rows != SCM_EOL) {
        SCM item = scm_car(rows);
        bool is_null = item == SCM_EOL;
        Datum value;

        if (is_null)
	        value = PointerGetDatum(NULL);
        else
            value = scm_to_datum(item, type_oid);

        // Put the row into the tuplestore
        tuplestore_putvalues(tupstore, tupdesc, &value, &is_null);

        rows = scm_cdr(rows);
    }

    // Prepare for tuplestore consumption (optional)
    tuplestore_donestoring(tupstore);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;

    // Create a result tuplestore to be returned
    result = PointerGetDatum(tupstore);

    MemoryContextSwitchTo(prior_ctx);

    return result;
}

Datum convert_boxed_datum_to_datum(SCM scm, Oid target_type_oid)
{
	Oid source_type_oid = get_boxed_datum_type(scm);
	Datum value = get_boxed_datum_value(scm);

	if (!OidIsValid(target_type_oid) || target_type_oid == source_type_oid)
		return value;

	else {
		// Lookup casting function OID in pg_cast
		Oid cast_func_oid = InvalidOid;
		Relation rel = relation_open(CastRelationId, AccessShareLock);
		SysScanDesc scan;
		ScanKeyData key[2];
		HeapTuple tuple;
		Form_pg_cast cast_form;

		ScanKeyInit(&key[0],
		            Anum_pg_cast_castsource,
		            BTEqualStrategyNumber, F_OIDEQ,
		            ObjectIdGetDatum(source_type_oid));

		ScanKeyInit(&key[1],
		            Anum_pg_cast_casttarget,
		            BTEqualStrategyNumber, F_OIDEQ,
		            ObjectIdGetDatum(target_type_oid));

		scan = systable_beginscan(rel, CastSourceTargetIndexId, true,
		                          NULL, 2, key);

		tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "No cast from type OID %u to %u.", source_type_oid, target_type_oid);

		cast_form = (Form_pg_cast) GETSTRUCT(tuple);
		cast_func_oid = cast_form->castfunc;

		systable_endscan(scan);
		relation_close(rel, AccessShareLock);

		if (!OidIsValid(cast_func_oid))
			elog(ERROR, "No cast from type OID %u to %u.", source_type_oid, target_type_oid);

		return OidFunctionCall1(cast_func_oid, value);
	}
}

SCM datum_enum_to_scm(Datum x, Oid type_oid)
{
	HeapTuple tup;
	SCM type_name, value_name, result;

	tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(x));

	if (!HeapTupleIsValid(tup)) {
		elog(ERROR, "cache lookup failed for enum %lu", ObjectIdGetDatum(x));
	}

	type_name = type_desc_expr(type_oid);
	value_name = scm_from_locale_symbol(NameStr(((Form_pg_enum) GETSTRUCT(tup))->enumlabel));

	result = scm_cons(type_name, value_name);

	ReleaseSysCache(tup);

	return result;
}

Datum scm_to_datum_enum(SCM x, Oid type_oid)
{
	HeapTuple tup;
	SCM target_type_name, type_name, value_name;
	char *value_name_cstr;
	Datum result;

	if (!scm_is_pair(x)) {
		elog(ERROR, "scm_to_datum_enum: a cons of two symbols is expected, not %s", scm_to_string(x));
	}

	type_name = scm_car(x);
	value_name = scm_cdr(x);

	if (!scm_is_symbol(type_name) || ! scm_is_symbol(value_name)) {
		elog(ERROR, "scm_to_datum_enum: a cons of two symbols is expected, not %s", scm_to_string(x));
	}

	target_type_name = type_desc_expr(type_oid);

	if (!scm_is_eq(target_type_name, type_name)) {
		elog(ERROR, "scm_to_datum_enum: enum type mismatch: type %s expected, not %s", scm_to_string(target_type_name), scm_to_string(type_name));
	}

	value_name_cstr = scm_to_locale_string(scm_symbol_to_string(value_name));

	tup = SearchSysCache2(ENUMTYPOIDNAME,
	                      ObjectIdGetDatum(type_oid),
	                      CStringGetDatum(value_name_cstr));

	free(value_name_cstr);

	if (!HeapTupleIsValid(tup)) {
		elog(ERROR, "Could not find enum value for string: %s", scm_to_string(value_name));
	}

	result = ((Form_pg_enum) GETSTRUCT(tup))->oid;

	ReleaseSysCache(tup);

	return result;
}

SCM datum_array_to_scm(Datum x, Oid element_type_oid)
{
	ArrayType *array;
	Datum *elems;
	bool *nulls;
	int nelems;
	int16 elmlen;
	bool elmbyval;
	char elmalign;
	SCM result;

	// Convert Datum to ArrayType pointer after detoasting if required
	array = DatumGetArrayTypeP(PG_DETOAST_DATUM(x));

	// Extract the type information
	get_typlenbyvalalign(element_type_oid, &elmlen, &elmbyval, &elmalign);

	// Deconstruct the array into Datums and nulls flags
	deconstruct_array(array, element_type_oid, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

	result = scm_c_make_vector(nelems, SCM_EOL);

	for (int i = 0; i < nelems; i++)
		if (!nulls[i])
			scm_c_vector_set_x(result, i, datum_to_scm(elems[i], element_type_oid));

	pfree(elems);
	pfree(nulls);

	return result;
}

Datum scm_to_datum_array(SCM elements, Oid element_type_oid)
{
	size_t nelems = scm_c_vector_length(elements);
	Datum *elems = palloc(nelems * sizeof(Datum));
	bool *nulls = palloc0(nelems * sizeof(bool));

	int dims[1] = {nelems};
	int lbs[1] = {1}; // Lower bounds of array

	int16 elmlen;
	bool elmbyval;
	char elmalign;

	ArrayType *array;

	// Get type info for the element type.
	get_typlenbyvalalign(element_type_oid, &elmlen, &elmbyval, &elmalign);

	// Convert SCM values to Datums.
	for (size_t i = 0; i < nelems; i++) {
		SCM scm_value = scm_c_vector_ref(elements, i);
		if (scm_value == SCM_EOL)
			nulls[i] = true;
		else
			elems[i] = scm_to_datum(scm_value, element_type_oid);
	}

	// Construct a one-dimensional Postgres array.
	array = construct_md_array(elems, nulls, 1, dims, lbs,
	                           element_type_oid, elmlen, elmbyval, elmalign);

	pfree(elems);
	pfree(nulls);

	return PointerGetDatum(array);
}

SCM datum_composite_to_scm(Datum x, Oid ignored)
{
	HeapTupleHeader rec = DatumGetHeapTupleHeader(x);
	Oid type_oid = HeapTupleHeaderGetTypeId(rec);
	TupleDesc tuple_desc = lookup_rowtype_tupdesc(type_oid, HeapTupleHeaderGetTypMod(rec));

	return datum_heap_tuple_to_scm(x, tuple_desc);
}

SCM datum_heap_tuple_to_scm(Datum x, TupleDesc tuple_desc)
{
	SCM attr_names, attr_names_hash, type_names, values;

	HeapTupleHeader rec = DatumGetHeapTupleHeader(x);

	int natts = tuple_desc->natts;
	Datum *attrs = palloc(natts * sizeof(Datum));
	bool *is_null = palloc(natts * sizeof(bool));

	HeapTupleData tuple_data;

	ItemPointerSetInvalid(&(tuple_data.t_self));
	tuple_data.t_len = HeapTupleHeaderGetDatumLength(rec);
	tuple_data.t_tableOid = InvalidOid;
	tuple_data.t_data = rec;

	heap_deform_tuple(&tuple_data, tuple_desc, attrs, is_null);

	attr_names_hash = scm_c_make_hash_table(natts);
	attr_names = SCM_EOL;
	type_names = SCM_EOL;
	values = scm_c_make_vector(natts, SCM_EOL);

	for (int i = natts-1; i >= 0; i--) {
		Oid att_type_oid = tuple_desc->attrs[i].atttypid;
		SCM symbol = scm_from_locale_symbol(NameStr(tuple_desc->attrs[i].attname));

		attr_names = scm_cons(symbol, attr_names);
		type_names = scm_cons(type_desc_expr(att_type_oid), type_names);
		scm_hash_set_x(attr_names_hash, symbol, scm_from_int(i));

		if (!is_null[i])
			scm_c_vector_set_x(values, i, datum_to_scm(attrs[i], att_type_oid));
	}

	pfree(attrs);
	pfree(is_null);
	ReleaseTupleDesc(tuple_desc);

	return call_4(make_record_proc, type_names, values, attr_names, attr_names_hash);
}

SCM type_desc_expr(Oid type_oid)
{
	HeapTuple type_tuple;
	Form_pg_type type_form;
	char *type_name;

	// Look up the type by its OID in the system cache
	type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	if (!HeapTupleIsValid(type_tuple))
	{
		elog(ERROR, "Cache lookup failed for type %u", type_oid);
	}

	// Extract the type information from the tuple
	type_form = (Form_pg_type) GETSTRUCT(type_tuple);

	// Get the name of the type
	type_name = NameStr(type_form->typname);

	// Don't forget to release the tuple when done
	ReleaseSysCache(type_tuple);

	return scm_from_locale_symbol(type_name);
}


SCM datum_int2_to_scm(Datum x, Oid type_oid)
{
	return scm_from_short(DatumGetInt16(x));
}

Datum scm_to_datum_int2(SCM x, Oid type_oid)
{
	if (!is_int2(x)) {
		if (!scm_is_integer(x))
			elog(ERROR, "wrong type, integer expected: %s", scm_to_string(x));

		elog(ERROR, "out of range for int2: %s", scm_to_string(x));
	}

	return Int16GetDatum(scm_to_short(x));
}

SCM datum_int4_to_scm(Datum x, Oid type_oid)
{
	return scm_from_int32(DatumGetInt32(x));
}

Datum scm_to_datum_int4(SCM x, Oid type_oid)
{
	if (!is_int4(x)) {
		if (!scm_is_integer(x))
			elog(ERROR, "wrong type, integer expected: %s", scm_to_string(x));

		elog(ERROR, "out of range for int4: %s", scm_to_string(x));
	}

	return Int32GetDatum(scm_to_int32(x));
}

SCM datum_int8_to_scm(Datum x, Oid type_oid)
{
	return scm_from_int64(DatumGetInt64(x));
}

Datum scm_to_datum_int8(SCM x, Oid type_oid)
{
	if (!is_int8(x)) {
		if (!scm_is_integer(x))
			elog(ERROR, "wrong type, integer expected: %s", scm_to_string(x));

		elog(ERROR, "out of range for int8: %s", scm_to_string(x));
	}

	return Int64GetDatum(scm_to_int64(x));
}

SCM datum_float4_to_scm(Datum x, Oid type_oid)
{
	return scm_from_double((double)DatumGetFloat4(x));
}

Datum scm_to_datum_float4(SCM x, Oid type_oid)
{
	if (scm_is_number(x))
		return Float4GetDatum(scm_to_double(x));

	else if (is_decimal(x))
		return Float4GetDatum(scm_to_double(call_1(decimal_to_inexact_proc, x)));

	else {
		elog(ERROR, "wrong type, number expected: %s", scm_to_string(x));
	}
}

SCM datum_float8_to_scm(Datum x, Oid type_oid)
{
	return scm_from_double(DatumGetFloat8(x));
}

Datum scm_to_datum_float8(SCM x, Oid type_oid)
{
	if (scm_is_number(x))
		return Float8GetDatum(scm_to_double(x));

	else if (is_decimal(x))
		return Float8GetDatum(scm_to_double(call_1(decimal_to_inexact_proc, x)));

	else {
		elog(ERROR, "wrong type, number expected: %s", scm_to_string(x));
	}
}

Datum scm_to_datum_record(SCM result, Oid ignored)
{
	HeapTuple ret_heap_tuple;

	Datum *ret_values;
	bool *ret_is_null;

	Datum result_datum;

	SCM data;
	int len;
	TupleDesc tuple_desc;

	if (!is_record(result))
		elog(ERROR, "wrong type, record expected: %s", scm_to_string(result));

	data = call_1(record_attrs_proc, result);
	len = scm_c_vector_length(data);
	tuple_desc = record_tuple_desc(result);

	ret_values = (Datum *) palloc0(sizeof(Datum) * len);
	ret_is_null = (bool *) palloc0(sizeof(bool) * len);

	for (int i = 0; i < len; ++i) {

		Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
		SCM v = scm_c_vector_ref(data, i);

		if (v == SCM_EOL)
			ret_is_null[i] = true;
		else
			ret_values[i] = scm_to_datum(v, attr->atttypid);
	}

	ret_heap_tuple = heap_form_tuple(tuple_desc, ret_values, ret_is_null);
	result_datum = HeapTupleGetDatum(ret_heap_tuple);

	pfree(ret_values);
	pfree(ret_is_null);

	return result_datum;
}

SCM datum_text_to_scm(Datum x, Oid type_oid)
{
	char *cstr = text_to_cstring(DatumGetTextP(x));
	SCM scm_str = scm_from_locale_string(cstr);

	pfree(cstr);

	return scm_str;
}

Datum scm_to_datum_text(SCM x, Oid type_oid)
{
	char *cstr;
	text *pg_text;
	Datum text_datum;

	if (!scm_is_string(x))
		elog(ERROR, "wrong type, string expected: %s", scm_to_string(x));

	cstr = scm_to_locale_string(x);
	pg_text = cstring_to_text(cstr);
	text_datum = PointerGetDatum(pg_text);

	free(cstr);

	return text_datum;
}

SCM datum_bytea_to_scm(Datum x, Oid type_oid)
{
	bytea *bytea_data = DatumGetByteaP(x);
	char *binary_data = VARDATA(bytea_data);
	int len = VARSIZE(bytea_data) - VARHDRSZ;

	SCM scm_bytevector = scm_c_make_bytevector(len);

	for (int i = 0; i < len; i++) {
		scm_c_bytevector_set_x(scm_bytevector, i, binary_data[i]);
	}

	return scm_bytevector;
}

Datum scm_to_datum_bytea(SCM x, Oid type_oid)
{
	size_t len;
	bytea *result;
	char *data_ptr;

	if (!scm_is_bytevector(x)) {
		elog(ERROR, "bytea result expected, not: %s", scm_to_string(x));
	}

	len = scm_c_bytevector_length(x);
	result = (bytea *) palloc(VARHDRSZ + len);

	SET_VARSIZE(result, VARHDRSZ + len);

	data_ptr = VARDATA(result);

	for (size_t i = 0; i < len; i++) {
		data_ptr[i] = scm_c_bytevector_ref(x, i);
	}

	return PointerGetDatum(result);
}

SCM datum_timestamptz_to_scm(Datum x, Oid type_oid)
{
	TimestampTz timestamp = DatumGetTimestampTz(x);
	struct pg_tm tm;
	fsec_t msec;
	int tz_offset;

	if (timestamp2tm(timestamp, &tz_offset, &tm, &msec, NULL, NULL))
		elog(ERROR, "invalid timestamp datum");

	return call_8(
		make_date_proc,
		scm_from_long(msec * 1000),
		scm_from_int(tm.tm_sec),
		scm_from_int(tm.tm_min),
		scm_from_int(tm.tm_hour),
		scm_from_int(tm.tm_mday),
		scm_from_int(tm.tm_mon),
		scm_from_int(tm.tm_year),
		scm_from_int(tz_offset));
}

Datum scm_to_datum_timestamptz(SCM x, Oid type_oid)
{
	if (!is_date(x))
		elog(ERROR, "wrong type, date expected: %s", scm_to_string(x));

	else {
		// Extract the individual components of the SRFI-19 date object
		SCM nanosecond_scm = call_1(date_nanosecond_proc, x);
		SCM tz_offset_scm  = call_1(date_zone_offset_proc, x);
		SCM year_scm       = call_1(date_year_proc, x);
		SCM month_scm      = call_1(date_month_proc, x);
		SCM day_scm        = call_1(date_day_proc, x);
		SCM hour_scm       = call_1(date_hour_proc, x);
		SCM minute_scm     = call_1(date_minute_proc, x);
		SCM second_scm     = call_1(date_second_proc, x);

		// Convert from Scheme values to C types
		long nanoseconds = scm_to_long(nanosecond_scm);
		int tz_offset    = scm_to_int(tz_offset_scm);
		int year         = scm_to_int(year_scm);
		int month        = scm_to_int(month_scm);
		int day          = scm_to_int(day_scm);
		int hour         = scm_to_int(hour_scm);
		int minute       = scm_to_int(minute_scm);
		int second       = scm_to_int(second_scm);

		// Convert nanoseconds to fractional seconds (microseconds)
		fsec_t msec = nanoseconds / 1000;

		TimestampTz timestamp;
		struct pg_tm tm;

		tm.tm_year = year;
		tm.tm_mon  = month;
		tm.tm_mday = day;
		tm.tm_hour = hour;
		tm.tm_min  = minute;
		tm.tm_sec  = second;

		if (tm2timestamp(&tm, msec, &tz_offset, &timestamp))
			elog(ERROR, "Invalid date components");

		return TimestampTzGetDatum(timestamp);
	}
}

SCM datum_date_to_scm(Datum x, Oid type_oid)
{
	return datum_timestamptz_to_scm(datum_date_to_timestamptz(x), type_oid);
}

Datum scm_to_datum_date(SCM x, Oid type_oid)
{
	if (!is_date(x))
		elog(ERROR, "wrong type, date expected: %s", scm_to_string(x));

	else {
		// Extract the individual components of the SRFI-19 date object
		SCM year_scm  = call_1(date_year_proc, x);
		SCM month_scm = call_1(date_month_proc, x);
		SCM day_scm   = call_1(date_day_proc, x);

		// Convert from Scheme values to C types
		int year  = scm_to_int(year_scm);
		int month = scm_to_int(month_scm);
		int day   = scm_to_int(day_scm);

		return DateADTGetDatum(date2j(year, month, day) - POSTGRES_EPOCH_JDATE);
	}
}

SCM datum_time_to_scm(Datum x, Oid type_oid)
{
	struct pg_tm tm;
	fsec_t msec;

	if (time2tm(DatumGetTimeADT(x), &tm, &msec))
		elog(ERROR, "invalid time datum");

	return call_3(
		make_time_proc,
		time_monotonic_symbol,
		scm_from_long(msec * 1000),
		scm_from_int(tm.tm_sec + tm.tm_min * 60 + tm.tm_hour * 3600));
}

Datum scm_to_datum_time(SCM x, Oid type_oid)
{
	if (!is_time(x))
		elog(ERROR, "wrong type, time expected: %s", scm_to_string(x));

	else {
		int seconds = scm_to_int(call_1(time_second_proc, x));
		int nanoseconds = scm_to_int(call_1(time_nanosecond_proc, x));

		// Convert nanoseconds to fractional seconds (microseconds)
		fsec_t msec = nanoseconds / 1000;

		TimeADT time;
		struct pg_tm tm;

		tm.tm_year = 0;
		tm.tm_mon  = 0;
		tm.tm_mday = 0;
		tm.tm_hour = seconds / 3600;
		tm.tm_min  = (seconds % 3600) / 60;
		tm.tm_sec  = seconds % 60;

		if (tm2time(&tm, msec, &time))
			elog(ERROR, "Invalid date components");

		return TimeADTGetDatum(time);
	}
}

SCM datum_timetz_to_scm(Datum x, Oid type_oid)
{
	TimeTzADT *ttz = DatumGetTimeTzADTP(x);

	struct pg_tm tm;
	fsec_t msec;
	int tz;

	int secs;

	if (timetz2tm(ttz, &tm, &msec, &tz))
		elog(ERROR, "Invalid time");

	secs = (tm.tm_sec + tm.tm_min * 60 + tm.tm_hour * 3600 + tz) % SECS_PER_DAY;

	if (secs < 0)
		secs += SECS_PER_DAY;

	return call_3(
		make_time_proc,
		time_monotonic_symbol,
		scm_from_long(msec * 1000),
		scm_from_int(secs));
}

Datum scm_to_datum_timetz(SCM x, Oid type_oid)
{
	if (!is_time(x))
		elog(ERROR, "wrong type, time expected: %s", scm_to_string(x));

	else {
		TimeTzADT *result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

		int seconds = scm_to_int(call_1(time_second_proc, x));
		int nanoseconds = scm_to_int(call_1(time_nanosecond_proc, x));

		// Get tz from current time
		struct pg_tm tm;
		fsec_t fsec;
		int tz;
		GetCurrentTimeUsec(&tm, &fsec, &tz);

		result->time = (seconds - tz) * USECS_PER_SEC + (nanoseconds/NS_PER_USEC) % USECS_PER_DAY;

		if (result->time < 0)
			result->time += USECS_PER_DAY;

		result->zone = tz;

		return TimeTzADTPGetDatum(result);
	}
}

SCM datum_interval_to_scm(Datum x, Oid type_oid)
{
	Interval *interval = DatumGetIntervalP(x);

	return call_3(
		make_time_proc,
		time_duration_symbol,
		scm_from_long((interval->time % USECS_PER_SEC) * NS_PER_USEC),
		scm_from_long(
			(int64)interval->month * SECS_PER_MONTH
			+ (int64)interval->day * SECS_PER_DAY
			+ interval->time / USECS_PER_SEC));
}

Datum scm_to_datum_interval(SCM x, Oid type_oid)
{
	SCM seconds = call_1(time_second_proc, x);

	Interval *interval = (Interval *)palloc(sizeof(Interval));

	interval->month = scm_to_int(scm_floor_quotient(seconds, scm_from_int(SECS_PER_MONTH)));

	interval->day = scm_to_int(
		scm_floor_quotient(
			scm_floor_remainder(seconds, scm_from_int(SECS_PER_MONTH)),
			scm_from_int(SECS_PER_DAY)));

	interval->time =
		scm_to_long(scm_floor_remainder(seconds, scm_from_int(SECS_PER_DAY))) * USECS_PER_SEC
		+ scm_to_long(call_1(time_nanosecond_proc, x)) / NS_PER_USEC;

	return IntervalPGetDatum(interval);
}

SCM datum_numeric_to_scm(Datum x, Oid type_oid)
{
	SCM s = scm_from_locale_string(DatumGetCString(DirectFunctionCall1(numeric_out, x)));

	return call_1(string_to_decimal_proc, s);
}

Datum scm_to_datum_numeric(SCM x, Oid type_oid)
{
	char *numeric_str = NULL;

	if (is_decimal(x)) {
		numeric_str = scm_to_locale_string(call_1(decimal_to_string_proc, x));
	}
	else if (scm_is_number(x)) {
		if (scm_is_real(x)) {
			if (scm_to_bool(scm_nan_p(x))) {
				numeric_str = "NaN";
			}
			else if (scm_to_bool(scm_inf_p(x))) {
				if (scm_to_bool(scm_less_p(scm_from_int(0), x)))
					numeric_str = "Infinity";
				else
					numeric_str = "-Infinity";
			}
			else if (scm_is_exact(x) && scm_is_rational(x) && scm_to_int(scm_denominator(x)) > 1) {
				numeric_str = scm_to_string(scm_exact_to_inexact(x));
			}
			else {
				numeric_str = scm_to_string(x);
			}
		}
	}

	if (numeric_str == NULL) {
		elog(ERROR, "decimal result expected, not: %s", scm_to_string(x));
	}

	return DirectFunctionCall3(numeric_in,
	                           CStringGetDatum(numeric_str),
	                           ObjectIdGetDatum(InvalidOid),
	                           Int32GetDatum(-1));
}

SCM datum_bool_to_scm(Datum x, Oid type_oid)
{
	return scm_from_bool(DatumGetBool(x));
}

Datum scm_to_datum_bool(SCM x, Oid type_oid)
{
	return BoolGetDatum(scm_to_bool(x));
}

SCM datum_point_to_scm(Datum x, Oid type_oid)
{
	// Extract the point value from the Datum
	Point *point = DatumGetPointP(x);

	// Convert the point's x and y to SCM numbers
	SCM scm_x = scm_from_double(point->x);
	SCM scm_y = scm_from_double(point->y);

	// Create the SCM point object using the Scheme function
	SCM scm_point = call_2(make_point_proc, scm_x, scm_y);

	return scm_point;
}

Datum scm_to_datum_point(SCM x, Oid type_oid)
{
	SCM scm_x, scm_y;
	float8 x_val, y_val;
	Point *point;

	if (is_point(x)) {
		scm_x = call_1(point_x_proc, x);
		scm_y = call_1(point_y_proc, x);
	}
	else {
		if (!scm_is_vector(x) || scm_c_vector_length(x) != 2)
			elog(ERROR, "point result expected, not: %s", scm_to_string(x));

		scm_x = scm_c_vector_ref(x, 0);
		scm_y = scm_c_vector_ref(x, 1);
	}

	// Get x and y components from the Scheme point object
	// Convert SCM x and y to float8
	x_val = scm_to_double(scm_x);
	y_val = scm_to_double(scm_y);

	// Create a new PostgreSQL point
	point = (Point *) palloc(sizeof(Point));

	point->x = x_val;
	point->y = y_val;

	// Convert the point to a Datum
	return PointPGetDatum(point);
}

SCM datum_line_to_scm(Datum x, Oid type_oid)
{
	// Extract the line value from the Datum
	LINE *line = DatumGetLineP(x);

	// Convert the line's a and y to SCM numbers
	SCM scm_a = scm_from_double(line->A);
	SCM scm_b = scm_from_double(line->B);
	SCM scm_c = scm_from_double(line->C);

	// Create the SCM line object using the Scheme function
	SCM scm_line = call_3(make_line_proc, scm_a, scm_b, scm_c);

	return scm_line;
}

Datum scm_to_datum_line(SCM x, Oid type_oid)
{
	if (!is_line(x)) {
		elog(ERROR, "line result expected, not: %s", scm_to_string(x));
	}
	else {

		// Get a, b, and c components from the Scheme line object
		SCM scm_a = call_1(line_a_proc, x);
		SCM scm_b = call_1(line_b_proc, x);
		SCM scm_c = call_1(line_c_proc, x);

		// Convert SCM a and b to float8
		float8 a_val = scm_to_double(scm_a);
		float8 b_val = scm_to_double(scm_b);
		float8 c_val = scm_to_double(scm_c);

		// Create a new PostgreSQL line
		LINE *line = (LINE *) palloc(sizeof(LINE));

		line->A = a_val;
		line->B = b_val;
		line->C = c_val;

		// Convert the line to a Datum
		return LinePGetDatum(line);
	}
}

SCM datum_lseg_to_scm(Datum x, Oid type_oid)
{
	// Extract the lseg value from the Datum
	LSEG *lseg = DatumGetLsegP(x);

	// Convert the endpoints to Scheme objects
	SCM scm_point_a = datum_point_to_scm(PointPGetDatum(&lseg->p[0]), OID_NOT_USED);
	SCM scm_point_b = datum_point_to_scm(PointPGetDatum(&lseg->p[1]), OID_NOT_USED);

	// Create the Scheme lseg object
	SCM scm_lseg = call_2(make_lseg_proc, scm_point_a, scm_point_b);

	return scm_lseg;
}

Datum scm_to_datum_lseg(SCM x, Oid type_oid)
{
	if (!is_lseg(x)) {
		elog(ERROR, "lseg result expected, not: %s", scm_to_string(x));
	}
	else {

		// Get point components from the Scheme lseg object
		SCM scm_a = call_1(lseg_a_proc, x);
		SCM scm_b = call_1(lseg_b_proc, x);

		// Convert SCM a and b to float8
		Datum a_val = scm_to_datum_point(scm_a, OID_NOT_USED);
		Datum b_val = scm_to_datum_point(scm_b, OID_NOT_USED);

		// Create a new PostgreSQL lseg
		LSEG *lseg = (LSEG *) palloc(sizeof(LSEG));

		lseg->p[0] = *DatumGetPointP(a_val);
		lseg->p[1] = *DatumGetPointP(b_val);

		// Convert the lseg to a Datum
		return LsegPGetDatum(lseg);
	}
}

SCM datum_box_to_scm(Datum x, Oid type_oid)
{
	// Extract the box value from the Datum
	BOX *box = DatumGetBoxP(x);

	// Convert the endpoints to Scheme objects
	SCM scm_point_a = datum_point_to_scm(PointPGetDatum(&box->high), OID_NOT_USED);
	SCM scm_point_b = datum_point_to_scm(PointPGetDatum(&box->low), OID_NOT_USED);

	// Create the Scheme box object
	SCM scm_box = call_2(make_box_proc, scm_point_a, scm_point_b);

	return scm_box;
}

Datum scm_to_datum_box(SCM x, Oid type_oid)
{
	if (!is_box(x)) {
		elog(ERROR, "box result expected, not: %s", scm_to_string(x));
	}
	else {

		// Get a and b components from the Scheme box object
		SCM scm_a = call_1(box_a_proc, x);
		SCM scm_b = call_1(box_b_proc, x);

		// Convert SCM a and b to float8
		Datum a_val = scm_to_datum_point(scm_a, OID_NOT_USED);
		Datum b_val = scm_to_datum_point(scm_b, OID_NOT_USED);

		// Create a new PostgreSQL box
		BOX *box = (BOX *) palloc(sizeof(BOX));

		box->high = *DatumGetPointP(a_val);
		box->low = *DatumGetPointP(b_val);

		// Convert the box to a Datum
		return BoxPGetDatum(box);
	}
}

SCM datum_path_to_scm(Datum x, Oid type_oid)
{
	// Extract the path struct from the Datum
	PATH *path = DatumGetPathP(x);

	// Create a Scheme vector to hold the points
	SCM scm_points_vector = scm_c_make_vector(path->npts, SCM_BOOL_F);
	SCM scm_is_closed = scm_from_bool(path->closed);

	// Convert each point to Scheme object and populate the vector
	for (int i = 0; i < path->npts; i++) {
		Datum point_datum = PointPGetDatum(&path->p[i]);
		SCM scm_point = datum_point_to_scm(point_datum, OID_NOT_USED);
		scm_c_vector_set_x(scm_points_vector, i, scm_point);
	}

	return call_2(make_path_proc, scm_is_closed, scm_points_vector);
}

Datum scm_to_datum_path(SCM x, Oid type_oid)
{
	if (!is_path(x)) {
		elog(ERROR, "path result expected, not: %s", scm_to_string(x));
	}
	else {

		PATH *path;

		// Get the Scheme vector of points from the Scheme path object
		SCM scm_points_vector = call_1(path_points_proc, x);

		// Get the number of points in the vector
		int npts = scm_to_int(scm_vector_length(scm_points_vector));

		// Compute the size of allocation required
		int base_size = sizeof(path->p[0]) * npts;
		int size = offsetof(PATH, p) + base_size;

		/* Check for integer overflow */
		if (base_size / npts != sizeof(path->p[0]) || size <= base_size) {
			ereport(ERROR, errmsg("too many points requested"));
		}

		path = (PATH *) palloc(size);

		SET_VARSIZE(path, size);
		path->npts = npts;

		path->closed = scm_to_bool(call_1(path_is_closed_proc, x));

		/* prevent instability in unused pad bytes */
		path->dummy = 0;

		// Convert each point from Scheme object to Datum and populate the PATH
		for (int i = 0; i < npts; i++) {
			SCM scm_point = scm_c_vector_ref(scm_points_vector, i);
			Datum point_datum = scm_to_datum_point(scm_point, OID_NOT_USED);
			path->p[i] = *DatumGetPointP(point_datum); // Assuming DatumGetPointP returns a Point *
		}

		return PathPGetDatum(path);
	}
}

SCM datum_polygon_to_scm(Datum x, Oid type_oid)
{
	// Extract the polygon struct from the Datum
	POLYGON *polygon = DatumGetPolygonP(x);

	// Create a Scheme vector to hold the points
	SCM scm_points_vector = scm_c_make_vector(polygon->npts, SCM_BOOL_F);
	SCM scm_boundbox = datum_box_to_scm(BoxPGetDatum(&polygon->boundbox), OID_NOT_USED);

	// Convert each point to Scheme object and populate the vector
	for (int i = 0; i < polygon->npts; i++) {
		Datum point_datum = PointPGetDatum(&polygon->p[i]);
		SCM scm_point = datum_point_to_scm(point_datum, OID_NOT_USED);
		scm_c_vector_set_x(scm_points_vector, i, scm_point);
	}

	return call_2(make_polygon_proc, scm_boundbox, scm_points_vector);
}

Datum scm_to_datum_polygon(SCM x, Oid type_oid)
{
	if (!is_polygon(x)) {
		elog(ERROR, "polygon result expected, not: %s", scm_to_string(x));
	}
	else {

		POLYGON *polygon;

		// Get the Scheme vector of points from the Scheme polygon object
		SCM scm_points_vector = call_1(polygon_points_proc, x);

		// Get the number of points in the vector
		int npts = scm_to_int(scm_vector_length(scm_points_vector));

		// Compute the size of allocation required
		int base_size = sizeof(polygon->p[0]) * npts;
		int size = offsetof(POLYGON, p) + base_size;
		SCM scm_boundbox;

		/* Check for integer overflow */
		if (base_size / npts != sizeof(polygon->p[0]) || size <= base_size) {
			ereport(ERROR, errmsg("too many points requested"));
		}

		polygon = (POLYGON *) palloc(size);

		SET_VARSIZE(polygon, size);
		polygon->npts = npts;

		scm_boundbox = call_1(polygon_boundbox_proc, x);
		polygon->boundbox = *DatumGetBoxP(scm_to_datum_box(scm_boundbox, OID_NOT_USED));

		// Convert each point from Scheme object to Datum and populate the POLYGON
		for (int i = 0; i < npts; i++) {
			SCM scm_point = scm_c_vector_ref(scm_points_vector, i);
			Datum point_datum = scm_to_datum_point(scm_point, OID_NOT_USED);
			polygon->p[i] = *DatumGetPointP(point_datum); // Assuming DatumGetPointP returns a Point *
		}

		return PolygonPGetDatum(polygon);
	}
}

SCM datum_circle_to_scm(Datum x, Oid type_oid)
{
	CIRCLE *circle = DatumGetCircleP(x);  // Convert Datum to CIRCLE type
	Point center = circle->center;
	float8 radius = circle->radius;

	// Convert the center point and radius to SCM
	SCM scm_center = datum_point_to_scm(CirclePGetDatum(&center), OID_NOT_USED);
	SCM scm_radius = scm_from_double(radius);

	// Use your Scheme procedure to create the circle record
	return call_2(make_circle_proc, scm_center, scm_radius);
}

Datum scm_to_datum_circle(SCM x, Oid type_oid)
{
	if (!is_circle(x)) {
		elog(ERROR, "circle result expected, not: %s", scm_to_string(x));
	}
	else {
		// Retrieve the center point and radius from the Scheme record
		SCM scm_center = call_1(circle_center_proc, x);
		SCM scm_radius = call_1(circle_radius_proc, x);

		// Convert the center to a Point Datum
		Datum center_datum = scm_to_datum_point(scm_center, OID_NOT_USED);

		// Convert the radius to a float8
		float8 radius = scm_to_double(scm_radius);

		// Create a CIRCLE structure and populate it
		CIRCLE *circle = (CIRCLE *) palloc(sizeof(CIRCLE));

		circle->center = *DatumGetPointP(center_datum);
		circle->radius = radius;

		return CirclePGetDatum(circle);
	}
}

SCM datum_inet_to_scm(Datum x, Oid type_oid)
{
	inet *inet_val = DatumGetInetPP(x); // Assuming inet type is pass-by-reference

	// Extract family
	int family = ip_family(inet_val);
	SCM scm_family = (family == PGSQL_AF_INET) ? scm_from_utf8_symbol("inet") : scm_from_utf8_symbol("inet6");

	// Extract netmask bits
	SCM scm_bits = scm_from_int(ip_bits(inet_val));

	// Extract the address
	unsigned char *addr = ip_addr(inet_val);
	int addr_len = (family == PGSQL_AF_INET) ? 4 : 16;
	SCM scm_address = scm_c_make_bytevector(addr_len);

	for (int i = 0; i < addr_len; ++i) {
		scm_c_bytevector_set_x(scm_address, i, addr[i]);
	}

	return call_3(make_inet_proc, scm_family, scm_bits, scm_address);
}

Datum scm_to_datum_inet(SCM x, Oid type_oid)
{
	if (!is_inet(x)) {
		elog(ERROR, "inet result expected, not: %s", scm_to_string(x));
	}
	else {
		inet *inet;

		// Get the family, bits, and address from the SCM object
		SCM scm_family = call_1(inet_family_proc, x);
		SCM scm_bits = call_1(inet_bits_proc, x);
		SCM scm_address = call_1(inet_address_proc, x);

		char *family_str = scm_to_locale_string(scm_symbol_to_string(scm_family));
		int bits = scm_to_int(scm_bits);
		size_t addr_len = scm_c_bytevector_length(scm_address);

		int family;

		// Check family and address length

		if (strcmp(family_str, "inet") == 0) {
			if (addr_len != 4) {
				free(family_str);
				elog(ERROR, "Incorrect address length %ld for type %s", addr_len, scm_to_string(scm_family));
			}
			family = PGSQL_AF_INET;
		}
		else if (strcmp(family_str, "inet6") == 0) {
			if (addr_len != 16) {
				free(family_str);
				elog(ERROR, "Incorrect address length %ld for type %s", addr_len, scm_to_string(scm_family));
			}
			family = PGSQL_AF_INET6;
		}
		else {
			free(family_str);
			elog(ERROR, "Unknown inet family %s", scm_to_string(scm_family));
		}

		free(family_str);

		// Create a new inet struct
		// inet = (inet *) palloc0(sizeof(inet));
		inet = palloc0(sizeof(inet));
		SET_INET_VARSIZE(inet);

		inet->inet_data.family = family;
		inet->inet_data.bits = bits;

		for (size_t i = 0; i < addr_len; ++i) {
			inet->inet_data.ipaddr[i] = scm_c_bytevector_ref(scm_address, i);
		}

		return PointerGetDatum(inet);
	}
}

SCM datum_macaddr_to_scm(Datum x, Oid type_oid)
{
	macaddr *mac = (macaddr *) DatumGetPointer(x);

	// Create a new SCM bytevector to hold the 6 bytes
	SCM scm_data = scm_c_make_bytevector(6);

	// Populate the bytevector with the bytes from the macaddr

	scm_c_bytevector_set_x(scm_data, 0, mac->a);
	scm_c_bytevector_set_x(scm_data, 1, mac->b);
	scm_c_bytevector_set_x(scm_data, 2, mac->c);
	scm_c_bytevector_set_x(scm_data, 3, mac->d);
	scm_c_bytevector_set_x(scm_data, 4, mac->e);
	scm_c_bytevector_set_x(scm_data, 5, mac->f);

	return call_1(make_macaddr_proc, scm_data);
}

Datum scm_to_datum_macaddr(SCM x, Oid type_oid)
{
	if (!is_macaddr(x)) {
		elog(ERROR, "macaddr result expected, not: %s", scm_to_string(x));
	}
	else {
		// Get the data bytevector from the SCM object
		SCM scm_data = call_1(macaddr_data_proc, x);
		macaddr *mac;

		// Check the length of the bytevector
		if (scm_c_bytevector_length(scm_data) != 6) {
			elog(ERROR, "Invalid data length for macaddr. Expected 6 bytes.");
		}

		// Create a new PostgreSQL macaddr
		mac = (macaddr *) palloc(sizeof(macaddr));

		// Populate the macaddr with bytes from the bytevector

		mac->a = scm_c_bytevector_ref(scm_data, 0);
		mac->b = scm_c_bytevector_ref(scm_data, 1);
		mac->c = scm_c_bytevector_ref(scm_data, 2);
		mac->d = scm_c_bytevector_ref(scm_data, 3);
		mac->e = scm_c_bytevector_ref(scm_data, 4);
		mac->f = scm_c_bytevector_ref(scm_data, 5);

		return PointerGetDatum(mac);
	}
}

SCM datum_macaddr8_to_scm(Datum x, Oid type_oid)
{
	macaddr8 *mac = (macaddr8 *) DatumGetPointer(x);

	// Create a new SCM bytevector to hold the 8 bytes
	SCM scm_data = scm_c_make_bytevector(8);

	// Populate the bytevector with the bytes from the macaddr8

	scm_c_bytevector_set_x(scm_data, 0, mac->a);
	scm_c_bytevector_set_x(scm_data, 1, mac->b);
	scm_c_bytevector_set_x(scm_data, 2, mac->c);
	scm_c_bytevector_set_x(scm_data, 3, mac->d);
	scm_c_bytevector_set_x(scm_data, 4, mac->e);
	scm_c_bytevector_set_x(scm_data, 5, mac->f);
	scm_c_bytevector_set_x(scm_data, 6, mac->g);
	scm_c_bytevector_set_x(scm_data, 7, mac->h);

	return call_1(make_macaddr8_proc, scm_data);
}

Datum scm_to_datum_macaddr8(SCM x, Oid type_oid)
{
	if (!is_macaddr8(x)) {
		elog(ERROR, "macaddr8 result expected, not: %s", scm_to_string(x));
	}
	else {
		// Get the data bytevector from the SCM object
		SCM scm_data = call_1(macaddr8_data_proc, x);
		macaddr8 *mac;

		// Check the length of the bytevector
		if (scm_c_bytevector_length(scm_data) != 8) {
			elog(ERROR, "Invalid data length for macaddr8. Expected 8 bytes.");
		}

		// Create a new PostgreSQL macaddr8
		mac = (macaddr8 *) palloc(sizeof(macaddr8));

		// Populate the macaddr8 with bytes from the bytevector

		mac->a = scm_c_bytevector_ref(scm_data, 0);
		mac->b = scm_c_bytevector_ref(scm_data, 1);
		mac->c = scm_c_bytevector_ref(scm_data, 2);
		mac->d = scm_c_bytevector_ref(scm_data, 3);
		mac->e = scm_c_bytevector_ref(scm_data, 4);
		mac->f = scm_c_bytevector_ref(scm_data, 5);
		mac->g = scm_c_bytevector_ref(scm_data, 6);
		mac->h = scm_c_bytevector_ref(scm_data, 7);

		return PointerGetDatum(mac);
	}
}

SCM datum_bit_string_to_scm(Datum x, Oid type_oid)
{

	VarBit *bit_str = DatumGetVarBitP(x);

	// Get the length in bits
	int bitlen = VARBITLEN(bit_str);

	// Create a new bytevector in Scheme to hold the data
	SCM scm_data = scm_c_make_bytevector(VARBITBYTES(bit_str));

	// Copy the data from the varbit struct to the bytevector
	memcpy(SCM_BYTEVECTOR_CONTENTS(scm_data), VARBITS(bit_str), VARBITBYTES(bit_str));

	return call_2(make_bit_string_proc, scm_data, scm_from_int(bitlen));
}

Datum scm_to_datum_bit_string(SCM x, Oid type_oid)
{
	if (!is_bit_string(x)) {
		elog(ERROR, "bit string result expected, not: %s", scm_to_string(x));
	}
	else {
		// Get the length and data from the Scheme record
		SCM scm_length = call_1(bit_string_length_proc, x);
		SCM scm_data = call_1(bit_string_data_proc, x);

		// Convert the length to a C int
		int bitlen = scm_to_int(scm_length);

		// Allocate space for the varbit struct
		VarBit *bit_str = (VarBit *) palloc0(VARBITTOTALLEN(bitlen));

		SET_VARSIZE(bit_str, VARBITTOTALLEN(bitlen));
		VARBITLEN(bit_str) = bitlen;

		// Copy the data from the Scheme bytevector to the varbit struct
		memcpy(VARBITS(bit_str), SCM_BYTEVECTOR_CONTENTS(scm_data), VARBITBYTES(bit_str));

		// Return the varbit struct as a Datum
		return VarBitPGetDatum(bit_str);
	}
}

SCM datum_uuid_to_scm(Datum x, Oid type_oid)
{
	// Assume x is a UUID Datum, cast it to pg_uuid_t *
	pg_uuid_t *uuid_ptr = DatumGetUUIDP(x);

	// Create an SCM bytevector to hold the UUID bytes
	SCM scm_bytevector = scm_c_make_bytevector(UUID_LEN);

	// Copy the UUID bytes into the SCM bytevector
	memcpy(SCM_BYTEVECTOR_CONTENTS(scm_bytevector), uuid_ptr->data, UUID_LEN);

	// Return the SCM bytevector
	return scm_bytevector;
}

Datum scm_to_datum_uuid(SCM x, Oid type_oid)
{
	if (!scm_is_bytevector(x)) {
		// Handle error: x is not a bytevector
		ereport(ERROR, (errmsg("Expected a bytevector for UUID conversion")));
	}
	else {

		size_t len = SCM_BYTEVECTOR_LENGTH(x);

		if (len != UUID_LEN) {
			// Handle error: incorrect bytevector length
			ereport(ERROR, (errmsg("Expected a bytevector of length %d for UUID conversion", UUID_LEN)));
		}
		else {

			pg_uuid_t *uuid_ptr = (pg_uuid_t *) palloc(UUID_LEN);
			memcpy(uuid_ptr->data, SCM_BYTEVECTOR_CONTENTS(x), len);

			return UUIDPGetDatum(uuid_ptr);
		}
	}
}

SCM datum_xml_to_scm(Datum x, Oid type_oid)
{
	// Convert Datum to text
	text *xml_text = DatumGetTextP(x);

	// Convert text to C string
	char *cstr = text_to_cstring(xml_text);

	// Convert C string to SCM string
	SCM scm_string = scm_from_locale_string(cstr);

	// Free C string
	pfree(cstr);

	return scm_string;
}

Datum scm_to_datum_xml(SCM x, Oid type_oid)
{
	// Convert SCM string to C string
	char *cstr = scm_to_locale_string(x);

	// Convert C string to text
	text *xml_text = cstring_to_text(cstr);

	// Convert text to Datum
	Datum result = PointerGetDatum(xml_text);

	// Free C string
	free(cstr);

	return result;
}

SCM datum_json_to_scm(Datum x, Oid type_oid)
{
	// Convert Datum to text
	text *json_text = DatumGetTextP(x);

	// Convert text to C string
	char *cstr = text_to_cstring(json_text);

	// Convert C string to SCM string
	SCM scm_string = scm_from_locale_string(cstr);

	// Free C string
	pfree(cstr);

	return scm_string;
}

Datum scm_to_datum_json(SCM x, Oid type_oid)
{
	// Convert SCM string to C string
	char *cstr = scm_to_locale_string(x);

	// Convert C string to text
	text *json_text = cstring_to_text(cstr);

	// Convert text to Datum
	Datum result = PointerGetDatum(json_text);

	// Free C string
	free(cstr);

	return result;
}

SCM datum_jsonb_to_scm(Datum x, Oid type_oid)
{
	return call_1(make_jsonb_proc, jsonb_to_scm_expr(DatumGetJsonbP(x)));
}

SCM jsonb_to_scm_expr(Jsonb *jsb)
{
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken type;
	bool raw_scalar = false;

	SCM current = SCM_EOL, stack = SCM_EOL;

	it = JsonbIteratorInit(&jsb->root);

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {

		switch (type) {

			case WJB_BEGIN_ARRAY:
				stack = scm_cons(current, stack);
				current = SCM_EOL;
				raw_scalar = v.val.array.rawScalar;
				break;

			case WJB_BEGIN_OBJECT:
				stack = scm_cons(current, stack);
				current = SCM_EOL;
				break;

			case WJB_ELEM:
			case WJB_KEY:
			case WJB_VALUE:
				current = scm_cons(jsb_scalar_to_scm(&v), current);
				break;

			case WJB_END_ARRAY:
				if (raw_scalar)
					current = scm_cons(scm_car(current), scm_car(stack));
				else
					current = scm_cons(scm_vector(scm_reverse(current)), scm_car(stack));

				stack = scm_cdr(stack);
				break;

			case WJB_END_OBJECT:
				current = scm_cons(scm_reverse(current), scm_car(stack));
				stack = scm_cdr(stack);
				break;

			default:
				elog(ERROR, "unknown jsonb iterator token type");
		}
	}

	return scm_car(current);
}

SCM jsb_scalar_to_scm(JsonbValue *jsb)
{
	switch (jsb->type) {
		case jbvNull:
			return null_symbol;

		case jbvString:
			return scm_from_locale_stringn(jsb->val.string.val, jsb->val.string.len);

		case jbvNumeric:
			return datum_numeric_to_scm(PointerGetDatum(jsb->val.numeric), InvalidOid);

		case jbvBool:
			return scm_from_bool(jsb->val.boolean);

		default:
			elog(ERROR, "unknown jsonb scalar type");
	}
}

Datum scm_to_datum_jsonb(SCM x, Oid type_oid)
{
	return JsonbPGetDatum(jsonb_root_expr_to_jsonb(x));
}

Jsonb *jsonb_root_expr_to_jsonb(SCM expr)
{
	JsonbValue jsbv;

	while (is_jsonb(expr))
		expr = call_1(jsonb_expr_proc, expr);

	if (scm_is_vector(expr) || scm_is_pair(expr))
		jsonb_expr_to_jsbv(expr, &jsbv);

	else
		jsonb_raw_scalar_expr_to_jsbv(expr, &jsbv);

	return JsonbValueToJsonb(&jsbv);
}

void jsonb_expr_to_jsbv(SCM expr, JsonbValue *jsbv)
{
	while (is_jsonb(expr))
		expr = call_1(jsonb_expr_proc, expr);

	if (scm_is_vector(expr))
		jsonb_array_expr_to_jsbv(expr, jsbv);

	else if (scm_is_pair(expr))
		jsonb_object_expr_to_jsbv(expr, jsbv);

	else
		jsonb_scalar_expr_to_jsbv(expr, jsbv);
}

void jsonb_array_expr_to_jsbv(SCM expr, JsonbValue *jsbv)
{
	int i, len = scm_c_vector_length(expr);
	JsonbValue *elem;

	jsbv->type = jbvArray;
	jsbv->val.array.nElems = len;
	jsbv->val.array.elems = (JsonbValue *)palloc(sizeof(JsonbValue) * len);
	jsbv->val.array.rawScalar = false;

	for (i = 0, elem = jsbv->val.array.elems; i < len; i++, elem++)
		jsonb_expr_to_jsbv(scm_c_vector_ref(expr, i), elem);
}

void jsonb_object_expr_to_jsbv(SCM expr, JsonbValue *jsbv)
{
	long count, i, len = scm_ilength(expr);
	JsonbPair *pair;
	SCM x = expr;

	if (len < 0 || len % 2)
		elog(ERROR, "jsonb object expression must be an alist with string keys: %s", scm_to_string(expr));

	count = len / 2;

	jsbv->type = jbvObject;
	jsbv->val.object.nPairs = count;
	jsbv->val.object.pairs = (JsonbPair *)palloc(sizeof(JsonbPair) * count);

	for (i = 0, pair = jsbv->val.object.pairs; i < count; i++, pair++) {

		SCM key;

		key = scm_car(x);

		while (is_jsonb(key))
			key = call_1(jsonb_expr_proc, key);

		if (!scm_is_string(key))
			elog(ERROR, "jsonb object expression must be an alist with string keys: %s", scm_to_string(expr));

		jsonb_scalar_expr_to_jsbv(key, &pair->key);

		x = scm_cdr(x);

		jsonb_expr_to_jsbv(scm_car(x), &pair->value);

		x = scm_cdr(x);

		pair->order = i;
	}
}

void jsonb_raw_scalar_expr_to_jsbv(SCM expr, JsonbValue *jsbv)
{
	jsbv->type = jbvArray;
	jsbv->val.array.nElems = 1;
	jsbv->val.array.elems = (JsonbValue *)palloc(sizeof(JsonbValue));
	jsbv->val.array.rawScalar = true;
	jsonb_scalar_expr_to_jsbv(expr, jsbv->val.array.elems);
}

void jsonb_scalar_expr_to_jsbv(SCM expr, JsonbValue *jsbv)
{
	if (scm_is_string(expr)) {
		size_t len;
		char *c_str = scm_to_locale_stringn(expr, &len);

		jsbv->type = jbvString;
		jsbv->val.string.len = (int)len;
		jsbv->val.string.val = pstrdup(c_str);
		free(c_str);
	}

	else if (scm_is_number(expr) || is_decimal(expr)) {
		jsbv->type = jbvNumeric;
		jsbv->val.numeric = DatumGetNumeric(scm_to_datum_numeric(expr, OID_NOT_USED));
	}

	else if (expr == null_symbol) {
		jsbv->type = jbvNull;
	}

	else if (expr == SCM_BOOL_T || expr == SCM_BOOL_F) {
		jsbv->type = jbvBool;
		jsbv->val.boolean = scm_to_bool(expr);
	}

	else
		elog(ERROR, "d");
}

SCM datum_jsonpath_to_scm(Datum x, Oid type_oid)
{
	JsonPath *jsp = DatumGetJsonPathP(x);
	JsonPathItem elem;
	SCM expr = SCM_EOL;

	jsp_init(&elem, jsp);
	expr = jsp_to_scm(&elem, expr);

	return call_2(make_jsonpath_proc, scm_from_bool(!(jsp->header & JSONPATH_LAX)), expr);
}

Datum scm_to_datum_jsonpath(SCM x, Oid type_oid)
{
	StringInfoData buf;
	JsonPath *jsp;

	call_1(validate_jsonpath_proc, x);

	initStringInfo(&buf);
	appendStringInfoSpaces(&buf, JSONPATH_HDRSZ);

	scm_expr_to_jsp(&buf, call_1(jsonpath_expr_proc, x));

	jsp = (JsonPath *)buf.data;
	SET_VARSIZE(jsp, buf.len);

	jsp->header = JSONPATH_VERSION;

	if (call_1(jsonpath_is_strict_proc, x) == SCM_BOOL_F)
		jsp->header |= JSONPATH_LAX;

	return PointerGetDatum(jsp);
}

SCM jsp_to_scm(JsonPathItem *v, SCM expr)
{
	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	do
		expr = jsp_item_to_scm(v, expr);
	while (jsp_next(v));

	return expr;
}

SCM jsp_item_to_scm(JsonPathItem *v, SCM expr)
{
	JsonPathItem elem, left_elem, right_elem;
	Datum n;
	SCM arg, left, op, right;

	switch (v->type)
	{
		// Literals
		case jpiBool:
			return jspGetBool(v) ? SCM_BOOL_T : SCM_BOOL_F;

		case jpiNull:
			return SCM_EOL;

		case jpiNumeric:
			n = NumericGetDatum(jsp_get_numeric(v));
			arg = scm_from_locale_string(DatumGetCString(DirectFunctionCall1(numeric_out, n)));
			return call_1(string_to_decimal_proc, arg);

		case jpiString:
			return scm_from_locale_string(jsp_get_string(v));

		// Special Symbols
		case jpiCurrent:
		case jpiLast:
		case jpiRoot:
			return scm_list_1(jsp_operation_symbol(v->type));

		// Generic forms
		case jpiAbs:
		case jpiAnyArray:
		case jpiAnyKey:
		case jpiCeiling:
		case jpiDouble:
		case jpiFloor:
		case jpiKeyValue:
		case jpiSize:
		case jpiType:
			op = jsp_operation_symbol(v->type);
			return scm_list_2(op, expr);

		case jpiExists:
		case jpiIsUnknown:
		case jpiMinus:
		case jpiNot:
		case jpiPlus:
			op = jsp_operation_symbol(v->type);
			jsp_get_arg(v, &elem);
			return scm_list_2(op, jsp_to_scm(&elem, SCM_EOL));

		case jpiAdd:
		case jpiAnd:
		case jpiDiv:
		case jpiEqual:
		case jpiGreater:
		case jpiGreaterOrEqual:
		case jpiLess:
		case jpiLessOrEqual:
		case jpiMod:
		case jpiMul:
		case jpiNotEqual:
		case jpiOr:
		case jpiStartsWith:
		case jpiSub:

			op = jsp_operation_symbol(v->type);

			jsp_get_left_arg(v, &left_elem);
			jsp_get_right_arg(v, &right_elem);

			return scm_list_3(
				op,
				jsp_to_scm(&left_elem, SCM_EOL),
				jsp_to_scm(&right_elem, SCM_EOL));

		// Special forms
		case jpiAny:

			left = jsp_bounds_to_scm(v->content.anybounds.first);
			right = jsp_bounds_to_scm(v->content.anybounds.last);

			return scm_list_4(any_symbol, left, right, expr);

		case jpiDatetime:

			if (!v->content.arg)
				left = SCM_BOOL_F;

			else {
				jsp_get_arg(v, &left_elem);
				left = jsp_to_scm(&left_elem, SCM_EOL);
			}

			return scm_list_3(datetime_symbol, left, expr);

		case jpiFilter:

			jsp_get_arg(v, &elem);
			return scm_list_3(filter_symbol, jsp_to_scm(&elem, SCM_EOL), expr);

		case jpiIndexArray:

			arg = SCM_EOL;

			for (int i = v->content.array.nelems-1; i >= 0 ; i--) {

				JsonPathItem from;
				JsonPathItem to;
				bool range = jsp_get_array_subscript(v, &from, &to, i);

				arg = scm_cons(
					scm_list_2(
						jsp_to_scm(&from, SCM_EOL),
						range ? jsp_to_scm(&to, SCM_EOL) : SCM_BOOL_F),
					arg);
			}

			return scm_list_3(index_array_symbol, arg, expr);

		case jpiKey:
			return scm_list_3(key_symbol, scm_from_locale_string(jsp_get_string(v)), expr);

		case jpiLikeRegex:

			jsp_init_by_buffer(&left_elem, v->base, v->content.like_regex.expr);

			return scm_list_4(
				like_regex_symbol,
				jsp_to_scm(&left_elem, SCM_EOL),
				scm_from_locale_string(v->content.like_regex.pattern),
				jsp_regex_flags_to_scm(v->content.like_regex.flags));

		case jpiVariable:
			return scm_list_2(var_symbol, scm_from_locale_string(jsp_get_string(v)));

		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", v->type);
	}

}

SCM jsp_operation_symbol(JsonPathItemType type)
{
	switch (type)
	{
		case jpiAbs:            return abs_symbol;
		case jpiAdd:            return add_symbol;
		case jpiAnd:            return and_symbol;
		case jpiAnyArray:       return any_array_symbol;
		case jpiAnyKey:         return any_key_symbol;
		case jpiCeiling:        return ceiling_symbol;
		case jpiCurrent:        return current_symbol;
		case jpiDiv:            return div_symbol;
		case jpiDouble:         return double_symbol;
		case jpiEqual:          return equal_symbol;
		case jpiExists:         return exists_symbol;
		case jpiFloor:          return floor_symbol;
		case jpiGreater:        return greater_symbol;
		case jpiGreaterOrEqual: return greater_or_equal_symbol;
		case jpiIsUnknown:      return is_unknown_symbol;
		case jpiKeyValue:       return keyvalue_symbol;
		case jpiLast:           return last_symbol;
		case jpiLess:           return less_symbol;
		case jpiLessOrEqual:    return less_or_equal_symbol;
		case jpiLikeRegex:      return like_regex_symbol;
		case jpiMinus:          return negate_symbol;
		case jpiMod:            return mod_symbol;
		case jpiMul:            return mul_symbol;
		case jpiNot:            return not_symbol;
		case jpiNotEqual:       return not_equal_symbol;
		case jpiOr:             return or_symbol;
		case jpiPlus:           return nop_symbol; // unary plus doesn't do anything
		case jpiRoot:           return root_symbol;
		case jpiSize:           return size_symbol;
		case jpiStartsWith:     return starts_with_symbol;
		case jpiSub:            return sub_symbol;
		case jpiType:           return type_symbol;
		default:
			return SCM_BOOL_F;
	}
}

Numeric jsp_get_numeric(JsonPathItem *v)
{
	return (Numeric) v->content.value.data;
}

char *jsp_get_string(JsonPathItem *v)
{
	return v->content.value.data;
}

SCM jsp_bounds_to_scm(uint32 value)
{
	if (value == PG_UINT32_MAX)
		return SCM_BOOL_F;

	return scm_from_uint32(value);
}

SCM jsp_regex_flags_to_scm(uint32 flags)
{
	SCM result = SCM_EOL;

	if (flags) {
		if (flags & JSP_REGEX_DOTALL) result = scm_cons(dotall_symbol, result);
		if (flags & JSP_REGEX_ICASE)  result = scm_cons(icase_symbol, result);
		if (flags & JSP_REGEX_MLINE)  result = scm_cons(mline_symbol, result);
		if (flags & JSP_REGEX_QUOTE)  result = scm_cons(quote_symbol, result);
		if (flags & JSP_REGEX_WSPACE) result = scm_cons(wspace_symbol, result);
	}

	return result;
}

void jsp_get_arg(JsonPathItem *v, JsonPathItem *a)
{
	jsp_init_by_buffer(a, v->base, v->content.arg);
}

void jsp_get_left_arg(JsonPathItem *v, JsonPathItem *a)
{
	jsp_init_by_buffer(a, v->base, v->content.args.left);
}

void jsp_get_right_arg(JsonPathItem *v, JsonPathItem *a)
{
	jsp_init_by_buffer(a, v->base, v->content.args.right);
}

bool jsp_get_array_subscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem *to, int i)
{
	jsp_init_by_buffer(from, v->base, v->content.array.elems[i].from);

	if (!v->content.array.elems[i].to)
		return false;

	jsp_init_by_buffer(to, v->base, v->content.array.elems[i].to);

	return true;
}

void jsp_init(JsonPathItem *v, JsonPath *js)
{
	jsp_init_by_buffer(v, js->data, 0);
}

bool jsp_next(JsonPathItem *v)
{
	return v->nextPos > 0 && (jsp_init_by_buffer(v, v->base, v->nextPos), true);
}

#define read_byte(v, b, p) do {			\
	(v) = *(uint8*)((b) + (p));			\
	(p) += 1;							\
} while(0)								\

#define read_int32(v, b, p) do {		\
	(v) = *(uint32*)((b) + (p));		\
	(p) += sizeof(int32);				\
} while(0)								\

#define read_int32_n(v, b, p, n) do {	\
	(v) = (void *)((b) + (p));			\
	(p) += sizeof(int32) * (n);			\
} while(0)								\

void jsp_init_by_buffer(JsonPathItem *v, char *base, int32 pos)
{
	v->base = base + pos;

	read_byte(v->type, base, pos);
	pos = INTALIGN((uintptr_t) (base + pos)) - (uintptr_t) base;
	read_int32(v->nextPos, base, pos);

	switch (v->type)
	{
		case jpiNull:
		case jpiRoot:
		case jpiCurrent:
		case jpiAnyArray:
		case jpiAnyKey:
		case jpiType:
		case jpiSize:
		case jpiAbs:
		case jpiFloor:
		case jpiCeiling:
		case jpiDouble:
		case jpiKeyValue:
		case jpiLast:
			break;
		case jpiKey:
		case jpiString:
		case jpiVariable:
			read_int32(v->content.value.datalen, base, pos);
			/* FALLTHROUGH */
		case jpiNumeric:
		case jpiBool:
			v->content.value.data = base + pos;
			break;
		case jpiAnd:
		case jpiOr:
		case jpiAdd:
		case jpiSub:
		case jpiMul:
		case jpiDiv:
		case jpiMod:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiStartsWith:
			read_int32(v->content.args.left, base, pos);
			read_int32(v->content.args.right, base, pos);
			break;
		case jpiLikeRegex:
			read_int32(v->content.like_regex.flags, base, pos);
			read_int32(v->content.like_regex.expr, base, pos);
			read_int32(v->content.like_regex.patternlen, base, pos);
			v->content.like_regex.pattern = base + pos;
			break;
		case jpiNot:
		case jpiExists:
		case jpiIsUnknown:
		case jpiPlus:
		case jpiMinus:
		case jpiFilter:
		case jpiDatetime:
			read_int32(v->content.arg, base, pos);
			break;
		case jpiIndexArray:
			read_int32(v->content.array.nelems, base, pos);
			read_int32_n(v->content.array.elems, base, pos,
						 v->content.array.nelems * 2);
			break;
		case jpiAny:
			read_int32(v->content.anybounds.first, base, pos);
			read_int32(v->content.anybounds.last, base, pos);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", v->type);
	}
}

int scm_expr_to_jsp(StringInfo buf, SCM expr)
{
	int32 base, next = 0;
	int32 arg, left, right;

	JsonPathItemType type = jsp_expr_type(expr);

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch (type) {

		// Literals
		case jpiBool:
			write_jsp_header(buf, type);
			write_jsp_bool(buf, expr);
			break;

		case jpiNull:
			write_jsp_header(buf, type);
			break;

		case jpiNumeric:
			write_jsp_header(buf, type);
			write_jsp_numeric(buf, expr);
			break;

		case jpiString:
			write_jsp_header(buf, type);
			write_jsp_string(buf, expr);
			break;

		// Special symbols
		case jpiRoot:
		case jpiCurrent:
		case jpiLast:
			next = write_jsp_header(buf, type);
			break;

		// Generic forms
		case jpiAbs:
		case jpiAnyArray:
		case jpiAnyKey:
		case jpiCeiling:
		case jpiDouble:
		case jpiFloor:
		case jpiKeyValue:
		case jpiSize:
		case jpiType:
			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 1));
			next = write_jsp_header(buf, type);
			break;

		case jpiExists:
		case jpiIsUnknown:
		case jpiMinus:
		case jpiNot:
		case jpiPlus:
			base = buf->len;
			next = write_jsp_header(buf, type);
			write_jsp_arg(buf, base, reserve_jsp_offset(buf), scm_c_list_ref(expr, 1));
			break;

		case jpiAdd:
		case jpiAnd:
		case jpiDiv:
		case jpiEqual:
		case jpiGreater:
		case jpiGreaterOrEqual:
		case jpiLess:
		case jpiLessOrEqual:
		case jpiMod:
		case jpiMul:
		case jpiNotEqual:
		case jpiOr:
		case jpiStartsWith:
		case jpiSub:
			/*
			 * First, reserve place for left/right arg's positions, then
			 * record both args and sets actual position in reserved
			 * places.
			 */
			base = buf->len;
			write_jsp_header(buf, type);

			left  = reserve_jsp_offset(buf);
			right = reserve_jsp_offset(buf);

			write_jsp_arg(buf, base, left,  scm_c_list_ref(expr, 1));
			write_jsp_arg(buf, base, right, scm_c_list_ref(expr, 2));

			break;

		// Special forms
		case jpiAny:

			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 3));
			next = write_jsp_header(buf, type);

			write_jsp_any_bounds(buf, scm_c_list_ref(expr, 1));
			write_jsp_any_bounds(buf, scm_c_list_ref(expr, 2));

			break;

		case jpiDatetime: {

			SCM format;

			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 2));
			base = buf->len;
			next = write_jsp_header(buf, type);

			arg = reserve_jsp_offset(buf);

			format = scm_c_list_ref(expr, 1);

			if (format != SCM_BOOL_F)
				write_jsp_arg(buf, base, arg, format);

			break;
		}

		case jpiFilter:

			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 2));

			base = buf->len;
			next = write_jsp_header(buf, type);

			arg = reserve_jsp_offset(buf);

			write_jsp_arg(buf, base, arg, scm_c_list_ref(expr, 1));

			break;

		case jpiIndexArray: {

			SCM indices = scm_c_list_ref(expr, 1);
			SCM count = scm_length(indices); // TODO check -- guarded by validate-jsonpath
			int32 offset;

			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 2));

			base = buf->len;
			next = write_jsp_header(buf, type);

			write_jsp_uint32(buf, count);

			offset = reserve_jsp_offsets(buf, 2 * scm_to_uint32(count));

			while (indices != SCM_EOL) {

				SCM index = scm_car(indices);
				SCM from = scm_c_list_ref(index, 0);
				SCM to = scm_c_list_ref(index, 1);

				write_jsp_arg(buf, base, offset, from);
				offset += sizeof(uint32);

				if (to != SCM_BOOL_F)
					write_jsp_arg(buf, base, offset, to);

				offset += sizeof(uint32);

				indices = scm_cdr(indices);
			}
			break;
		}

		case jpiKey:

			scm_chained_expr_to_jsp(buf, scm_c_list_ref(expr, 2));
			next = write_jsp_header(buf, type);

			write_jsp_string(buf, scm_c_list_ref(expr, 1));

			break;

		case jpiLikeRegex:

			base = buf->len;
			next = write_jsp_header(buf, type);

			write_jsp_like_regex_flags(buf, scm_c_list_ref(expr, 3));

			arg = reserve_jsp_offset(buf);

			write_jsp_string(buf, scm_c_list_ref(expr, 2));
			write_jsp_arg(buf, base, arg, scm_c_list_ref(expr, 1));

			break;

		case jpiVariable:
			next = write_jsp_header(buf, type);
			write_jsp_string(buf, scm_c_list_ref(expr, 1));
			break;

		default:
			break;
	}

	return next;
}

JsonPathItemType jsp_expr_type(SCM expr)
{
	if (expr == SCM_EOL)
		return jpiNull;

	if (scm_is_pair(expr)) {

		SCM op = scm_car(expr);
		SCM type = scm_hash_ref(jsp_op_types_hash, op, SCM_BOOL_F);

		if (type == SCM_BOOL_F)
			elog(ERROR, "jsp_expr_type: unknown operator: %s", scm_to_string(op));

		return (JsonPathItemType)scm_to_int(type);
	}

	if (scm_is_bool(expr))
		return jpiBool;

	if (scm_is_number(expr) || is_decimal(expr))
		return jpiNumeric;

	if (scm_is_string(expr))
		return jpiString;

	elog(ERROR, "jsp_expr_type: unknown jsonpath expression: %s", scm_to_string(expr));
	// unreachable
	return 0;
}

void scm_chained_expr_to_jsp(StringInfo buf, SCM expr)
{
	int32 next = scm_expr_to_jsp(buf, expr);

	align_buffer(buf);
	*(int32 *)(buf->data + next) = buf->len - (next - sizeof(int32));
}

void align_buffer(StringInfo buf)
{
	uint32 size = INTALIGN(buf->len) - buf->len;

	if (size) {
		memset(buf->data, 0, size);
		buf->len += size;
	}
}

int32 reserve_jsp_offset(StringInfo buf)
{
	return reserve_jsp_offsets(buf, 1);
}

int32 reserve_jsp_offsets(StringInfo buf, uint32 count)
{
	int32 pos = buf->len;

	if (count > 0) {

		uint32 size = count * sizeof(uint32);

		enlargeStringInfo(buf, size);

		memset(buf->data + buf->len, 0, size);
		buf->len += size;
	}

	return pos;
}

void write_jsp_any_bounds(StringInfo buf, SCM expr)
{
	uint32 v = expr == SCM_BOOL_F ? PG_UINT32_MAX : scm_to_uint32(expr);
	appendBinaryStringInfo(buf, (char *)&v, sizeof(v));
}

void write_jsp_arg(StringInfo buf, int32 base, int32 offset, SCM expr)
{
	*(int32 *)(buf->data + offset) = buf->len - base;
	scm_expr_to_jsp(buf, expr);
}

void write_jsp_bool(StringInfo buf, SCM expr)
{
	bool b = scm_to_bool(expr);
	appendBinaryStringInfo(buf, (char *)&b, sizeof(b));
}

int32 write_jsp_header(StringInfo buf, JsonPathItemType type)
{
	appendStringInfoChar(buf, (char)type);

	/*
	 * We align buffer to int32 because a series of int32 values often goes
	 * after the header, and we want to read them directly by dereferencing
	 * int32 pointer (see jspInitByBuffer()).
	 */
	align_buffer(buf);

	/*
	 * Reserve space for next item pointer.  Actual value will be recorded
	 * later, after next and children items processing.
	 */
	return reserve_jsp_offset(buf);
}

void write_jsp_like_regex_flags(StringInfo buf, SCM flags) {

	uint32 regex_flags = 0;

	while (flags != SCM_EOL) {

		SCM flag = scm_car(flags);

		if (flag == dotall_symbol)
			regex_flags |= JSP_REGEX_DOTALL;

		else if (flag == icase_symbol)
			regex_flags |= JSP_REGEX_ICASE;

		else if (flag == mline_symbol)
			regex_flags |= JSP_REGEX_MLINE;

		else if (flag == quote_symbol)
			regex_flags |= JSP_REGEX_QUOTE;

		else if (flag == wspace_symbol)
			regex_flags |= JSP_REGEX_WSPACE;

		else
			elog(ERROR, "write_jsp_like_regex_flags: unrecognized flag: %s", scm_to_string(flag));

		flags = scm_cdr(flags);
	}

	appendBinaryStringInfo(buf, (char *)&regex_flags, sizeof(regex_flags));
}

void write_jsp_numeric(StringInfo buf, SCM expr)
{
	Datum datum = scm_to_datum_numeric(expr, InvalidOid);
	Numeric numeric = DatumGetNumeric(datum);
	appendBinaryStringInfo(buf, (char *)numeric, VARSIZE(numeric));
}

void write_jsp_string(StringInfo buf, SCM s)
{
	size_t len;
	char *c_str = scm_to_locale_stringn(s, &len);
	uint32 u32_len = len;

	appendBinaryStringInfo(buf, (char *)&u32_len, sizeof(u32_len));
	appendBinaryStringInfo(buf, c_str, u32_len);
	appendStringInfoChar(buf, '\0');

	free(c_str);
}

void write_jsp_uint32(StringInfo buf, SCM expr)
{
	uint32 v = scm_to_uint32(expr);
	appendBinaryStringInfo(buf, (char *)&v, sizeof(v));
}

SCM datum_tsquery_to_scm(Datum x, Oid type_oid)
{
	TSQuery query = DatumGetTSQuery(x);
	QueryItem *query_items = GETQUERY(query);
	char *operands = GETOPERAND(query);

	return call_1(make_tsquery_proc, tsquery_items_to_scm(&query_items, operands));
}

Datum scm_to_datum_tsquery(SCM x, Oid type_oid)
{
	int item_count = 0;
	size_t operand_bytes = 0;
	size_t alloc_size;

	SCM expr;
	TSQuery query;
	QueryItem *query_items;
	size_t offset = 0;

	if (!is_tsquery(x))
		elog(ERROR, "tsquery result expected, not: %s", scm_to_string(x));

	call_1(validate_tsquery_proc, x);

	expr = call_1(tsquery_expr_proc, x);

	tsquery_expr_size(expr, &item_count, &operand_bytes);

	alloc_size = COMPUTESIZE(item_count, operand_bytes);

	/* Pack the QueryItems in the final TSQuery struct to return to caller */
	query = (TSQuery) palloc0(alloc_size);
	SET_VARSIZE(query, alloc_size);
	query->size = item_count;

	query_items = GETQUERY(query);
	offset = 0;

	scm_to_tsquery_items(expr, &query_items, GETOPERAND(query), &offset);

	return TSQueryGetDatum(query);
}

void scm_to_tsquery_items(SCM expr, QueryItem **qi_iter, char *operands, size_t *offset)
{
	SCM op = scm_car(expr);

	if (op == value_symbol) {

		QueryOperand *operand = &(*qi_iter)->qoperand;
		size_t len;
		char *c_str = scm_to_locale_stringn(scm_cadr(expr), &len);
		SCM weight = scm_caddr(expr);
		SCM prefix = scm_cadddr(expr);

		pg_crc32 valcrc;

		operand->type = QI_VAL;
		operand->weight = scm_to_uint8(weight);
		operand->prefix = scm_to_bool(prefix);

		INIT_LEGACY_CRC32(valcrc);
		COMP_LEGACY_CRC32(valcrc, c_str, len);
		FIN_LEGACY_CRC32(valcrc);

		operand->valcrc = valcrc;

		operand->length = len;
		operand->distance = *offset;

		strncpy(operands + *offset, c_str, len+1);

		free(c_str);

		*offset += len+1;
		(*qi_iter)++;

	}
	else if (op == not_symbol) {

		QueryOperator *operator = &(*qi_iter)->qoperator;

		operator->type = QI_OPR;
		operator->oper = OP_NOT;
		operator->left = 1;

		(*qi_iter)++;

		scm_to_tsquery_items(scm_cadr(expr), qi_iter, operands, offset);

	}
	else if (op == and_symbol || op == or_symbol || op == phrase_symbol) {

		QueryOperator *operator = &(*qi_iter)->qoperator;
		QueryItem *op_item = *qi_iter;
		SCM left_expr = scm_cadr(expr);
		SCM right_expr = scm_caddr(expr);

		operator->type = QI_OPR;

		if (op != phrase_symbol)
			operator->oper = op == and_symbol ? OP_AND : OP_OR;

		else {
			operator->oper = OP_PHRASE;
			operator->distance = scm_to_int16(scm_cadddr(expr));
		}

		(*qi_iter)++;

		scm_to_tsquery_items(right_expr, qi_iter, operands, offset);

		operator->left = *qi_iter - op_item;

		scm_to_tsquery_items(left_expr, qi_iter, operands, offset);

	}
	else
		elog(ERROR, "scm_to_tsquery_items: unknown op; %s", scm_to_string(op));
}

void tsquery_expr_size(SCM expr, int *count, size_t *bytes)
{
	SCM op = scm_car(expr);

	if (op == value_symbol) {
		*bytes += scm_c_string_utf8_length(scm_cadr(expr))+1;
		*count += 1;
	}
	else if (op == not_symbol) {
		*count += 1;
		tsquery_expr_size(scm_cadr(expr), count, bytes);
	}
	else if (op == and_symbol || op == or_symbol || op == phrase_symbol) {
		*count += 1;
		tsquery_expr_size(scm_cadr(expr), count, bytes);
		tsquery_expr_size(scm_caddr(expr), count, bytes);
	}
	else
		elog(ERROR, "tsquery_expr_size: unknown op; %s", scm_to_string(op));
}


SCM tsquery_items_to_scm(QueryItem **qi_iter, char *operands)
{
	/* since this function recurses, it could be driven to stack overflow. */
	check_stack_depth();

	if ((*qi_iter)->type == QI_VAL) {

		QueryOperand *operand = &(*qi_iter)->qoperand;
		char *text = operands + operand->distance;

		SCM value = scm_from_locale_string(text);
		SCM weight = scm_from_uint8(operand->weight);
		SCM prefix = scm_from_bool(operand->prefix);

		(*qi_iter)++;

		return scm_list_4(value_symbol, value, weight, prefix);
	}
	else if ((*qi_iter)->qoperator.oper == OP_NOT) {

		(*qi_iter)++;

		return scm_list_2(not_symbol, tsquery_items_to_scm(qi_iter, operands));
	}
	else {

		QueryOperator *operator = &(*qi_iter)->qoperator;
		int8 op = operator->oper;
		SCM distance, left, right;

		(*qi_iter)++;

		right = tsquery_items_to_scm(qi_iter, operands);
		left = tsquery_items_to_scm(qi_iter, operands);

		switch (op)
		{
			case OP_OR:
				return scm_list_3(or_symbol, left, right);

			case OP_AND:
				return scm_list_3(and_symbol, left, right);

			case OP_PHRASE:
				distance = scm_from_uint16(operator->distance);
				return scm_list_4(phrase_symbol, left, right, distance);

			default:
				/* OP_NOT is handled in above if-branch */
				elog(ERROR, "unrecognized operator type: %d", op);
				return SCM_EOL;
		}
	}
}

SCM datum_tsvector_to_scm(Datum x, Oid type_oid)
{
    TSVector tsvector = DatumGetTSVector(x);
    WordEntry *entries = tsvector->entries;
    SCM tsvector_scm_list = SCM_EOL;
    SCM tslexeme_scm, tsposition_scm, tsvector_scm;
    int i, j;

    for (i = 0; i < tsvector->size; i++)
    {
	    char *lexeme = strndup(STRPTR(tsvector)+entries[i].pos, entries[i].len);
        SCM lexeme_scm = scm_from_locale_string(lexeme);
        SCM positions_scm_list = SCM_EOL;

        WordEntryPos *positions = POSDATAPTR(tsvector, &entries[i]);
        int num_positions = POSDATALEN(tsvector, &entries[i]);

        free(lexeme);

        for (j = 0; j < num_positions; j++)
        {
            int pos = WEP_GETPOS(positions[j]);
            char weight = WEP_GETWEIGHT(positions[j]);
            SCM pos_scm = scm_from_int(pos);
            SCM weight_scm = scm_from_int(weight);

            tsposition_scm = call_2(make_tsposition_proc, pos_scm, weight_scm);
            positions_scm_list = scm_append(scm_list_2(positions_scm_list, scm_list_1(tsposition_scm)));
        }

        tslexeme_scm = call_2(make_tslexeme_proc, lexeme_scm, positions_scm_list);
        tsvector_scm_list = scm_append(scm_list_2(tsvector_scm_list, scm_list_1(tslexeme_scm)));
    }

    tsvector_scm = call_1(make_tsvector_proc, tsvector_scm_list);

    return tsvector_scm;
}

Datum scm_to_datum_tsvector(SCM x, Oid type_oid)
{
	SCM n = call_1(normalize_tsvector_proc, x);
	size_t buffer_size = calculate_tsvector_buffer_size(n);
	SCM lexemes = call_1(tsvector_lexemes_proc, n);
	long lexeme_count = scm_ilength(lexemes);
	size_t alloc_size = CALCDATASIZE(lexeme_count, buffer_size);

	TSVector result;
	WordEntry *entries;
	char *buffer;
	size_t offset;

	if (buffer_size > MAXSTRPOS)
		ereport(
			ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg(
				 "string is too long for tsvector (%ld bytes, max %d bytes)",
				 buffer_size, MAXSTRPOS)));

	result = (TSVector) palloc0(alloc_size);
	SET_VARSIZE(result, alloc_size);
	result->size = lexeme_count;

	entries = ARRPTR(result);
	buffer = STRPTR(result);
	offset = 0;

	for (long i = 0; i < lexeme_count; i++) {
		SCM lexeme = scm_car(lexemes);
		SCM lexeme_str = call_1(tslexeme_lexeme_proc, lexeme);
		SCM positions = call_1(tslexeme_positions_proc, lexeme);

		size_t str_len = scm_c_string_utf8_length(lexeme_str);

		entries[i].pos = offset;
		entries[i].len = str_len;
		memcpy(buffer + offset, scm_to_locale_string(lexeme_str), str_len);

		offset += str_len;

		if (positions == SCM_EOL)
			entries[i].haspos = false;

		else {
			long position_count = scm_ilength(positions);

			if (position_count > 0xffff)
				elog(ERROR, "too many positions");

			entries[i].haspos = true;

			offset = SHORTALIGN(offset);
			*(uint16 *)(buffer + offset) = (uint16) position_count;
			offset += sizeof(uint16);

			while (positions != SCM_EOL) {
				SCM position = scm_car(positions);
				WordEntryPos *wep = (WordEntryPos *)(buffer + offset);

				WEP_SETPOS(*wep, get_tsposition_index(position));
				WEP_SETWEIGHT(*wep, get_tsposition_weight(position));

				offset += sizeof(WordEntryPos);
				positions = scm_cdr(positions);
			}
		}

		lexemes = scm_cdr(lexemes);
	}

	return TSVectorGetDatum(result);
}

uint16 get_tsposition_index(SCM position)
{
	uint16 index = scm_to_uint16(call_1(tsposition_index_proc, position));

	if (index > 0x3fff)
		elog(ERROR, "get_tsposition_index: index out of range: %d > %d", index, 0x3fff);

	return index;
}

uint16 get_tsposition_weight(SCM position)
{
	uint16 weight = scm_to_uint16(call_1(tsposition_weight_proc, position));

	if (weight > 3)
		elog(ERROR, "get_tsposition_weight: weight out of range: %d > %d", weight, 3);

	return weight;
}

size_t calculate_tsvector_buffer_size(SCM x)
{
	SCM lexemes = call_1(tsvector_lexemes_proc, x);
	size_t buflen = 0;

	while (lexemes != SCM_EOL) {
		SCM lexeme = scm_car(lexemes);
		long pos_count = scm_ilength(call_1(tslexeme_positions_proc, lexeme));

		buflen += scm_c_string_utf8_length(call_1(tslexeme_lexeme_proc, lexeme));
		buflen = SHORTALIGN(buflen);
		buflen += pos_count * sizeof(WordEntryPos) + sizeof(uint16);

		lexemes = scm_cdr(lexemes);
	}

	return buflen;
}

SCM datum_range_to_scm(Datum x, Oid type_oid)
{
	RangeType *range = DatumGetRangeTypeP(x);
	TypeCacheEntry *typcache = lookup_type_cache(type_oid, TYPECACHE_RANGE_INFO);
	Oid subtype_oid = get_range_subtype(type_oid);

	RangeBound lower, upper;
	bool empty;
	char flags;

	SCM scm_flags, scm_lower, scm_upper;

	range_deserialize(typcache, range, &lower, &upper, &empty);
	flags = range_get_flags(range);

	scm_flags = range_flags_to_scm(flags);

	scm_lower = range_bound_to_scm(&lower, subtype_oid);
	scm_upper = range_bound_to_scm(&upper, subtype_oid);

	return call_3(make_range_proc, scm_lower, scm_upper, scm_flags);
}

SCM range_flags_to_scm(char flags)
{
	SCM result = SCM_EOL;

	if (flags) {
		if (flags & RANGE_EMPTY)  result = scm_cons(empty_symbol, result);
		if (flags & RANGE_LB_INC) result = scm_cons(lower_inclusive_symbol, result);
		if (flags & RANGE_LB_INF) result = scm_cons(lower_infinite_symbol, result);
		if (flags & RANGE_UB_INC) result = scm_cons(upper_inclusive_symbol, result);
		if (flags & RANGE_UB_INF) result = scm_cons(upper_infinite_symbol, result);
	}

	return result;
}

SCM range_bound_to_scm(const RangeBound *bound, Oid subtype_oid)
{
	return bound->infinite ? empty_symbol : datum_to_scm(bound->val, subtype_oid);
}

Datum scm_to_datum_range(SCM x, Oid type_oid)
{
	TypeCacheEntry *typcache = lookup_type_cache(type_oid, TYPECACHE_RANGE_INFO);
	Oid subtype_oid = get_range_subtype(type_oid);

	RangeBound lower, upper;
	char flags;

	SCM scm_lower, scm_upper;

	RangeType *range;

	if (!is_range(x))
		elog(ERROR, "range result expected, not: %s", scm_to_string(x));

	flags = scm_range_flags_to_char(x);

	lower.lower = true;
	upper.lower = false;

	scm_lower = call_1(range_lower_proc, x);
	scm_upper = call_1(range_upper_proc, x);

	lower.inclusive = flags & RANGE_LB_INC;
	upper.inclusive = flags & RANGE_UB_INC;

	lower.infinite = scm_lower == empty_symbol;
	upper.infinite = scm_upper == empty_symbol;

	if (!lower.infinite)
		lower.val = scm_to_datum(scm_lower, subtype_oid);

	if (!upper.infinite)
		upper.val = scm_to_datum(scm_upper, subtype_oid);

	range = range_serialize(typcache, &lower, &upper, flags & RANGE_EMPTY);

	return RangeTypePGetDatum(range);
}

char scm_range_flags_to_char(SCM range)
{

	SCM scm_flags = call_1(range_flags_proc, range);

	char flags = 0;

	while (scm_flags != SCM_EOL) {

		SCM flag = scm_car(scm_flags);

		if (flag == empty_symbol)
			flags |= RANGE_EMPTY;

		else if (flag == lower_inclusive_symbol)
			flags |= RANGE_LB_INC;

		else if (flag == lower_infinite_symbol)
			flags |= RANGE_LB_INF;

		else if (flag == upper_inclusive_symbol)
			flags |= RANGE_UB_INC;

		else if (flag == upper_infinite_symbol)
			flags |= RANGE_UB_INF;

		else
			elog(ERROR, "scm_range_flags_to_char: unrecognized flag: %s", scm_to_string(flag));

		scm_flags = scm_cdr(scm_flags);
	}

	return flags;
}

SCM datum_multirange_to_scm(Datum x, Oid type_oid)
{
	Oid range_type_oid = get_multirange_range(type_oid);
	TypeCacheEntry *rangetyp = lookup_type_cache(range_type_oid, TYPECACHE_RANGE_INFO);

	MultirangeType *range = DatumGetMultirangeTypeP(x);
	int32 range_count;
	RangeType **ranges;

	SCM scm_ranges = SCM_EOL;

	multirange_deserialize(rangetyp, range, &range_count, &ranges);

	for (int i = range_count-1; i >= 0; i--) {
		SCM scm_range = datum_range_to_scm(RangeTypePGetDatum(ranges[i]), range_type_oid);
		scm_ranges = scm_cons(scm_range, scm_ranges);
	}

	return call_1(make_multirange_proc, scm_ranges);
}

Datum scm_to_datum_multirange(SCM x, Oid type_oid)
{
	if (!is_multirange(x))
		elog(ERROR, "multirange result expected, not: %s", scm_to_string(x));

	else {
		Oid range_type_oid = get_multirange_range(type_oid);
		TypeCacheEntry *rangetyp = lookup_type_cache(range_type_oid, TYPECACHE_RANGE_INFO);

		SCM scm_ranges = call_1(multirange_ranges_proc, x);
		long range_count = scm_ilength(scm_ranges);

		RangeType **ranges;
		MultirangeType *multirange;

		if (range_count <= 0)
			elog(ERROR, "multirange must be a list of ranges: %s", scm_to_string(scm_ranges));

		ranges = palloc(range_count * sizeof(RangeType *));

		for (long i = 0; i < range_count; i++) {
			Datum range = scm_to_datum_range(scm_car(scm_ranges), range_type_oid);
			ranges[i] = DatumGetRangeTypeP(range);
			scm_ranges = scm_cdr(scm_ranges);
		}

		multirange = make_multirange(type_oid, rangetyp, range_count, ranges);

		return MultirangeTypePGetDatum(multirange);
	}
}

SCM datum_void_to_scm(Datum x, Oid type_oid)
{
	return SCM_UNDEFINED;
}

Datum scm_to_datum_void(SCM x, Oid type_oid)
{
	return (Datum) 0;
}

Oid get_boxed_datum_type(SCM x)
{
	return scm_to_uint32(call_1(boxed_datum_type_proc, x));
}

Datum get_boxed_datum_value(SCM x)
{
	return scm_to_uint64(call_1(boxed_datum_value_proc, x));
}

bool is_bit_string(SCM x)
{
	return scm_is_true(call_1(is_bit_string_proc, x));
}

bool is_box(SCM x)
{
	return scm_is_true(call_1(is_box_proc, x));
}

bool is_boxed_datum(SCM x)
{
	return scm_is_true(call_1(is_boxed_datum_proc, x));
}

bool is_circle(SCM x)
{
	return scm_is_true(call_1(is_circle_proc, x));
}

bool is_cursor(SCM x)
{
	return scm_is_true(call_1(is_cursor_proc, x));
}

bool is_date(SCM x)
{
	return scm_is_true(call_1(is_date_proc, x));
}

bool is_decimal(SCM x)
{
	return scm_is_true(call_1(is_decimal_proc, x));
}

bool is_inet(SCM x)
{
	return scm_is_true(call_1(is_inet_proc, x));
}

bool is_int2(SCM x)
{
	return scm_is_true(call_1(is_int2_proc, x));
}

bool is_int4(SCM x)
{
	return scm_is_true(call_1(is_int4_proc, x));
}

bool is_int8(SCM x)
{
	return scm_is_true(call_1(is_int8_proc, x));
}

bool is_jsonb(SCM x)
{
	return scm_is_true(call_1(is_jsonb_proc, x));
}

bool is_jsonpath(SCM x)
{
	return scm_is_true(call_1(is_jsonpath_proc, x));
}

bool is_line(SCM x)
{
	return scm_is_true(call_1(is_line_proc, x));
}

bool is_lseg(SCM x)
{
	return scm_is_true(call_1(is_lseg_proc, x));
}

bool is_macaddr(SCM x)
{
	return scm_is_true(call_1(is_macaddr_proc, x));
}

bool is_macaddr8(SCM x)
{
	return scm_is_true(call_1(is_macaddr8_proc, x));
}

bool is_multirange(SCM x)
{
	return scm_is_true(call_1(is_multirange_proc, x));
}

bool is_path(SCM x)
{
	return scm_is_true(call_1(is_path_proc, x));
}

bool is_point(SCM x)
{
	return scm_is_true(call_1(is_point_proc, x));
}

bool is_polygon(SCM x)
{
	return scm_is_true(call_1(is_polygon_proc, x));
}

bool is_range(SCM x)
{
	return scm_is_true(call_1(is_range_proc, x));
}

bool is_record(SCM x)
{
	return scm_is_true(call_1(is_record_proc, x));
}

bool is_table(SCM x)
{
	return scm_is_true(call_1(is_table_proc, x));
}

bool is_time(SCM x)
{
	return scm_is_true(call_1(is_time_proc, x));
}

bool is_tsquery(SCM x)
{
	return scm_is_true(call_1(is_tsquery_proc, x));
}

bool is_tsvector(SCM x)
{
	return scm_is_true(call_1(is_tsvector_proc, x));
}

Datum datum_date_to_timestamptz(Datum x)
{
	DateADT date = DatumGetDateADT(x);
	TimestampTz timestamp = date2timestamptz_opt_overflow(date, NULL);
	return TimestampTzGetDatum(timestamp);
}

SCM scm_c_list_ref(SCM obj, size_t k)
{
	return scm_list_ref(obj, scm_from_size_t(k));
}

char *scm_to_string(SCM obj)
{
	MemoryContext context = CurrentMemoryContext;

	SCM proc = scm_eval_string(scm_from_locale_string("(lambda (x) (format #f \"~s\" x))"));
	SCM str_scm = call_1(proc, obj);

	// Convert SCM string to C string and allocate it in the given memory context
	size_t len;
	char *c_str = scm_to_locale_stringn(str_scm, &len);
	char *result = MemoryContextStrdup(context, c_str);

	free(c_str);  // Free temporary string

	return result;
}

void insert_range_cache_entry(Oid subtype_oid, Oid range_type_oid, Oid multirange_type_oid)
{
	bool found;
	RangeCacheEntry *entry;

	entry = (RangeCacheEntry *)hash_search(range_cache, &subtype_oid, HASH_ENTER, &found);

	if (found)
		elog(ERROR, "Unexpected duplicate in range cache: %d", subtype_oid);

	entry->subtype_oid = subtype_oid;
	entry->range_type_oid = range_type_oid;
	entry->multirange_type_oid = multirange_type_oid;
}

void insert_type_cache_entry(Oid type_oid, ToScmFunc to_scm, ToDatumFunc to_datum)
{
	bool found;
	TypeConvCacheEntry *entry;

	entry = (TypeConvCacheEntry *)hash_search(type_cache, &type_oid, HASH_ENTER, &found);

	if (found)
		elog(ERROR, "Unexpected duplicate in type cache: %d", type_oid);

	entry->type_oid = type_oid;
	entry->to_scm = to_scm;
	entry->to_datum = to_datum;
}

////////////////////////////////////////////////////////////////////////////////////////////////
//
// SPI Integration
//

SCM spi_execute(SCM command, SCM args, SCM count)
{
	int ret;
	SCM rows_processed;
	SCM table;

	if (!scm_is_string(command))
		throw_wrong_argument_type("command", "string", command);

	if (!scm_is_integer(count) || scm_to_bool(scm_negative_p(count)))
		throw_wrong_argument_type("count", "non-negative integer", count);

	ret = SPI_connect();

	if (ret < 0)
		throw_runtime_error("SPI_connect failed: %s", SPI_result_code_string(ret));

	PG_TRY();
	{
		if (args == SCM_EOL)
			ret = SPI_execute(
				scm_to_locale_string(command),
				current_volatility != PROVOLATILE_VOLATILE,
				scm_to_long(count));
		else {
			long nargs = scm_ilength(args);
			Oid *arg_types;
			Datum *arg_values;
			char *arg_nulls;

			SCM rest = args;

			if (nargs < 0)
				throw_wrong_argument_type("args", "list", args);

			arg_types = (Oid *)palloc(sizeof(Oid) * nargs);
			arg_values = (Datum *)palloc(sizeof(Datum) * nargs);
			arg_nulls = (char *)palloc0(sizeof(char) * nargs);

			for (long i = 0; i < nargs; i++) {
				SCM arg = scm_car(rest);
				Oid type_oid = infer_scm_type_oid(arg);

				if (type_oid == InvalidOid)
					elog(ERROR, "spi_execute: unable to infer result type");

				arg_types[i] = type_oid;
				arg_values[i] = scm_to_datum(arg, type_oid);
				// TODO support NULL specification
				// arg_nulls[i] = 1; set to 'n' for null
				rest = scm_cdr(rest);
			}

			ret = SPI_execute_with_args(
				scm_to_locale_string(command), nargs, arg_types, arg_values, arg_nulls,
				current_volatility != PROVOLATILE_VOLATILE, scm_to_long(count));
		}
	}
	PG_CATCH();
	{
		handle_execute_error();
	}
	PG_END_TRY();

	if (ret < 0) {
		SPI_finish();
		throw_runtime_error("SPI_execute failed: %s", SPI_result_code_string(ret));
	}

	switch (ret) {
	case SPI_OK_SELECT:
	case SPI_OK_INSERT_RETURNING:
	case SPI_OK_DELETE_RETURNING:
	case SPI_OK_UPDATE_RETURNING: {

		TupleDesc tuple_desc;
		SCM attr_names, attr_names_hash, records, type_names;

		tuple_desc = SPI_tuptable->tupdesc;

		attr_names = SCM_EOL;
		type_names = SCM_EOL;
		attr_names_hash = scm_c_make_hash_table(tuple_desc->natts);

		for (int col = tuple_desc->natts-1; col >= 0; col--) {
			Oid att_type_oid = tuple_desc->attrs[col].atttypid;
			SCM symbol = scm_from_locale_symbol(NameStr(tuple_desc->attrs[col].attname));
			attr_names = scm_cons(symbol, attr_names);
			type_names = scm_cons(type_desc_expr(att_type_oid), type_names);
			scm_hash_set_x(attr_names_hash, symbol, scm_from_int(col));
		}

		records = SCM_EOL;

		for (long row = SPI_tuptable->numvals-1; row >= 0 ; row--) {

			HeapTuple tuple = SPI_tuptable->vals[row];
			SCM attrs = scm_c_make_vector(tuple_desc->natts, SCM_EOL);
			SCM record;

			for (int col = 0; col < tuple_desc->natts; col++) {

				bool is_null;
				Datum datum = SPI_getbinval(tuple, tuple_desc, col+1, &is_null);
				Oid type_oid = tuple_desc->attrs[col].atttypid;

				if (!is_null)
					scm_c_vector_set_x(attrs, col, datum_to_scm(datum, type_oid));
			}

			record = call_4(make_record_proc, type_names, attrs, attr_names, attr_names_hash);
			records = scm_cons(record, records);
		}

		table = call_4(make_table_proc, type_names, records, attr_names, attr_names_hash);

		break;
	}
	default:
		table = SCM_BOOL_F;
		break;
	}

	rows_processed = scm_from_int64(SPI_processed);

	SPI_finish();
	return scm_values(scm_list_2(table, rows_processed));
}

void throw_wrong_argument_type(const char *param_name, const char *type, SCM v)
{
	SCM error_symbol = scm_from_utf8_symbol("wrong-argument-type");
	SCM arg_name_symbol = scm_from_utf8_symbol(param_name);
	SCM expected_type_desc = scm_from_locale_string(type);
	SCM args = scm_list_3(arg_name_symbol, expected_type_desc, v);
	scm_throw(error_symbol, args);
}

void throw_runtime_error(const char *format, ...)
{
	va_list vargs;
	char buffer[1000]; // TODO const

	SCM error_symbol = scm_from_utf8_symbol("runtime-error");
	SCM message, args;

	va_start(vargs, format);
	vsnprintf(buffer, sizeof(buffer), format, vargs);
	elog(NOTICE, "throw_runtime_error: %s", buffer);

	message = scm_from_locale_string(buffer);
	args = scm_list_1(message);
	scm_throw(error_symbol, args);
}

void throw_execute_error(ErrorData *edata)
{
	SCM error_symbol = scm_from_utf8_symbol("execute-error");

	SCM args = scm_list_4(
		scm_from_locale_string(unpack_sql_state(edata->sqlerrcode)),
		edata->message ? scm_from_locale_string(edata->message) : SCM_BOOL_F,
		edata->detail  ? scm_from_locale_string(edata->detail)  : SCM_BOOL_F,
		edata->hint    ? scm_from_locale_string(edata->hint)    : SCM_BOOL_F);

	scm_throw(error_symbol, args);
}

SCM stop_command_execution()
{
	return stop_marker;
}

SCM spi_execute_with_receiver(SCM receiver_proc, SCM command, SCM args, SCM count)
{
	Receiver dest = {
		{ dest_receive, dest_startup, dest_shutdown, dest_destroy, DestNone },
		receiver_proc,
		SCM_EOL,
		SCM_EOL,
	};

	ParamListInfo param_list;
	ParamExternData *param_ptr;

	long nargs = scm_ilength(args);
	Oid *arg_types = NULL;

	int ret;
	SPIExecuteOptions options = {0};

	if (nargs < 0)
		throw_wrong_argument_type("args", "list", args);

	if (scm_procedure_p(receiver_proc) == SCM_BOOL_F)
		throw_wrong_argument_type("receiver", "procedure", receiver_proc);

	if (!scm_is_string(command))
		throw_wrong_argument_type("command", "string", command);

	if (!scm_is_integer(count) || scm_to_bool(scm_negative_p(count)))
		throw_wrong_argument_type("count", "non-negative integer", count);

	param_list = (ParamListInfo)palloc0(sizeof(ParamListInfoData) + nargs * sizeof(ParamExternData));
	param_list->numParams = nargs;
	param_ptr = (ParamExternData *)&param_list->params;

	if (nargs) {

		SCM rest = args;

		arg_types = (Oid *)palloc(sizeof(Oid) * nargs);

		for (long i = 0; i < nargs; i++) {
			SCM arg = scm_car(rest);
			Oid type_oid = infer_scm_type_oid(arg);

			if (type_oid == InvalidOid)
				elog(ERROR, "spi_execute_with_receiver: unable to infer result type");

			arg_types[i] = type_oid;
			param_ptr->value = scm_to_datum(arg, type_oid);
			param_ptr->isnull = 0; // set to 'n' for null
			rest = scm_cdr(rest);
			param_ptr++;
		}
	}

	options.allow_nonatomic = true;
	options.tcount          = scm_to_long(count);
	options.dest            = (DestReceiver *)&dest;
	options.owner           = NULL;
	options.params          = param_list;
	options.read_only       = current_volatility != PROVOLATILE_VOLATILE;

	dest.result = dest.tail = scm_cons(SCM_EOL, SCM_EOL);

	ret = SPI_connect();

	if (ret < 0)
		throw_runtime_error("SPI_connect failed: %s", SPI_result_code_string(ret));

	PG_TRY();
	{
		SPIPlanPtr plan = SPI_prepare(scm_to_locale_string(command), nargs, arg_types);
		SPI_execute_plan_extended(plan, &options);
	}
	PG_CATCH();
	{
		handle_execute_error();
	}
	PG_END_TRY();

	SPI_finish();

	return scm_cdr(dest.result);
}

void handle_execute_error(void)
{
	ErrorData *edata = CopyErrorData();
	SPI_finish();
	AbortCurrentTransaction();
	StartTransactionCommand();
	throw_execute_error(edata);
}

void dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
}

bool dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	Receiver *receiver = (Receiver *)self;
	SCM proc = receiver->proc;
	SCM args = SCM_EOL;
	FormData_pg_attribute *attrs = slot->tts_tupleDescriptor->attrs;
	SCM result, tail;

	slot_getallattrs(slot);

	for (int i = slot->tts_nvalid-1; i >= 0; i--) {

		Datum datum = slot->tts_values[i];
		bool is_null = slot->tts_isnull[i];
		Oid type_oid = attrs[i].atttypid;

		args = scm_cons(is_null ? SCM_EOL : datum_to_scm(datum, type_oid), args);
	}

	result = apply_in_sandbox(proc, args);

	if (result == stop_marker)
		return false;

	tail = scm_cons(result, SCM_EOL);
	scm_set_cdr_x(receiver->tail, tail);
	receiver->tail = tail;

	return true;
}

void dest_shutdown(DestReceiver *self)
{
}

void dest_destroy(DestReceiver *self)
{
}

SCM spi_cursor_open(SCM command, SCM args, SCM count, SCM hold, SCM name, SCM scroll)
{
	Portal portal;
	ParamListInfo param_list;
	SPIParseOpenOptions options = {0};

	ParamExternData *param_ptr;

	long nargs = scm_ilength(args);
	Oid *arg_types = NULL;

	int ret;
	SCM cursor;

	if (nargs < 0)
		throw_wrong_argument_type("args", "list", args);

	if (!scm_is_string(command))
		throw_wrong_argument_type("command", "string", command);

	param_list = (ParamListInfo)palloc0(sizeof(ParamListInfoData) + nargs * sizeof(ParamExternData));
	param_list->numParams = nargs;
	param_ptr = (ParamExternData *)&param_list->params;

	if (nargs) {

		SCM rest = args;

		arg_types = (Oid *)palloc(sizeof(Oid) * nargs);

		for (long i = 0; i < nargs; i++) {
			SCM arg = scm_car(rest);
			Oid type_oid = infer_scm_type_oid(arg);

			if (type_oid == InvalidOid)
				elog(ERROR, "spi_cursor_open: unable to infer result type");

			arg_types[i] = type_oid;
			param_ptr->value = scm_to_datum(arg, type_oid);
			param_ptr->isnull = 0; // set to 'n' for null
			rest = scm_cdr(rest);
			param_ptr++;
		}
	}

	options.params        = param_list;
	options.read_only     = current_volatility != PROVOLATILE_VOLATILE;
	options.cursorOptions = CURSOR_OPT_BINARY;

	ret = SPI_connect();

	if (ret < 0)
		throw_runtime_error("SPI_connect failed: %s", SPI_result_code_string(ret));

	PG_TRY();
	{
		portal = SPI_cursor_parse_open(
			scm_string_to_pstr(name), scm_string_to_pstr(command), &options);
	}
	PG_CATCH();
	{
		handle_execute_error();
	}
	PG_END_TRY();

	cursor = call_1(make_cursor_proc, scm_from_locale_string(portal->name));

	SPI_finish();

	return cursor;
}

char *scm_string_to_pstr(SCM s)
{
	if (s == SCM_EOL || s == SCM_BOOL_F)
		return NULL;
	else {
		char *cstr = scm_to_locale_string(s);
		char *pstr = pstrdup(cstr);
		free(cstr);
		return pstr;
	}
}

SCM spi_cursor_fetch(SCM cursor, SCM direction, SCM count)
{
	const char *name;
	int ret;
	Portal portal;

	TupleDesc tuple_desc;
	SCM attr_names, attr_names_hash, records, type_names;

	if (!is_cursor(cursor))
		throw_wrong_argument_type("cursor", "cursor", cursor);

	name = scm_to_locale_string(call_1(cursor_name_proc, cursor));

	ret = SPI_connect();

	if (ret < 0)
		throw_runtime_error("SPI_connect failed: %s", SPI_result_code_string(ret));

	portal = SPI_cursor_find(name);

	if (portal == NULL) {
		SPI_finish();
		throw_runtime_error("no such cursor: %s", name);
	}

	SPI_scroll_cursor_fetch(portal, scm_to_fetch_direction(direction), scm_to_fetch_count(count));

	tuple_desc = SPI_tuptable->tupdesc;

	attr_names = SCM_EOL;
	type_names = SCM_EOL;
	attr_names_hash = scm_c_make_hash_table(tuple_desc->natts);

	for (int col = tuple_desc->natts-1; col >= 0; col--) {
		Oid att_type_oid = tuple_desc->attrs[col].atttypid;
		SCM symbol = scm_from_locale_symbol(NameStr(tuple_desc->attrs[col].attname));
		attr_names = scm_cons(symbol, attr_names);
		type_names = scm_cons(type_desc_expr(att_type_oid), type_names);
		scm_hash_set_x(attr_names_hash, symbol, scm_from_int(col));
	}

	records = SCM_EOL;

	for (long row = SPI_tuptable->numvals-1; row >= 0 ; row--) {

		HeapTuple tuple = SPI_tuptable->vals[row];
		SCM attrs = scm_c_make_vector(tuple_desc->natts, SCM_EOL);
		SCM record;

		for (int col = 0; col < tuple_desc->natts; col++) {

			bool is_null;
			Datum datum = SPI_getbinval(tuple, tuple_desc, col+1, &is_null);
			Oid type_oid = tuple_desc->attrs[col].atttypid;

			if (!is_null)
				scm_c_vector_set_x(attrs, col, datum_to_scm(datum, type_oid));
		}

		record = call_4(make_record_proc, type_names, attrs, attr_names, attr_names_hash);
		records = scm_cons(record, records);
	}

	SPI_finish();

	return records;
}

SCM spi_cursor_move(SCM cursor, SCM direction, SCM count)
{
	const char *name = (const char *)scm_foreign_object_ref(cursor, 0);
	int ret;
	Portal portal;

	ret = SPI_connect();

	if (ret < 0)
		throw_runtime_error("SPI_connect failed: %s", SPI_result_code_string(ret));

	portal = SPI_cursor_find(name);

	if (portal == NULL) {
		SPI_finish();
		throw_runtime_error("no such cursor: %s", name);
	}

	SPI_scroll_cursor_move(portal, scm_to_fetch_direction(direction), scm_to_fetch_count(count));

	SPI_finish();

	return SCM_EOL;
}

FetchDirection scm_to_fetch_direction(SCM direction)
{
	if (direction == forward_symbol)
		return FETCH_FORWARD;

	if (direction == backward_symbol)
		return FETCH_BACKWARD;

	if (direction == absolute_symbol)
		return FETCH_ABSOLUTE;

	if (direction == relative_symbol)
		return FETCH_RELATIVE;

	elog(ERROR, "scm_to_fetch_direction: unknown direction: %s", scm_to_string(direction));
	// unreachable
	return FETCH_FORWARD;
}

long scm_to_fetch_count(SCM count)
{
	if (count == all_symbol)
		return FETCH_ALL;

	return scm_to_long(count);
}

Oid infer_scm_type_oid(SCM x)
{
	if (scm_is_number(x)) {

		if (!scm_is_exact(x))
			return TypenameGetTypid("float8");

		else {

			if (is_int2(x))
				return TypenameGetTypid("int2");

			if (is_int4(x))
				return TypenameGetTypid("int4");

			if (is_int8(x))
				return TypenameGetTypid("int8");

			if (scm_is_rational(x) && scm_to_int(scm_denominator(x)) > 1)
				return TypenameGetTypid("float8");

			return TypenameGetTypid("numeric");
		}
	}

	if (scm_is_string(x))
		return TypenameGetTypid("text");

	if (x == SCM_BOOL_F || x == SCM_BOOL_T)
		return TypenameGetTypid("bool");

	if (is_boxed_datum(x))
		return get_boxed_datum_type(x);

	if (scm_is_pair(x) && scm_is_symbol(scm_car(x)) && scm_is_symbol(scm_cdr(x)))
		return find_enum_datum_type(scm_car(x));

	if (scm_is_vector(x))
		return infer_array_type_oid(x);

	if (is_bit_string(x))
		return TypenameGetTypid("bit");

	if (is_box(x))
		return TypenameGetTypid("box");

	if (scm_is_bytevector(x))
		return TypenameGetTypid("bytea");

	if (is_circle(x))
		return TypenameGetTypid("circle");

	if (is_date(x))
		return TypenameGetTypid("timestamptz");

	if (is_decimal(x))
		return TypenameGetTypid("numeric");

	if (is_inet(x))
		return TypenameGetTypid("inet");

	if (is_line(x))
		return TypenameGetTypid("line");

	if (is_lseg(x))
		return TypenameGetTypid("lseg");

	if (is_macaddr(x))
		return TypenameGetTypid("macaddr");

	if (is_macaddr8(x))
		return TypenameGetTypid("macaddr8");

	if (is_path(x))
		return TypenameGetTypid("path");

	if (is_point(x))
		return TypenameGetTypid("point");

	if (is_polygon(x))
		return TypenameGetTypid("polygon");

	if (is_record(x))
		return TypenameGetTypid("record");

	if (is_time(x))
		return TypenameGetTypid("time");

	if (is_tsquery(x))
		return TypenameGetTypid("tsquery");

	if (is_tsvector(x))
		return TypenameGetTypid("tsvector");

	if (is_jsonpath(x))
		return TypenameGetTypid("jsonpath");

	if (is_range(x))
		return infer_range_type_oid(x);

	if (is_multirange(x))
		return infer_multirange_type_oid(x);

	return InvalidOid;
}

Oid infer_array_type_oid(SCM x)
{
	Oid element_type_oid = InvalidOid;
	int len = scm_c_vector_length(x);

	for (int i = 0; i < len; ++i) {

		Oid ith_type_oid = infer_scm_type_oid(scm_c_vector_ref(x, i));

		if (element_type_oid == InvalidOid)
			element_type_oid = ith_type_oid;

		if (element_type_oid == ith_type_oid)
			continue;

		element_type_oid = unify_type_oid(element_type_oid, ith_type_oid);

		if (element_type_oid == InvalidOid)
			return InvalidOid;
	}

	if (element_type_oid == InvalidOid)
		return InvalidOid;

	return get_array_type(element_type_oid);
}

Oid unify_type_oid(Oid t1, Oid t2)
{
	if (t1 == int2_oid) {
		if (t2 == int2_oid || t2 == int4_oid || t2 == int8_oid || t2 == numeric_oid)
			return t2;
	}
	else if (t1 == int4_oid) {
		if (t2 == int2_oid)
			return t1;

		if (t2 == int4_oid || t2 == int8_oid || t2 == numeric_oid)
			return t2;
	}
	else if (t1 == int8_oid) {
		if (t2 == int2_oid || t2 == int4_oid || t2 == int8_oid)
			return t1;

		if (t2 == numeric_oid)
			return t2;
	}
	else if (t1 == numeric_oid) {
		if (t2 == int2_oid || t2 == int4_oid || t2 == int8_oid || t2 == numeric_oid)
			return t1;
	}
	else if (t1 == float4_oid) {
		if (t2 == float8_oid)
			return t2;
	}
	else if (t1 == float8_oid) {
		if (t2 == float4_oid)
			return t1;
	}

	return InvalidOid;
}

Oid find_enum_datum_type(SCM type_name)
{
	char *type_name_cstr = scm_to_locale_string(scm_symbol_to_string(type_name));
	Oid type_oid = TypenameGetTypid(type_name_cstr);
	free(type_name_cstr);
	return type_oid;
}

Oid infer_range_type_oid(SCM x)
{
	SCM lower, upper;
	Oid lower_oid, upper_oid;
	Oid multirange_type_oid, range_type_oid, subtype_oid;

	RangeCacheEntry *entry;
	bool found;

	lower = call_1(range_lower_proc, x);
	upper = call_1(range_upper_proc, x);

	lower_oid = infer_scm_type_oid(lower);
	upper_oid = infer_scm_type_oid(upper);

	if (lower == empty_symbol) {
		if (upper == empty_symbol)
			// empty range
			return int4range_oid;

		else
			subtype_oid = upper_oid;
	}
	else {
		if (upper == empty_symbol)
			subtype_oid = lower_oid;

		else
			subtype_oid = unify_type_oid(lower_oid, upper_oid);
	}

	if (subtype_oid == InvalidOid)
		elog(ERROR, "infer_range_type_oid: unable to infer range type for %s", scm_to_string(x));

	entry = (RangeCacheEntry *)hash_search(range_cache, &subtype_oid, HASH_FIND, &found);

	if (found)
		return entry->range_type_oid;

	if (!find_range_type_oid(subtype_oid, &range_type_oid, &multirange_type_oid))
		elog(ERROR, "infer_range_type_oid: unable to infer range type for %s", scm_to_string(x));

	insert_range_cache_entry(subtype_oid, range_type_oid, multirange_type_oid);

	return range_type_oid;
}

bool find_range_type_oid(Oid subtype_oid, Oid *range_type_oid, Oid *multirange_type_oid)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    bool found = false;

    rel = table_open(RangeRelationId, AccessShareLock);

    scan = table_beginscan_catalog(rel, 0, NULL);

    while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        Form_pg_range range_form = (Form_pg_range) GETSTRUCT(tup);
        if (range_form->rngsubtype == subtype_oid)
        {
            *range_type_oid = range_form->rngtypid;
            *multirange_type_oid = range_form->rngmultitypid;

            found = true;
            break;
        }
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    return found;
}

Oid infer_multirange_type_oid(SCM x)
{
	SCM ranges = call_1(multirange_ranges_proc, x);

	Oid subtype_oid = InvalidOid;

	RangeCacheEntry *entry;
	bool found;

	Oid multirange_type_oid, range_type_oid;

	while (ranges != SCM_EOL) {

		SCM range = scm_car(ranges);

		SCM lower, upper;
		Oid lower_oid, upper_oid;

		lower = call_1(range_lower_proc, range);
		upper = call_1(range_upper_proc, range);

		lower_oid = infer_scm_type_oid(lower);
		upper_oid = infer_scm_type_oid(upper);

		if (lower != empty_symbol) {

			if (lower_oid == InvalidOid)
				elog(ERROR, "infer_multirange_type_oid: invalid range: %s", scm_to_string(range));

			subtype_oid = unify_range_subtype_oid(lower_oid, subtype_oid);

			if (subtype_oid == InvalidOid)
				elog(ERROR, "infer_multirange_type_oid: unable to infer multirange type for: %s", scm_to_string(x));
		}

		if (upper != empty_symbol) {

			if (upper_oid == InvalidOid)
				elog(ERROR, "infer_multirange_type_oid: invalid range: %s", scm_to_string(range));

			subtype_oid = unify_range_subtype_oid(upper_oid, subtype_oid);

			if (subtype_oid == InvalidOid)
				elog(ERROR, "infer_multirange_type_oid: unable to infer multirange type for: %s", scm_to_string(x));
		}

		ranges = scm_cdr(ranges);
	}

	if (subtype_oid == InvalidOid)
		subtype_oid = int4_oid;

	entry = (RangeCacheEntry *)hash_search(range_cache, &subtype_oid, HASH_FIND, &found);

	if (found)
		return entry->multirange_type_oid;

	if (!find_range_type_oid(subtype_oid, &range_type_oid, &multirange_type_oid))
		elog(ERROR, "infer_multirange_type_oid: unable to infer multirange type for %s", scm_to_string(x));

	insert_range_cache_entry(subtype_oid, range_type_oid, multirange_type_oid);

	return multirange_type_oid;
}

Oid unify_range_subtype_oid(Oid t1, Oid t2)
{
	if (t1 == InvalidOid)
		return t2;

	if (t2 == InvalidOid)
		return t1;

	return unify_type_oid(t1, t2);
}
