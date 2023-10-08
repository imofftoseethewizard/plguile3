#include <libguile.h>

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <access/genam.h>
#include <access/htup_details.h>
#include <access/relation.h>
#include <catalog/namespace.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_enum.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <lib/stringinfo.h>
#include <mb/pg_wchar.h>
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
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <utils/uuid.h>
#include <utils/varbit.h>
#include <utils/xml.h>

#define NS_PER_USEC 1000
#define SECS_PER_MONTH (DAYS_PER_MONTH * SECS_PER_DAY)

// Most datum-scm conversion functions accept but do not use the oid parameter. This is a
// convenience for calling one of these where the appropriate type oid is not available.
#define OID_NOT_USED ((Oid)0)

// Defines const char *src_scruple_scm.
#include "scruple.scm.h"

PG_MODULE_MAGIC;

PGDLLEXPORT Datum scruple_call(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum scruple_call_inline(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum scruple_compile(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(scruple_call);
PG_FUNCTION_INFO_V1(scruple_call_inline);
PG_FUNCTION_INFO_V1(scruple_compile);

void _PG_init(void);
void _PG_fini(void);

typedef struct {
    Oid func_oid;
    SCM scm_proc;
} FuncCacheEntry;

typedef SCM (*ToScmFunc)(Datum, Oid);
typedef Datum (*ToDatumFunc)(SCM, Oid);

typedef struct TypeCacheEntry {
    Oid type_oid;          // OID of a PostgreSQL type
    ToScmFunc to_scm;      // Function pointer to convert Datum to SCM
    ToDatumFunc to_datum;  // Function pointer to convert SCM to Datum
} TypeCacheEntry;

static SCM make_boxed_datum(Oid type_oid, Datum x);

static SCM datum_bit_string_to_scm(Datum x, Oid type_oid);
static SCM datum_bool_to_scm(Datum x, Oid type_oid);
static SCM datum_box_to_scm(Datum x, Oid type_oid);
static SCM datum_bytea_to_scm(Datum x, Oid type_oid);
static SCM datum_circle_to_scm(Datum x, Oid type_oid);
static SCM datum_date_to_scm(Datum x, Oid type_oid);
//static SCM datum_enum_to_scm(Datum x, Oid type_oid);
static SCM datum_float4_to_scm(Datum x, Oid type_oid);
static SCM datum_float8_to_scm(Datum x, Oid type_oid);
static SCM datum_inet_to_scm(Datum x, Oid type_oid);
static SCM datum_int2_to_scm(Datum x, Oid type_oid);
static SCM datum_int4_to_scm(Datum x, Oid type_oid);
static SCM datum_int8_to_scm(Datum x, Oid type_oid);
static SCM datum_interval_to_scm(Datum x, Oid type_oid);
static SCM datum_json_to_scm(Datum x, Oid type_oid);
static SCM datum_jsonb_to_scm(Datum x, Oid type_oid);
static SCM datum_line_to_scm(Datum x, Oid type_oid);
static SCM datum_lseg_to_scm(Datum x, Oid type_oid);
static SCM datum_macaddr8_to_scm(Datum x, Oid type_oid);
static SCM datum_macaddr_to_scm(Datum x, Oid type_oid);
static SCM datum_numeric_to_scm(Datum x, Oid type_oid);
static SCM datum_path_to_scm(Datum x, Oid type_oid);
static SCM datum_point_to_scm(Datum x, Oid type_oid);
static SCM datum_polygon_to_scm(Datum x, Oid type_oid);
static SCM datum_text_to_scm(Datum x, Oid type_oid);
static SCM datum_time_to_scm(Datum x, Oid type_oid);
static SCM datum_timetz_to_scm(Datum x, Oid type_oid);
static SCM datum_timestamptz_to_scm(Datum x, Oid type_oid);
static SCM datum_uuid_to_scm(Datum x, Oid type_oid);
static SCM datum_void_to_scm(Datum x, Oid type_oid);
static SCM datum_xml_to_scm(Datum x, Oid type_oid);

static Datum scm_to_datum_bit_string(SCM x, Oid type_oid);
static Datum scm_to_datum_bool(SCM x, Oid type_oid);
static Datum scm_to_datum_box(SCM x, Oid type_oid);
static Datum scm_to_datum_bytea(SCM x, Oid type_oid);
static Datum scm_to_datum_circle(SCM x, Oid type_oid);
static Datum scm_to_datum_date(SCM x, Oid type_oid);
//static Datum scm_to_datum_enum(SCM x, Oid type_oid);
static Datum scm_to_datum_float4(SCM x, Oid type_oid);
static Datum scm_to_datum_float8(SCM x, Oid type_oid);
static Datum scm_to_datum_inet(SCM x, Oid type_oid);
static Datum scm_to_datum_int2(SCM x, Oid type_oid);
static Datum scm_to_datum_int4(SCM x, Oid type_oid);
static Datum scm_to_datum_int8(SCM x, Oid type_oid);
static Datum scm_to_datum_interval(SCM x, Oid type_oid);
static Datum scm_to_datum_json(SCM x, Oid type_oid);
static Datum scm_to_datum_jsonb(SCM x, Oid type_oid);
static Datum scm_to_datum_line(SCM x, Oid type_oid);
static Datum scm_to_datum_lseg(SCM x, Oid type_oid);
static Datum scm_to_datum_macaddr(SCM x, Oid type_oid);
static Datum scm_to_datum_macaddr8(SCM x, Oid type_oid);
static Datum scm_to_datum_numeric(SCM x, Oid type_oid);
static Datum scm_to_datum_path(SCM x, Oid type_oid);
static Datum scm_to_datum_point(SCM x, Oid type_oid);
static Datum scm_to_datum_polygon(SCM x, Oid type_oid);
static Datum scm_to_datum_text(SCM x, Oid type_oid);
static Datum scm_to_datum_time(SCM x, Oid type_oid);
static Datum scm_to_datum_timetz(SCM x, Oid type_oid);
static Datum scm_to_datum_timestamptz(SCM x, Oid type_oid);
static Datum scm_to_datum_uuid(SCM x, Oid type_oid);
static Datum scm_to_datum_void(SCM x, Oid type_oid);
static Datum scm_to_datum_xml(SCM x, Oid type_oid);

static Datum datum_date_to_timestamptz(Datum x);
static char* scm_to_string(SCM x);

static Oid get_boxed_datum_type(SCM x);
static Datum get_boxed_datum_value(SCM x);
static bool is_bit_string(SCM x);
static bool is_box(SCM x);
static bool is_boxed_datum(SCM x);
static bool is_circle(SCM x);
static bool is_date(SCM x);
static bool is_decimal(SCM x);
static bool is_valid_decimal(SCM x);
static bool is_inet(SCM x);
static bool is_int2(SCM x);
static bool is_int4(SCM x);
static bool is_int8(SCM x);
static bool is_line(SCM x);
static bool is_lseg(SCM x);
static bool is_macaddr(SCM x);
static bool is_macaddr8(SCM x);
static bool is_path(SCM x);
static bool is_point(SCM x);
static bool is_polygon(SCM x);
static bool is_time(SCM x);

static void insert_type_cache_entry(Oid type_oid, ToScmFunc to_scm, ToDatumFunc to_datum);
//static SCM datum_to_scm(Datum datum, Oid type_oid);
static Datum scm_to_datum(SCM scm, Oid type_oid);
static SCM scruple_compile_func(Oid func_oid);
static Datum convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo);
static Datum convert_boxed_datum_to_datum(SCM scm, Oid target_type_oid);
static Datum convert_result_to_composite_datum(SCM result, TupleDesc tuple_desc);
//static bool is_enum_type(Oid type_oid);
static Datum jsonb_from_cstring(char *json, int len);


static SCM bit_string_data_proc;
static SCM bit_string_length_proc;
static SCM box_a_proc;
static SCM box_b_proc;
static SCM boxed_datum_type_proc;
static SCM boxed_datum_value_proc;
static SCM circle_center_proc;
static SCM circle_radius_proc;
static SCM date_day_proc;
static SCM date_hour_proc;
static SCM date_minute_proc;
static SCM date_month_proc;
static SCM date_nanosecond_proc;
static SCM date_second_proc;
static SCM date_year_proc;
static SCM date_zone_offset_proc;
static SCM decimal_digits_proc;
static SCM decimal_scale_proc;
static SCM decimal_to_inexact_proc;
static SCM decimal_to_string_proc;
static SCM inet_address_proc;
static SCM inet_bits_proc;
static SCM inet_family_proc;
static SCM is_bit_string_proc;
static SCM is_box_proc;
static SCM is_boxed_datum_proc;
static SCM is_circle_proc;
static SCM is_date_proc;
static SCM is_decimal_proc;
static SCM is_valid_decimal_proc;
static SCM is_inet_proc;
static SCM is_int2_proc;
static SCM is_int4_proc;
static SCM is_int8_proc;
static SCM is_line_proc;
static SCM is_lseg_proc;
static SCM is_macaddr8_proc;
static SCM is_macaddr_proc;
static SCM is_path_proc;
static SCM is_point_proc;
static SCM is_polygon_proc;
static SCM is_time_proc;
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
static SCM make_date_proc;
static SCM make_decimal_proc;
static SCM make_inet_proc;
static SCM make_line_proc;
static SCM make_lseg_proc;
static SCM make_macaddr8_proc;
static SCM make_macaddr_proc;
static SCM make_path_proc;
static SCM make_point_proc;
static SCM make_polygon_proc;
static SCM make_time_proc;
static SCM path_is_closed_proc;
static SCM path_points_proc;
static SCM point_x_proc;
static SCM point_y_proc;
static SCM polygon_boundbox_proc;
static SCM polygon_points_proc;
static SCM string_to_decimal_proc;
static SCM time_duration_symbol;
static SCM time_monotonic_symbol;
static SCM time_nanosecond_proc;
static SCM time_second_proc;

static HTAB *funcCache;
static HTAB *typeCache;

void _PG_init(void) {

    HASHCTL funcInfo;
    HASHCTL typeInfo;

    // TODO: check that `SHOW server_encoding` is 'UTF8'

    /* Initialize hash tables */
    memset(&funcInfo, 0, sizeof(funcInfo));
    funcInfo.keysize = sizeof(Oid);
    funcInfo.entrysize = sizeof(FuncCacheEntry);
    funcCache = hash_create("Scruple Function Cache", 128, &funcInfo, HASH_ELEM | HASH_BLOBS);

    memset(&typeInfo, 0, sizeof(typeInfo));
    typeInfo.keysize = sizeof(Oid);
    typeInfo.entrysize = sizeof(TypeCacheEntry);
    typeCache = hash_create("Scruple Type Cache", 128, &typeInfo, HASH_ELEM | HASH_BLOBS);

    /* Fill type cache with to_scm and to_datum functions for known types */
    insert_type_cache_entry(TypenameGetTypid("bit"),         datum_bit_string_to_scm,  scm_to_datum_bit_string);
    insert_type_cache_entry(TypenameGetTypid("bool"),        datum_bool_to_scm,        scm_to_datum_bool);
    insert_type_cache_entry(TypenameGetTypid("box"),         datum_box_to_scm,         scm_to_datum_box);
    insert_type_cache_entry(TypenameGetTypid("bpchar"),      datum_text_to_scm,        scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("bytea"),       datum_bytea_to_scm,       scm_to_datum_bytea);
    insert_type_cache_entry(TypenameGetTypid("char"),        datum_text_to_scm,        scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("cidr"),        datum_inet_to_scm,        scm_to_datum_inet);
    insert_type_cache_entry(TypenameGetTypid("circle"),      datum_circle_to_scm,      scm_to_datum_circle);
    insert_type_cache_entry(TypenameGetTypid("date"),        datum_date_to_scm,        scm_to_datum_date);
    insert_type_cache_entry(TypenameGetTypid("float4"),      datum_float4_to_scm,      scm_to_datum_float4);
    insert_type_cache_entry(TypenameGetTypid("float8"),      datum_float8_to_scm,      scm_to_datum_float8);
    insert_type_cache_entry(TypenameGetTypid("inet"),        datum_inet_to_scm,        scm_to_datum_inet);
    insert_type_cache_entry(TypenameGetTypid("int2"),        datum_int2_to_scm,        scm_to_datum_int2);
    insert_type_cache_entry(TypenameGetTypid("int4"),        datum_int4_to_scm,        scm_to_datum_int4);
    insert_type_cache_entry(TypenameGetTypid("int8"),        datum_int8_to_scm,        scm_to_datum_int8);
    insert_type_cache_entry(TypenameGetTypid("interval"),    datum_interval_to_scm,    scm_to_datum_interval);
    insert_type_cache_entry(TypenameGetTypid("json"),        datum_json_to_scm,        scm_to_datum_json);
    insert_type_cache_entry(TypenameGetTypid("jsonb"),       datum_jsonb_to_scm,       scm_to_datum_jsonb);
    insert_type_cache_entry(TypenameGetTypid("line"),        datum_line_to_scm,        scm_to_datum_line);
    insert_type_cache_entry(TypenameGetTypid("lseg"),        datum_lseg_to_scm,        scm_to_datum_lseg);
    insert_type_cache_entry(TypenameGetTypid("macaddr"),     datum_macaddr_to_scm,     scm_to_datum_macaddr);
    insert_type_cache_entry(TypenameGetTypid("macaddr8"),    datum_macaddr8_to_scm,    scm_to_datum_macaddr8);
    insert_type_cache_entry(TypenameGetTypid("money"),       datum_int8_to_scm,        scm_to_datum_int8);
    insert_type_cache_entry(TypenameGetTypid("numeric"),     datum_numeric_to_scm,     scm_to_datum_numeric);
    insert_type_cache_entry(TypenameGetTypid("path"),        datum_path_to_scm,        scm_to_datum_path);
    insert_type_cache_entry(TypenameGetTypid("point"),       datum_point_to_scm,       scm_to_datum_point);
    insert_type_cache_entry(TypenameGetTypid("polygon"),     datum_polygon_to_scm,     scm_to_datum_polygon);
    insert_type_cache_entry(TypenameGetTypid("text"),        datum_text_to_scm,        scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("time"),        datum_time_to_scm,        scm_to_datum_time);
    insert_type_cache_entry(TypenameGetTypid("timetz"),      datum_timetz_to_scm,      scm_to_datum_timetz);
    insert_type_cache_entry(TypenameGetTypid("timestamp"),   datum_timestamptz_to_scm, scm_to_datum_timestamptz);
    insert_type_cache_entry(TypenameGetTypid("timestamptz"), datum_timestamptz_to_scm, scm_to_datum_timestamptz);
    insert_type_cache_entry(TypenameGetTypid("uuid"),        datum_uuid_to_scm,        scm_to_datum_uuid);
    insert_type_cache_entry(TypenameGetTypid("varbit"),      datum_bit_string_to_scm,  scm_to_datum_bit_string);
    insert_type_cache_entry(TypenameGetTypid("varchar"),     datum_text_to_scm,        scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("void"),        datum_void_to_scm,        scm_to_datum_void);
    insert_type_cache_entry(TypenameGetTypid("xml"),         datum_xml_to_scm,         scm_to_datum_xml);

    /* Initialize the Guile interpreter */
    scm_init_guile();

    scm_eval_string(scm_from_locale_string((const char *)src_scruple_scm));

    /* Define names in our scheme module for the type oids we work with. */

    scm_c_define("bit-type-oid",         scm_from_int(TypenameGetTypid("bit")));
    scm_c_define("bool-type-oid",        scm_from_int(TypenameGetTypid("bool")));
    scm_c_define("box-type-oid",         scm_from_int(TypenameGetTypid("box")));
    scm_c_define("bpchar-type-oid",      scm_from_int(TypenameGetTypid("bpchar")));
    scm_c_define("bytea-type-oid",       scm_from_int(TypenameGetTypid("bytea")));
    scm_c_define("char-type-oid",        scm_from_int(TypenameGetTypid("char")));
    scm_c_define("cidr-type-oid",        scm_from_int(TypenameGetTypid("cidr")));
    scm_c_define("circle-type-oid",      scm_from_int(TypenameGetTypid("circle")));
    scm_c_define("date-type-oid",        scm_from_int(TypenameGetTypid("date")));
    scm_c_define("float4-type-oid",      scm_from_int(TypenameGetTypid("float4")));
    scm_c_define("float8-type-oid",      scm_from_int(TypenameGetTypid("float8")));
    scm_c_define("inet-type-oid",        scm_from_int(TypenameGetTypid("inet")));
    scm_c_define("int2-type-oid",        scm_from_int(TypenameGetTypid("int2")));
    scm_c_define("int4-type-oid",        scm_from_int(TypenameGetTypid("int4")));
    scm_c_define("int8-type-oid",        scm_from_int(TypenameGetTypid("int8")));
    scm_c_define("interval-type-oid",    scm_from_int(TypenameGetTypid("interval")));
    scm_c_define("json-type-oid",        scm_from_int(TypenameGetTypid("json")));
    scm_c_define("jsonb-type-oid",       scm_from_int(TypenameGetTypid("jsonb")));
    scm_c_define("line-type-oid",        scm_from_int(TypenameGetTypid("line")));
    scm_c_define("lseg-type-oid",        scm_from_int(TypenameGetTypid("lseg")));
    scm_c_define("macaddr-type-oid",     scm_from_int(TypenameGetTypid("macaddr")));
    scm_c_define("macaddr8-type-oid",    scm_from_int(TypenameGetTypid("macaddr8")));
    scm_c_define("money-type-oid",       scm_from_int(TypenameGetTypid("money")));
    scm_c_define("numeric-type-oid",     scm_from_int(TypenameGetTypid("numeric")));
    scm_c_define("path-type-oid",        scm_from_int(TypenameGetTypid("path")));
    scm_c_define("point-type-oid",       scm_from_int(TypenameGetTypid("point")));
    scm_c_define("polygon-type-oid",     scm_from_int(TypenameGetTypid("polygon")));
    scm_c_define("text-type-oid",        scm_from_int(TypenameGetTypid("text")));
    scm_c_define("time-type-oid",        scm_from_int(TypenameGetTypid("time")));
    scm_c_define("timetz-type-oid",      scm_from_int(TypenameGetTypid("timetz")));
    scm_c_define("timestamp-type-oid",   scm_from_int(TypenameGetTypid("timestamp")));
    scm_c_define("timestamptz-type-oid", scm_from_int(TypenameGetTypid("timestamptz")));
    scm_c_define("uuid-type-oid",        scm_from_int(TypenameGetTypid("uuid")));
    scm_c_define("varbit-type-oid",      scm_from_int(TypenameGetTypid("varbit")));
    scm_c_define("varchar-type-oid",     scm_from_int(TypenameGetTypid("varchar")));
    scm_c_define("void-type-oid",        scm_from_int(TypenameGetTypid("void")));
    scm_c_define("xml-type-oid",         scm_from_int(TypenameGetTypid("xml")));

    /* Procedures defined by define-record-type are inlinable, meaning that instead of being
       procedures, they are actually syntax transformers.  In non-call contexts, they refer to
       the original procedure, but in call contexts, they interpolate the body of the
       procedure. The upshot to this is that using scm_c_lookup gets the syntax transfomer
       procedure, not the inlined procedure. To fix this, we just eval the name, inducing the
       syntax transformer to give us proc we need.
    */
    bit_string_data_proc   = scm_eval_string(scm_from_locale_string("bit-string-data"));
    bit_string_length_proc = scm_eval_string(scm_from_locale_string("bit-string-length"));
    box_a_proc             = scm_eval_string(scm_from_locale_string("box-a"));
    box_b_proc             = scm_eval_string(scm_from_locale_string("box-b"));
    boxed_datum_type_proc  = scm_eval_string(scm_from_locale_string("boxed-datum-type"));
    boxed_datum_value_proc = scm_eval_string(scm_from_locale_string("boxed-datum-value"));
    circle_center_proc     = scm_eval_string(scm_from_locale_string("circle-center"));
    circle_radius_proc     = scm_eval_string(scm_from_locale_string("circle-radius"));
    date_day_proc          = scm_eval_string(scm_from_locale_string("date-day"));
    date_hour_proc         = scm_eval_string(scm_from_locale_string("date-hour"));
    date_minute_proc       = scm_eval_string(scm_from_locale_string("date-minute"));
    date_month_proc        = scm_eval_string(scm_from_locale_string("date-month"));
    date_nanosecond_proc   = scm_eval_string(scm_from_locale_string("date-nanosecond"));
    date_second_proc       = scm_eval_string(scm_from_locale_string("date-second"));
    date_year_proc         = scm_eval_string(scm_from_locale_string("date-year"));
    date_zone_offset_proc  = scm_eval_string(scm_from_locale_string("date-zone-offset"));
    decimal_digits_proc    = scm_eval_string(scm_from_locale_string("decimal-digits"));
    decimal_scale_proc     = scm_eval_string(scm_from_locale_string("decimal-scale"));
    inet_address_proc      = scm_eval_string(scm_from_locale_string("inet-address"));
    inet_bits_proc         = scm_eval_string(scm_from_locale_string("inet-bits"));
    inet_family_proc       = scm_eval_string(scm_from_locale_string("inet-family"));
    is_bit_string_proc     = scm_eval_string(scm_from_locale_string("bit-string?"));
    is_box_proc            = scm_eval_string(scm_from_locale_string("box?"));
    is_boxed_datum_proc    = scm_eval_string(scm_from_locale_string("boxed-datum?"));
    is_circle_proc         = scm_eval_string(scm_from_locale_string("circle?"));
    is_date_proc           = scm_eval_string(scm_from_locale_string("date?"));
    is_decimal_proc        = scm_eval_string(scm_from_locale_string("decimal?"));
    is_valid_decimal_proc  = scm_eval_string(scm_from_locale_string("valid-decimal?"));
    is_inet_proc           = scm_eval_string(scm_from_locale_string("inet?"));
    is_line_proc           = scm_eval_string(scm_from_locale_string("line?"));
    is_lseg_proc           = scm_eval_string(scm_from_locale_string("lseg?"));
    is_macaddr8_proc       = scm_eval_string(scm_from_locale_string("macaddr8?"));
    is_macaddr_proc        = scm_eval_string(scm_from_locale_string("macaddr?"));
    is_path_proc           = scm_eval_string(scm_from_locale_string("path?"));
    is_point_proc          = scm_eval_string(scm_from_locale_string("point?"));
    is_polygon_proc        = scm_eval_string(scm_from_locale_string("polygon?"));
    is_time_proc           = scm_eval_string(scm_from_locale_string("time?"));
    line_a_proc            = scm_eval_string(scm_from_locale_string("line-a"));
    line_b_proc            = scm_eval_string(scm_from_locale_string("line-b"));
    line_c_proc            = scm_eval_string(scm_from_locale_string("line-c"));
    lseg_a_proc            = scm_eval_string(scm_from_locale_string("lseg-a"));
    lseg_b_proc            = scm_eval_string(scm_from_locale_string("lseg-b"));
    macaddr8_data_proc     = scm_eval_string(scm_from_locale_string("macaddr8-data"));
    macaddr_data_proc      = scm_eval_string(scm_from_locale_string("macaddr-data"));
    make_bit_string_proc   = scm_eval_string(scm_from_locale_string("make-bit-string"));
    make_box_proc          = scm_eval_string(scm_from_locale_string("make-box"));
    make_boxed_datum_proc  = scm_eval_string(scm_from_locale_string("make-boxed-datum"));
    make_circle_proc       = scm_eval_string(scm_from_locale_string("make-circle"));
    make_date_proc         = scm_eval_string(scm_from_locale_string("make-date"));
    make_decimal_proc      = scm_eval_string(scm_from_locale_string("make-decimal"));
    make_inet_proc         = scm_eval_string(scm_from_locale_string("make-inet"));
    make_line_proc         = scm_eval_string(scm_from_locale_string("make-line"));
    make_lseg_proc         = scm_eval_string(scm_from_locale_string("make-lseg"));
    make_macaddr8_proc     = scm_eval_string(scm_from_locale_string("make-macaddr8"));
    make_macaddr_proc      = scm_eval_string(scm_from_locale_string("make-macaddr"));
    make_path_proc         = scm_eval_string(scm_from_locale_string("make-path"));
    make_point_proc        = scm_eval_string(scm_from_locale_string("make-point"));
    make_polygon_proc      = scm_eval_string(scm_from_locale_string("make-polygon"));
    make_time_proc         = scm_eval_string(scm_from_locale_string("make-time"));
    path_is_closed_proc    = scm_eval_string(scm_from_locale_string("path-closed?"));
    path_points_proc       = scm_eval_string(scm_from_locale_string("path-points"));
    point_x_proc           = scm_eval_string(scm_from_locale_string("point-x"));
    point_y_proc           = scm_eval_string(scm_from_locale_string("point-y"));
    polygon_boundbox_proc  = scm_eval_string(scm_from_locale_string("polygon-boundbox"));
    polygon_points_proc    = scm_eval_string(scm_from_locale_string("polygon-points"));
    time_duration_symbol   = scm_eval_string(scm_from_locale_string("time-duration"));
    time_monotonic_symbol  = scm_eval_string(scm_from_locale_string("time-monotonic"));
    time_nanosecond_proc   = scm_eval_string(scm_from_locale_string("time-nanosecond"));
    time_second_proc       = scm_eval_string(scm_from_locale_string("time-second"));

    decimal_to_string_proc  = scm_variable_ref(scm_c_lookup("decimal->string"));
    decimal_to_inexact_proc = scm_variable_ref(scm_c_lookup("decimal->inexact"));
    is_int2_proc            = scm_variable_ref(scm_c_lookup("int2-compatible?"));
    is_int4_proc            = scm_variable_ref(scm_c_lookup("int4-compatible?"));
    is_int8_proc            = scm_variable_ref(scm_c_lookup("int8-compatible?"));
    string_to_decimal_proc  = scm_variable_ref(scm_c_lookup("string->decimal"));

}

void _PG_fini(void) {

    /* Clean up Guile interpreter */
    HASH_SEQ_STATUS status;
    FuncCacheEntry *entry;

    hash_seq_init(&status, funcCache);

    while ((entry = (FuncCacheEntry *) hash_seq_search(&status)) != NULL) {
        scm_gc_unprotect_object(entry->scm_proc);
    }

    hash_seq_term(&status);

    /* Clean up hash tables */
    hash_destroy(funcCache);
    hash_destroy(typeCache);
}

Datum scruple_call(PG_FUNCTION_ARGS) {

    Oid func_oid = fcinfo->flinfo->fn_oid;
    HeapTuple proc_tuple;
    SCM proc;

    FuncCacheEntry entry;

    bool found;
    FuncCacheEntry *hash_entry;

    SCM arg_list = SCM_EOL;

    Datum result;

    elog(NOTICE, "scruple_call: begin");

    // Find compiled code for function in the cache.
    entry.func_oid = func_oid;
    entry.scm_proc = SCM_EOL;

    hash_entry = (FuncCacheEntry *)hash_search(funcCache,
                                               (void *)&entry.func_oid,
                                               HASH_ENTER,
                                               &found);

    if (found) {
        proc = hash_entry->scm_proc;
    }
    else {
        // Not found, compile and store in cache
        proc = entry.scm_proc = scruple_compile_func(func_oid);
        scm_gc_protect_object(entry.scm_proc);
        memcpy(hash_entry, &entry, sizeof(entry));
    }

    // Fetch tuple from pg_proc for the given function Oid
    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "scruple_call: failed to fetch function details.");

    // Loop through all arguments, convert them to SCM and add them to
    // the list.
    for (int i = PG_NARGS()-1; i >= 0; i--) {

        Datum arg = PG_GETARG_DATUM(i);
        Oid arg_type = get_fn_expr_argtype(fcinfo->flinfo, i);

        // SCM scm_arg = datum_to_scm(arg, arg_type);
        SCM scm_arg = make_boxed_datum(arg_type, arg);

        arg_list = scm_cons(scm_arg, arg_list);
    }

    elog(NOTICE, "x");
    result = convert_result_to_datum(scm_apply(proc, arg_list, SCM_EOL), proc_tuple, fcinfo);

    ReleaseSysCache(proc_tuple);

    PG_RETURN_DATUM(result);
}

SCM
make_boxed_datum(Oid type_oid, Datum x) {
    return scm_call_2(make_boxed_datum_proc, scm_from_int32(type_oid), scm_from_int64(x));
}

Datum
convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo) {

    TypeFuncClass typefunc_class;
    Oid rettype_oid;
    TupleDesc tuple_desc;

    elog(NOTICE, "x");
    typefunc_class = get_call_result_type(fcinfo, &rettype_oid, &tuple_desc);

    elog(NOTICE, "convert_result_to_datum: return type oid: %d", rettype_oid);

    switch (typefunc_class) {

    case TYPEFUNC_SCALAR:
        elog(NOTICE, "x");
        return scm_to_datum(result, rettype_oid);

    case TYPEFUNC_COMPOSITE:
        return convert_result_to_composite_datum(result, tuple_desc);

    case TYPEFUNC_COMPOSITE_DOMAIN: /* domain over determinable rowtype result */
    case TYPEFUNC_RECORD:	    /* indeterminate rowtype result            */
    case TYPEFUNC_OTHER:	    /* bogus type, eg pseudotype               */
        elog(ERROR, "convert_result_to_datum: not implemented");
        return (Datum) 0;

    default:
        elog(ERROR, "convert_result_to_datum: unknown TypeFuncClass value: %d", typefunc_class);
        return (Datum) 0;
    }
}

Datum
convert_result_to_composite_datum(SCM result, TupleDesc tuple_desc) {

    HeapTuple ret_heap_tuple;

    Datum *ret_values;
    bool *ret_is_null;

    Datum result_datum;

    if (scm_c_nvalues(result) != tuple_desc->natts)
        elog(ERROR, "convert_result_to_datum: num values %zu does not equal num out params %d", scm_c_nvalues(result), tuple_desc->natts);

    ret_values = (Datum *) palloc0(sizeof(Datum) * tuple_desc->natts);
    ret_is_null = (bool *) palloc0(sizeof(bool) * tuple_desc->natts);

    for (int i = 0; i < tuple_desc->natts; ++i) {

        Form_pg_attribute attr = TupleDescAttr(tuple_desc, i);
        SCM v = scm_c_value_ref(result, i);

        // TODO: handle attr->atttypmod
        elog(NOTICE, "convert_result_to_datum: slot %d, attr->atttypid: %d", i, attr->atttypid);
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

Datum scruple_call_inline(PG_FUNCTION_ARGS) {
    /* Handle an inline Guile statement */
    elog(NOTICE, "scruple_call_inline: not implemented");
    PG_RETURN_NULL();
}


Datum scruple_compile(PG_FUNCTION_ARGS) {
    Oid func_oid = PG_GETARG_OID(0);

    FuncCacheEntry entry;
    bool found;
    FuncCacheEntry *hash_entry;

    entry.func_oid = func_oid;
    entry.scm_proc = scruple_compile_func(func_oid);
    scm_gc_protect_object(entry.scm_proc);

    hash_entry = (FuncCacheEntry *)hash_search(funcCache,
                                               (void *)&entry.func_oid,
                                               HASH_ENTER,
                                               &found);

    if (found) {
        // Unprotect the previously compiled function and replace the
        // existing compiled function with the new one.
        scm_gc_unprotect_object(hash_entry->scm_proc);
        hash_entry->scm_proc = entry.scm_proc;
    }
    else {
        // Save the compiled function in the hash table.
        memcpy(hash_entry, &entry, sizeof(entry));
    }

    return (Datum) 1;
}

SCM scruple_compile_func(Oid func_oid) {

    HeapTuple proc_tuple;
    Datum prosrc_datum;
    char *prosrc;
    bool is_null;

    Form_pg_proc proc_struct;
    char *proc_name;

    Datum argnames_datum, argmodes_datum;

    ArrayType *argnames_array, *argmodes_array;
    int num_args, num_modes, i;

    char *argmodes;

    StringInfoData buf;
    SCM scm_proc;

    elog(NOTICE, "scruple_compile: begin");

    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "scruple_compile: Failed to fetch function details.");

    initStringInfo(&buf);

    proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
    SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proname, &is_null);

    if (!is_null) {
        proc_name = NameStr(proc_struct->proname);
        // TODO check that the function name is a proper scheme identifier
        elog(NOTICE, "scruple_compile: function name: %s", proc_name);

        appendStringInfo(&buf, "(define (%s", proc_name);
    }
    else {
        ereport(ERROR, (errmsg("scruple_compile: func with oid %d has null name", func_oid)));
    }

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
                elog(ERROR, "scruple_compile: num arg modes %d and num args %d differ", num_modes, num_args);
            }
        }

        elog(NOTICE, "scruple_compile: num args: %d", num_args);
        for (i = 1; i <= num_args; i++) {
            if (argmodes == NULL || argmodes[i-1] != 'o') {
                Datum name_datum = array_get_element(argnames_datum, 1, &i, -1, -1, false, 'i', &is_null);
                if (!is_null) {
                    char *name = TextDatumGetCString(name_datum);
                    elog(NOTICE, "scruple_compile: parameter name: %s", name);
                    // TODO check that the parameter name is a proper scheme identifier
                    appendStringInfo(&buf, " %s", name);
                }
                else {
                    elog(NOTICE, "scruple_compile: %d: name_datum is null", i);
                }
            }
        }
    }

    prosrc_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &is_null);
    prosrc = TextDatumGetCString(prosrc_datum);

    if (is_null) {
        elog(ERROR, "scruple_compile: source datum is null.");
    }

    appendStringInfo(&buf, ")\n%s)", prosrc);

    elog(NOTICE, "scruple_compile: compiling source:\n%s", buf.data);

    scm_eval_string(scm_from_locale_string(buf.data));
    scm_proc = scm_variable_ref(scm_c_lookup(proc_name));

    elog(NOTICE, "scruple_compile: complete");

    ReleaseSysCache(proc_tuple);

    return scm_proc;
}

/* SCM */
/* datum_to_scm(Datum datum, Oid type_oid) { */

/*     bool found; */
/*     TypeCacheEntry *entry = (TypeCacheEntry *) hash_search( */
/*         typeCache, */
/*         &type_oid, */
/*         HASH_FIND, */
/*         &found); */

/*     if (found && entry->to_scm) { */
/*         return entry->to_scm(datum, type_oid); */
/*     } */
/*     else { */
/*         if (is_enum_type(type_oid)) { */
/*             insert_type_cache_entry(type_oid, datum_enum_to_scm, scm_to_datum_enum); */
/*             return datum_enum_to_scm(datum, type_oid); */
/*         } */

/*         elog(ERROR, "Conversion function for type OID %u not found", type_oid); */
/*         // Unreachable */
/*         return SCM_EOL; */
/*     } */
/* } */

Datum
scm_to_datum(SCM scm, Oid type_oid) {

    bool found;
    TypeCacheEntry *entry;

    elog(NOTICE, "x");
    if (is_boxed_datum(scm))
        return convert_boxed_datum_to_datum(scm, type_oid);

    entry = (TypeCacheEntry *)hash_search(typeCache, &type_oid, HASH_FIND, &found);

    if (found && entry->to_datum)
        return entry->to_datum(scm, type_oid);

    elog(ERROR, "Conversion function for type OID %u not found", type_oid);
    // Unreachable
    return (Datum)0;
}

Datum
convert_boxed_datum_to_datum(SCM scm, Oid target_type_oid) {

    elog(NOTICE, "y");
    if (true) {
        Oid source_type_oid = get_boxed_datum_type(scm);
        Datum value = get_boxed_datum_value(scm);

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

/* SCM */
/* datum_enum_to_scm(Datum x, Oid type_oid) { */

/*     HeapTuple tup; */
/*     SCM result; */

/*     tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(x)); */

/*     if (!HeapTupleIsValid(tup)) { */
/*         elog(ERROR, "cache lookup failed for enum %lu", ObjectIdGetDatum(x)); */
/*     } */

/*     result = scm_from_locale_symbol(NameStr(((Form_pg_enum) GETSTRUCT(tup))->enumlabel)); */

/*     ReleaseSysCache(tup); */

/*     return result; */
/* } */

/* Datum */
/* scm_to_datum_enum(SCM x, Oid type_oid) { */

/*     HeapTuple tup; */
/*     char *value_name; */
/*     Datum result; */

/*     if (!scm_is_symbol(x)) { */
/*         elog(ERROR, "The SCM value must be a symbol"); */
/*     } */

/*     value_name = scm_to_locale_string(scm_symbol_to_string(x)); */

/*     tup = SearchSysCache2(ENUMTYPOIDNAME, */
/*                           ObjectIdGetDatum(type_oid), */
/*                           CStringGetDatum(value_name)); */

/*     free(value_name); */

/*     if (!HeapTupleIsValid(tup)) { */
/*         elog(ERROR, "Could not find enum value for string: %s", scm_to_string(x)); */
/*     } */

/*     result = ((Form_pg_enum) GETSTRUCT(tup))->oid; */

/*     ReleaseSysCache(tup); */

/*     return result; */
/* } */

SCM
datum_int2_to_scm(Datum x, Oid type_oid) {
    return scm_from_short(DatumGetInt16(x));
}

Datum
scm_to_datum_int2(SCM x, Oid type_oid) {

    if (!is_int2(x)) {
        elog(ERROR, "int2 result expected, not: %s", scm_to_string(x));
    }

    return Int16GetDatum(scm_to_short(x));
}

SCM
datum_int4_to_scm(Datum x, Oid type_oid) {
    return scm_from_int32(DatumGetInt32(x));
}

Datum
scm_to_datum_int4(SCM x, Oid type_oid) {

    if (!is_int4(x)) {
        elog(ERROR, "int4 result expected, not: %s", scm_to_string(x));
    }

    return Int32GetDatum(scm_to_int32(x));
}

SCM
datum_int8_to_scm(Datum x, Oid type_oid) {
    return scm_from_int64(DatumGetInt64(x));
}

Datum
scm_to_datum_int8(SCM x, Oid type_oid) {

    if (!is_int8(x)) {
        elog(ERROR, "int8 result expected, not: %s", scm_to_string(x));
    }

    return Int64GetDatum(scm_to_int64(x));
}

SCM
datum_float4_to_scm(Datum x, Oid type_oid) {
    return scm_from_double((double)DatumGetFloat4(x));
}

Datum
scm_to_datum_float4(SCM x, Oid type_oid) {

    if (scm_is_number(x))
        return Float4GetDatum(scm_to_double(x));

    else if (is_decimal(x))
        return Float4GetDatum(scm_to_double(scm_call_1(decimal_to_inexact_proc, x)));

    else {
        elog(ERROR, "number result expected, not: %s", scm_to_string(x));
    }
}

SCM
datum_float8_to_scm(Datum x, Oid type_oid) {
    return scm_from_double(DatumGetFloat8(x));
}

Datum
scm_to_datum_float8(SCM x, Oid type_oid) {

    if (scm_is_number(x))
        return Float8GetDatum(scm_to_double(x));

    else if (is_decimal(x))
        return Float8GetDatum(scm_to_double(scm_call_1(decimal_to_inexact_proc, x)));

    else {
        elog(ERROR, "number result expected, not: %s", scm_to_string(x));
    }
}

SCM
datum_text_to_scm(Datum x, Oid type_oid) {

    char *cstr = text_to_cstring(DatumGetTextP(x));
    SCM scm_str = scm_from_locale_string(cstr);

    pfree(cstr);

    return scm_str;
}

Datum
scm_to_datum_text(SCM x, Oid type_oid) {

    char *cstr;
    text *pg_text;
    Datum text_datum;

    if (!scm_is_string(x)) {
        elog(ERROR, "string result expected, not: %s", scm_to_string(x));
    }

    cstr = scm_to_locale_string(x);
    pg_text = cstring_to_text(cstr);
    text_datum = PointerGetDatum(pg_text);

    free(cstr);

    return text_datum;
}

SCM
datum_bytea_to_scm(Datum x, Oid type_oid) {

    bytea *bytea_data = DatumGetByteaP(x);
    char *binary_data = VARDATA(bytea_data);
    int len = VARSIZE(bytea_data) - VARHDRSZ;

    SCM scm_bytevector = scm_c_make_bytevector(len);

    for (int i = 0; i < len; i++) {
        scm_c_bytevector_set_x(scm_bytevector, i, binary_data[i]);
    }

    return scm_bytevector;
}

Datum
scm_to_datum_bytea(SCM x, Oid type_oid) {

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

SCM
datum_timestamptz_to_scm(Datum x, Oid type_oid) {

    TimestampTz timestamp = DatumGetTimestampTz(x);
    struct pg_tm tm;
    fsec_t msec;
    int tz_offset;

    if (timestamp2tm(timestamp, &tz_offset, &tm, &msec, NULL, NULL)) {

        elog(ERROR, "Invalid timestamp");
        return SCM_BOOL_F; // Shouldn't reach here; just to satisfy the compiler
    }
    else {
        return scm_call_8(
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
}

Datum
scm_to_datum_timestamptz(SCM x, Oid type_oid) {

    if (!is_date(x)) {
        elog(ERROR, "date result expected, not: %s", scm_to_string(x));
    }
    else {
        // Extract the individual components of the SRFI-19 date object
        SCM nanosecond_scm = scm_call_1(date_nanosecond_proc, x);
        SCM tz_offset_scm  = scm_call_1(date_zone_offset_proc, x);
        SCM year_scm       = scm_call_1(date_year_proc, x);
        SCM month_scm      = scm_call_1(date_month_proc, x);
        SCM day_scm        = scm_call_1(date_day_proc, x);
        SCM hour_scm       = scm_call_1(date_hour_proc, x);
        SCM minute_scm     = scm_call_1(date_minute_proc, x);
        SCM second_scm     = scm_call_1(date_second_proc, x);

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

        if (tm2timestamp(&tm, msec, &tz_offset, &timestamp) == 0) {
            return TimestampTzGetDatum(timestamp);
        }
        else {
            elog(ERROR, "Invalid date components");
            return 0; // Shouldn't reach here; just to satisfy the compiler
        }
    }
}

SCM
datum_date_to_scm(Datum x, Oid type_oid) {
    return datum_timestamptz_to_scm(datum_date_to_timestamptz(x), type_oid);
}

Datum
scm_to_datum_date(SCM x, Oid type_oid) {
    if (!is_date(x)) {
        elog(ERROR, "date result expected, not: %s", scm_to_string(x));
    }
    else {
        // Extract the individual components of the SRFI-19 date object
        SCM year_scm       = scm_call_1(date_year_proc, x);
        SCM month_scm      = scm_call_1(date_month_proc, x);
        SCM day_scm        = scm_call_1(date_day_proc, x);

        // Convert from Scheme values to C types
        int year         = scm_to_int(year_scm);
        int month        = scm_to_int(month_scm);
        int day          = scm_to_int(day_scm);

        return DateADTGetDatum(date2j(year, month, day) - POSTGRES_EPOCH_JDATE);
    }
}

SCM
datum_time_to_scm(Datum x, Oid type_oid) {

    struct pg_tm tm;
    fsec_t msec;

    if (time2tm(DatumGetTimeADT(x), &tm, &msec) == 0) {
        return scm_call_3(
            make_time_proc,
            time_monotonic_symbol,
            scm_from_long(msec * 1000),
            scm_from_int(tm.tm_sec + tm.tm_min * 60 + tm.tm_hour * 3600));
    }
    else {
        elog(ERROR, "Invalid time");
        return SCM_BOOL_F; // Shouldn't reach here; just to satisfy the compiler
    }
}

Datum
scm_to_datum_time(SCM x, Oid type_oid) {
    if (!is_time(x)) {
        elog(ERROR, "time result expected, not: %s", scm_to_string(x));
        return 0; // Shouldn't reach here; just to satisfy the compiler
    }
    else {
        int seconds = scm_to_int(scm_call_1(time_second_proc, x));
        int nanoseconds = scm_to_int(scm_call_1(time_nanosecond_proc, x));

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

        if (tm2time(&tm, msec, &time) == 0) {
            return TimeADTGetDatum(time);
        }
        else {
            elog(ERROR, "Invalid date components");
            return 0; // Shouldn't reach here; just to satisfy the compiler
        }
    }
}

SCM
datum_timetz_to_scm(Datum x, Oid type_oid) {

    TimeTzADT *ttz;
    fsec_t msec;
    int tz;
    struct pg_tm tm;

    /* Extract TimeTz struct from the input Datum */
    ttz = DatumGetTimeTzADTP(x);

    if (timetz2tm(ttz, &tm, &msec, &tz) == 0) {

        int secs = (tm.tm_sec + tm.tm_min * 60 + tm.tm_hour * 3600 + tz) % SECS_PER_DAY;

        if (secs < 0)
            secs += SECS_PER_DAY;

        return scm_call_3(
            make_time_proc,
            time_monotonic_symbol,
            scm_from_long(msec * 1000),
            scm_from_int(secs));
    }
    else {
        elog(ERROR, "Invalid time");
        return SCM_BOOL_F; // Shouldn't reach here; just to satisfy the compiler
    }
}

Datum
scm_to_datum_timetz(SCM x, Oid type_oid) {

    elog(NOTICE, "x");
    if (!is_time(x)) {
        elog(ERROR, "time result expected, not: %s", scm_to_string(x));
        return 0; // Shouldn't reach here; just to satisfy the compiler
    }
    else {
        TimeTzADT *result = (TimeTzADT *) palloc(sizeof(TimeTzADT));

        int seconds = scm_to_int(scm_call_1(time_second_proc, x));
        int nanoseconds = scm_to_int(scm_call_1(time_nanosecond_proc, x));

        // Get tz from current time
        struct pg_tm tm;
        fsec_t fsec;
        int tz;
        GetCurrentTimeUsec(&tm, &fsec, &tz);

        elog(NOTICE, "current tz: %d", tz);

        result->time = (seconds - tz) * USECS_PER_SEC + (nanoseconds/NS_PER_USEC) % USECS_PER_DAY;

        if (result->time < 0)
            result->time += USECS_PER_DAY;

        result->zone = tz;

        return TimeTzADTPGetDatum(result);
    }
}

SCM
datum_interval_to_scm(Datum x, Oid type_oid)
{
    Interval *interval = DatumGetIntervalP(x);

    return scm_call_3(
        make_time_proc,
        time_duration_symbol,
        scm_from_long((interval->time % USECS_PER_SEC) * NS_PER_USEC),
        scm_from_long(
            (int64)interval->month * SECS_PER_MONTH
            + (int64)interval->day * SECS_PER_DAY
            + interval->time / USECS_PER_SEC));
}

Datum
scm_to_datum_interval(SCM x, Oid type_oid) {

    SCM seconds = scm_call_1(time_second_proc, x);

    Interval *interval = (Interval *)palloc(sizeof(Interval));

    interval->month = scm_to_int(scm_floor_quotient(seconds, scm_from_int(SECS_PER_MONTH)));

    interval->day = scm_to_int(
        scm_floor_quotient(
            scm_floor_remainder(seconds, scm_from_int(SECS_PER_MONTH)),
            scm_from_int(SECS_PER_DAY)));

    interval->time =
      scm_to_long(scm_floor_remainder(seconds, scm_from_int(SECS_PER_DAY))) * USECS_PER_SEC
      + scm_to_long(scm_call_1(time_nanosecond_proc, x)) / NS_PER_USEC;

    return IntervalPGetDatum(interval);
}

SCM
datum_numeric_to_scm(Datum x, Oid type_oid) {
    SCM s = scm_from_locale_string(DatumGetCString(DirectFunctionCall1(numeric_out, x)));

    return scm_call_1(string_to_decimal_proc, s);
}

Datum
scm_to_datum_numeric(SCM x, Oid type_oid) {

    char *numeric_str = NULL;

    if (is_decimal(x)) {
        if (is_valid_decimal(x))
            numeric_str = scm_to_locale_string(scm_call_1(decimal_to_string_proc, x));
        else
            elog(ERROR, "invalid decimal result: %s", scm_to_string(x));
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
            else if (scm_is_rational(x)) {
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

SCM
datum_bool_to_scm(Datum x, Oid type_oid) {
    return scm_from_bool(DatumGetBool(x));
}

Datum
scm_to_datum_bool(SCM x, Oid type_oid) {
    return BoolGetDatum(scm_to_bool(x));
}

SCM datum_point_to_scm(Datum x, Oid type_oid) {

    // Extract the point value from the Datum
    Point *point = DatumGetPointP(x);

    // Convert the point's x and y to SCM numbers
    SCM scm_x = scm_from_double(point->x);
    SCM scm_y = scm_from_double(point->y);

    // Create the SCM point object using the Scheme function
    SCM scm_point = scm_call_2(make_point_proc, scm_x, scm_y);

    return scm_point;
}

Datum
scm_to_datum_point(SCM x, Oid type_oid) {

    if (!is_point(x)) {
        elog(ERROR, "point result expected, not: %s", scm_to_string(x));
    }
    else {

        // Get x and y components from the Scheme point object
        SCM scm_x = scm_call_1(point_x_proc, x);
        SCM scm_y = scm_call_1(point_y_proc, x);

        // Convert SCM x and y to float8
        float8 x_val = scm_to_double(scm_x);
        float8 y_val = scm_to_double(scm_y);

        // Create a new PostgreSQL point
        Point *point = (Point *) palloc(sizeof(Point));

        point->x = x_val;
        point->y = y_val;

        // Convert the point to a Datum
        return PointPGetDatum(point);
    }
}

SCM datum_line_to_scm(Datum x, Oid type_oid) {

    // Extract the line value from the Datum
    LINE *line = DatumGetLineP(x);

    // Convert the line's a and y to SCM numbers
    SCM scm_a = scm_from_double(line->A);
    SCM scm_b = scm_from_double(line->B);
    SCM scm_c = scm_from_double(line->C);

    // Create the SCM line object using the Scheme function
    SCM scm_line = scm_call_3(make_line_proc, scm_a, scm_b, scm_c);

    return scm_line;
}

Datum
scm_to_datum_line(SCM x, Oid type_oid) {

    if (!is_line(x)) {
        elog(ERROR, "line result expected, not: %s", scm_to_string(x));
    }
    else {

        // Get a, b, and c components from the Scheme line object
        SCM scm_a = scm_call_1(line_a_proc, x);
        SCM scm_b = scm_call_1(line_b_proc, x);
        SCM scm_c = scm_call_1(line_c_proc, x);

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

SCM datum_lseg_to_scm(Datum x, Oid type_oid) {

    // Extract the lseg value from the Datum
    LSEG *lseg = DatumGetLsegP(x);

    // Convert the endpoints to Scheme objects
    SCM scm_point_a = datum_point_to_scm(PointPGetDatum(&lseg->p[0]), OID_NOT_USED);
    SCM scm_point_b = datum_point_to_scm(PointPGetDatum(&lseg->p[1]), OID_NOT_USED);

    // Create the Scheme lseg object
    SCM scm_lseg = scm_call_2(make_lseg_proc, scm_point_a, scm_point_b);

    return scm_lseg;
}

Datum
scm_to_datum_lseg(SCM x, Oid type_oid) {

    if (!is_lseg(x)) {
        elog(ERROR, "lseg result expected, not: %s", scm_to_string(x));
    }
    else {

        // Get point components from the Scheme lseg object
        SCM scm_a = scm_call_1(lseg_a_proc, x);
        SCM scm_b = scm_call_1(lseg_b_proc, x);

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

SCM datum_box_to_scm(Datum x, Oid type_oid) {

    // Extract the box value from the Datum
    BOX *box = DatumGetBoxP(x);

    // Convert the endpoints to Scheme objects
    SCM scm_point_a = datum_point_to_scm(PointPGetDatum(&box->high), OID_NOT_USED);
    SCM scm_point_b = datum_point_to_scm(PointPGetDatum(&box->low), OID_NOT_USED);

    // Create the Scheme box object
    SCM scm_box = scm_call_2(make_box_proc, scm_point_a, scm_point_b);

    return scm_box;
}

Datum
scm_to_datum_box(SCM x, Oid type_oid) {

    if (!is_box(x)) {
        elog(ERROR, "box result expected, not: %s", scm_to_string(x));
    }
    else {

        // Get a and b components from the Scheme box object
        SCM scm_a = scm_call_1(box_a_proc, x);
        SCM scm_b = scm_call_1(box_b_proc, x);

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

SCM datum_path_to_scm(Datum x, Oid type_oid) {

    // Extract the path struct from the Datum
    PATH *path = DatumGetPathP(x);

    // Create a Scheme vector to hold the points
    SCM scm_points_vector = scm_make_vector(scm_from_int(path->npts), SCM_BOOL_F);
    SCM scm_is_closed = scm_from_bool(path->closed);

    // Convert each point to Scheme object and populate the vector
    for (int i = 0; i < path->npts; i++) {
        Datum point_datum = PointPGetDatum(&path->p[i]);
        SCM scm_point = datum_point_to_scm(point_datum, OID_NOT_USED);
        scm_vector_set_x(scm_points_vector, scm_from_int(i), scm_point);
    }

    return scm_call_2(make_path_proc, scm_is_closed, scm_points_vector);
}

Datum scm_to_datum_path(SCM x, Oid type_oid) {

    if (!is_path(x)) {
        elog(ERROR, "path result expected, not: %s", scm_to_string(x));
    }
    else {

        PATH *path;

        // Get the Scheme vector of points from the Scheme path object
        SCM scm_points_vector = scm_call_1(path_points_proc, x);

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

	path->closed = scm_to_bool(scm_call_1(path_is_closed_proc, x));

	/* prevent instability in unused pad bytes */
	path->dummy = 0;

        // Convert each point from Scheme object to Datum and populate the PATH
        for (int i = 0; i < npts; i++) {
            SCM scm_point = scm_vector_ref(scm_points_vector, scm_from_int(i));
            Datum point_datum = scm_to_datum_point(scm_point, OID_NOT_USED);
            path->p[i] = *DatumGetPointP(point_datum); // Assuming DatumGetPointP returns a Point *
        }

        return PathPGetDatum(path);
    }
}

SCM datum_polygon_to_scm(Datum x, Oid type_oid) {

    // Extract the polygon struct from the Datum
    POLYGON *polygon = DatumGetPolygonP(x);

    // Create a Scheme vector to hold the points
    SCM scm_points_vector = scm_make_vector(scm_from_int(polygon->npts), SCM_BOOL_F);
    SCM scm_boundbox = datum_box_to_scm(BoxPGetDatum(&polygon->boundbox), OID_NOT_USED);

    // Convert each point to Scheme object and populate the vector
    for (int i = 0; i < polygon->npts; i++) {
        Datum point_datum = PointPGetDatum(&polygon->p[i]);
        SCM scm_point = datum_point_to_scm(point_datum, OID_NOT_USED);
        scm_vector_set_x(scm_points_vector, scm_from_int(i), scm_point);
    }

    return scm_call_2(make_polygon_proc, scm_boundbox, scm_points_vector);
}

Datum scm_to_datum_polygon(SCM x, Oid type_oid) {

    if (!is_polygon(x)) {
        elog(ERROR, "polygon result expected, not: %s", scm_to_string(x));
    }
    else {

        POLYGON *polygon;

        // Get the Scheme vector of points from the Scheme polygon object
        SCM scm_points_vector = scm_call_1(polygon_points_proc, x);

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

        scm_boundbox = scm_call_1(polygon_boundbox_proc, x);
	polygon->boundbox = *DatumGetBoxP(scm_to_datum_box(scm_boundbox, OID_NOT_USED));

        // Convert each point from Scheme object to Datum and populate the POLYGON
        for (int i = 0; i < npts; i++) {
            SCM scm_point = scm_vector_ref(scm_points_vector, scm_from_int(i));
            Datum point_datum = scm_to_datum_point(scm_point, OID_NOT_USED);
            polygon->p[i] = *DatumGetPointP(point_datum); // Assuming DatumGetPointP returns a Point *
        }

        return PolygonPGetDatum(polygon);
    }
}

SCM
datum_circle_to_scm(Datum x, Oid type_oid) {

    CIRCLE *circle = DatumGetCircleP(x);  // Convert Datum to CIRCLE type
    Point center = circle->center;
    float8 radius = circle->radius;

    // Convert the center point and radius to SCM
    SCM scm_center = datum_point_to_scm(CirclePGetDatum(&center), OID_NOT_USED);
    SCM scm_radius = scm_from_double(radius);

    // Use your Scheme procedure to create the circle record
    return scm_call_2(make_circle_proc, scm_center, scm_radius);
}

Datum
scm_to_datum_circle(SCM x, Oid type_oid) {

    if (!is_circle(x)) {
        elog(ERROR, "circle result expected, not: %s", scm_to_string(x));
    }
    else {
        // Retrieve the center point and radius from the Scheme record
        SCM scm_center = scm_call_1(circle_center_proc, x);
        SCM scm_radius = scm_call_1(circle_radius_proc, x);

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

SCM
datum_inet_to_scm(Datum x, Oid type_oid) {
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

    return scm_call_3(make_inet_proc, scm_family, scm_bits, scm_address);
}

Datum scm_to_datum_inet(SCM x, Oid type_oid) {
    if (!is_inet(x)) {
        elog(ERROR, "inet result expected, not: %s", scm_to_string(x));
    }
    else {
        inet *inet;

        // Get the family, bits, and address from the SCM object
        SCM scm_family = scm_call_1(inet_family_proc, x);
        SCM scm_bits = scm_call_1(inet_bits_proc, x);
        SCM scm_address = scm_call_1(inet_address_proc, x);

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

SCM
datum_macaddr_to_scm(Datum x, Oid type_oid) {

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

    return scm_call_1(make_macaddr_proc, scm_data);
}

Datum
scm_to_datum_macaddr(SCM x, Oid type_oid) {

    if (!is_macaddr(x)) {
        elog(ERROR, "macaddr result expected, not: %s", scm_to_string(x));
    }
    else {
        // Get the data bytevector from the SCM object
        SCM scm_data = scm_call_1(macaddr_data_proc, x);
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

SCM
datum_macaddr8_to_scm(Datum x, Oid type_oid) {

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

    return scm_call_1(make_macaddr8_proc, scm_data);
}

Datum
scm_to_datum_macaddr8(SCM x, Oid type_oid) {

    if (!is_macaddr8(x)) {
        elog(ERROR, "macaddr8 result expected, not: %s", scm_to_string(x));
    }
    else {
        // Get the data bytevector from the SCM object
        SCM scm_data = scm_call_1(macaddr8_data_proc, x);
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

SCM
datum_bit_string_to_scm(Datum x, Oid type_oid) {
    // Get the pointer to the varbit struct
    VarBit *bit_str = DatumGetVarBitP(x);

    // Get the length in bits
    int bitlen = VARBITLEN(bit_str);

    // Create a new bytevector in Scheme to hold the data
    SCM scm_data = scm_c_make_bytevector(VARBITBYTES(bit_str));

    // Copy the data from the varbit struct to the bytevector
    memcpy(SCM_BYTEVECTOR_CONTENTS(scm_data), VARBITS(bit_str), VARBITBYTES(bit_str));

    return scm_call_2(make_bit_string_proc, scm_data, scm_from_int(bitlen));
}

Datum
scm_to_datum_bit_string(SCM x, Oid type_oid) {
    if (!is_bit_string(x)) {
        elog(ERROR, "bit string result expected, not: %s", scm_to_string(x));
    }
    else {
        // Get the length and data from the Scheme record
        SCM scm_length = scm_call_1(bit_string_length_proc, x);
        SCM scm_data = scm_call_1(bit_string_data_proc, x);

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

SCM
datum_uuid_to_scm(Datum x, Oid type_oid) {
    // Assume x is a UUID Datum, cast it to pg_uuid_t *
    pg_uuid_t *uuid_ptr = DatumGetUUIDP(x);

    // Create an SCM bytevector to hold the UUID bytes
    SCM scm_bytevector = scm_c_make_bytevector(UUID_LEN);

    // Copy the UUID bytes into the SCM bytevector
    memcpy(SCM_BYTEVECTOR_CONTENTS(scm_bytevector), uuid_ptr->data, UUID_LEN);

    // Return the SCM bytevector
    return scm_bytevector;
}

Datum
scm_to_datum_uuid(SCM x, Oid type_oid) {
    if (!SCM_BYTEVECTOR_P(x)) {
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

SCM
datum_xml_to_scm(Datum x, Oid type_oid) {
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

Datum
scm_to_datum_xml(SCM x, Oid type_oid) {
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

SCM
datum_json_to_scm(Datum x, Oid type_oid) {
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

Datum
scm_to_datum_json(SCM x, Oid type_oid) {
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

SCM
datum_jsonb_to_scm(Datum x, Oid type_oid) {
    // Convert Datum to JsonbValue
    Jsonb *jsonbValue = DatumGetJsonbP(x);

    // Serialize JsonbValue to C string
    char *cstr = JsonbToCString(NULL, &jsonbValue->root, VARSIZE(jsonbValue));

    // Convert C string to SCM string
    SCM scmString = scm_from_locale_string(cstr);

    return scmString;
}

Datum
scm_to_datum_jsonb(SCM x, Oid type_oid) {
    // Convert SCM string to C string
    char *cstr = scm_to_locale_string(x);
    int len = scm_to_int(scm_string_length(x));

    Datum result = jsonb_from_cstring(cstr, len);

    elog(NOTICE, "cstr: %s", cstr);
    // Free C string
    free(cstr);

    return result;
}

SCM
datum_void_to_scm(Datum x, Oid type_oid) {
    return SCM_UNDEFINED;
}

Datum
scm_to_datum_void(SCM x, Oid type_oid) {
    return (Datum) 0;
}

Oid
get_boxed_datum_type(SCM x) {
    return scm_to_uint32(scm_call_1(boxed_datum_type_proc, x));
}

Datum
get_boxed_datum_value(SCM x) {
    return scm_to_uint64(scm_call_1(boxed_datum_value_proc, x));
}

bool
is_bit_string(SCM x) {
    return scm_is_true(scm_call_1(is_bit_string_proc, x));
}

bool
is_box(SCM x) {
    return scm_is_true(scm_call_1(is_box_proc, x));
}

bool
is_boxed_datum(SCM x) {
    return scm_is_true(scm_call_1(is_boxed_datum_proc, x));
}

bool
is_circle(SCM x) {
    return scm_is_true(scm_call_1(is_circle_proc, x));
}

bool
is_date(SCM x) {
    return scm_is_true(scm_call_1(is_date_proc, x));
}

bool
is_decimal(SCM x) {
    return scm_is_true(scm_call_1(is_decimal_proc, x));
}

bool
is_inet(SCM x) {
    return scm_is_true(scm_call_1(is_inet_proc, x));
}

bool
is_int2(SCM x) {
    return scm_is_true(scm_call_1(is_int2_proc, x));
}

bool
is_int4(SCM x) {
    return scm_is_true(scm_call_1(is_int4_proc, x));
}

bool
is_int8(SCM x) {
    return scm_is_true(scm_call_1(is_int8_proc, x));
}

bool
is_line(SCM x) {
    return scm_is_true(scm_call_1(is_line_proc, x));
}

bool
is_lseg(SCM x) {
    return scm_is_true(scm_call_1(is_lseg_proc, x));
}

bool
is_macaddr(SCM x) {
    return scm_is_true(scm_call_1(is_macaddr_proc, x));
}

bool
is_macaddr8(SCM x) {
    return scm_is_true(scm_call_1(is_macaddr8_proc, x));
}

bool
is_path(SCM x) {
    return scm_is_true(scm_call_1(is_path_proc, x));
}

bool
is_point(SCM x) {
    return scm_is_true(scm_call_1(is_point_proc, x));
}

bool
is_polygon(SCM x) {
    return scm_is_true(scm_call_1(is_polygon_proc, x));
}

bool
is_time(SCM x) {
    return scm_is_true(scm_call_1(is_time_proc, x));
}

bool
is_valid_decimal(SCM x) {
    return scm_is_true(scm_call_1(is_valid_decimal_proc, x));
}

Datum
datum_date_to_timestamptz(Datum x)
{
    DateADT date = DatumGetDateADT(x);
    TimestampTz timestamp = date2timestamptz_opt_overflow(date, NULL);
    return TimestampTzGetDatum(timestamp);
}

char *
scm_to_string(SCM obj) {
    MemoryContext context = CurrentMemoryContext;

    SCM proc = scm_eval_string(scm_from_locale_string("(lambda (x) (format #f \"~s\" x))"));
    SCM str_scm = scm_call_1(proc, obj);

    // Convert SCM string to C string and allocate it in the given memory context
    size_t len;
    char *c_str = scm_to_locale_stringn(str_scm, &len);
    char *result = MemoryContextStrdup(context, c_str);

    free(c_str);  // Free temporary string

    return result;
}

void
insert_type_cache_entry(Oid type_oid, ToScmFunc to_scm, ToDatumFunc to_datum) {

    bool found;
    TypeCacheEntry *entry = (TypeCacheEntry *)hash_search(
        typeCache,
        &type_oid,
        HASH_ENTER,
        &found);

    if (found) {
        elog(ERROR, "Unexpected duplicate in type cache: %d", type_oid);
    }
    else {
        // Initialize the new entry.
        entry->type_oid = type_oid;
        entry->to_scm = to_scm;
        entry->to_datum = to_datum;
    }
}

/* bool */
/* is_enum_type(Oid type_oid) { */

/*     HeapTuple tup; */
/*     Form_pg_type type_form; */
/*     bool result; */

/*     tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid)); */

/*     if (!HeapTupleIsValid(tup)) { */
/*         elog(ERROR, "cache lookup failed for type %u", type_oid); */
/*     } */

/*     type_form = (Form_pg_type) GETSTRUCT(tup); */
/*     result = type_form->typtype == TYPTYPE_ENUM; */

/*     ReleaseSysCache(tup); */

/*     return result; */
/* } */


/* The following was taken from src/backend/utils/adt/jsonb.c of REL_14_9. */

static size_t checkStringLen(size_t len);
static void jsonb_in_object_start(void *pstate);
static void jsonb_in_object_end(void *pstate);
static void jsonb_in_array_start(void *pstate);
static void jsonb_in_array_end(void *pstate);
static void jsonb_in_object_field_start(void *pstate, char *fname, bool isnull);
static void jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype);

typedef struct JsonbInState
{
	JsonbParseState *parseState;
	JsonbValue *res;
} JsonbInState;

Datum
jsonb_from_cstring(char *json, int len)
{
    JsonLexContext *lex;
    JsonbInState state;
    JsonSemAction sem;

    memset(&state, 0, sizeof(state));
    memset(&sem, 0, sizeof(sem));
    lex = makeJsonLexContextCstringLen(json, len, GetDatabaseEncoding(), true);

    sem.semstate = (void *) &state;

    sem.object_start = jsonb_in_object_start;
    sem.array_start = jsonb_in_array_start;
    sem.object_end = jsonb_in_object_end;
    sem.array_end = jsonb_in_array_end;
    sem.scalar = jsonb_in_scalar;
    sem.object_field_start = jsonb_in_object_field_start;

    pg_parse_json_or_ereport(lex, &sem);

    /* after parsing, the item member has the composed jsonb structure */
    PG_RETURN_POINTER(JsonbValueToJsonb(state.res));
}

static size_t
checkStringLen(size_t len)
{
	if (len > JENTRY_OFFLENMASK)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("string too long to represent as jsonb string"),
				 errdetail("Due to an implementation restriction, jsonb strings cannot exceed %d bytes.",
						   JENTRY_OFFLENMASK)));

	return len;
}

static void
jsonb_in_object_start(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_OBJECT, NULL);
}

static void
jsonb_in_object_end(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_END_OBJECT, NULL);
}

static void
jsonb_in_array_start(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, NULL);
}

static void
jsonb_in_array_end(void *pstate)
{
	JsonbInState *_state = (JsonbInState *) pstate;

	_state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);
}

static void
jsonb_in_object_field_start(void *pstate, char *fname, bool isnull)
{
	JsonbInState *_state = (JsonbInState *) pstate;
	JsonbValue	v;

	Assert(fname != NULL);
	v.type = jbvString;
	v.val.string.len = checkStringLen(strlen(fname));
	v.val.string.val = fname;

	_state->res = pushJsonbValue(&_state->parseState, WJB_KEY, &v);
}

/*
 * For jsonb we always want the de-escaped value - that's what's in token
 */
static void
jsonb_in_scalar(void *pstate, char *token, JsonTokenType tokentype)
{
	JsonbInState *_state = (JsonbInState *) pstate;
	JsonbValue	v;
	Datum		numd;

	switch (tokentype)
	{

		case JSON_TOKEN_STRING:
			Assert(token != NULL);
			v.type = jbvString;
			v.val.string.len = checkStringLen(strlen(token));
			v.val.string.val = token;
			break;
		case JSON_TOKEN_NUMBER:

			/*
			 * No need to check size of numeric values, because maximum
			 * numeric size is well below the JsonbValue restriction
			 */
			Assert(token != NULL);
			v.type = jbvNumeric;
			numd = DirectFunctionCall3(numeric_in,
									   CStringGetDatum(token),
									   ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(-1));
			v.val.numeric = DatumGetNumeric(numd);
			break;
		case JSON_TOKEN_TRUE:
			v.type = jbvBool;
			v.val.boolean = true;
			break;
		case JSON_TOKEN_FALSE:
			v.type = jbvBool;
			v.val.boolean = false;
			break;
		case JSON_TOKEN_NULL:
			v.type = jbvNull;
			break;
		default:
			/* should not be possible */
			elog(ERROR, "invalid json token type");
			break;
	}

	if (_state->parseState == NULL)
	{
		/* single scalar */
		JsonbValue	va;

		va.type = jbvArray;
		va.val.array.rawScalar = true;
		va.val.array.nElems = 1;

		_state->res = pushJsonbValue(&_state->parseState, WJB_BEGIN_ARRAY, &va);
		_state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
		_state->res = pushJsonbValue(&_state->parseState, WJB_END_ARRAY, NULL);
	}
	else
	{
		JsonbValue *o = &_state->parseState->contVal;

		switch (o->type)
		{
			case jbvArray:
				_state->res = pushJsonbValue(&_state->parseState, WJB_ELEM, &v);
				break;
			case jbvObject:
				_state->res = pushJsonbValue(&_state->parseState, WJB_VALUE, &v);
				break;
			default:
				elog(ERROR, "unexpected parent of nested structure");
		}
	}
}

/* End quoted code */
