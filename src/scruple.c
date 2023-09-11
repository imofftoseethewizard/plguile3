#include "libguile.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

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

typedef SCM (*ToScmFunc)(Datum);
typedef Datum (*ToDatumFunc)(SCM);

static SCM datum_bytea_to_scm(Datum x);
static SCM datum_date_to_scm(Datum x);
static SCM datum_float4_to_scm(Datum x);
static SCM datum_float8_to_scm(Datum x);
static SCM datum_int2_to_scm(Datum x);
static SCM datum_int4_to_scm(Datum x);
static SCM datum_int8_to_scm(Datum x);
static SCM datum_interval_to_scm(Datum x);
static SCM datum_text_to_scm(Datum x);
static SCM datum_time_to_scm(Datum x);
static SCM datum_timestamptz_to_scm(Datum x);
static SCM datum_void_to_scm(Datum x);

static Datum scm_to_datum_bytea(SCM x);
static Datum scm_to_datum_date(SCM x);
static Datum scm_to_datum_float4(SCM x);
static Datum scm_to_datum_float8(SCM x);
static Datum scm_to_datum_int2(SCM x);
static Datum scm_to_datum_int4(SCM x);
static Datum scm_to_datum_int8(SCM x);
static Datum scm_to_datum_interval(SCM x);
static Datum scm_to_datum_text(SCM x);
static Datum scm_to_datum_time(SCM x);
static Datum scm_to_datum_timestamptz(SCM x);
static Datum scm_to_datum_void(SCM x);

static Datum datum_date_to_timestamptz(Datum x);
static char* scm_to_string(SCM x);

static bool is_date(SCM x);
static bool is_time(SCM x);
static bool is_int2(SCM x);
static bool is_int4(SCM x);
static bool is_int8(SCM x);
static bool is_float4(SCM x);
static bool is_float8(SCM x);

typedef struct TypeCacheEntry {
    Oid type_oid;          // OID of a PostgreSQL type
    ToScmFunc to_scm;      // Function pointer to convert Datum to SCM
    ToDatumFunc to_datum;  // Function pointer to convert SCM to Datum
} TypeCacheEntry;

static void insert_type_cache_entry(Oid type_oid, ToScmFunc to_scm, ToDatumFunc to_datum);
static SCM datum_to_scm(Datum datum, Oid type_oid);
static Datum scm_to_datum(SCM scm, Oid type_oid);
static SCM scruple_compile_func(Oid func_oid);
static Datum convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo);
static Datum convert_result_to_composite_datum(SCM result, TupleDesc tuple_desc);

static SCM date_day_proc;
static SCM date_hour_proc;
static SCM date_minute_proc;
static SCM date_month_proc;
static SCM date_nanosecond_proc;
static SCM date_second_proc;
static SCM date_year_proc;
static SCM date_zone_offset_proc;
static SCM is_date_proc;
static SCM is_time_proc;
static SCM is_int2_proc;
static SCM is_int4_proc;
static SCM is_int8_proc;
static SCM is_float4_proc;
static SCM is_float8_proc;
static SCM make_date_proc;
static SCM make_time_proc;
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
    insert_type_cache_entry(TypenameGetTypid("void"), datum_void_to_scm, scm_to_datum_void);
    insert_type_cache_entry(TypenameGetTypid("int2"), datum_int2_to_scm, scm_to_datum_int2);
    insert_type_cache_entry(TypenameGetTypid("int4"), datum_int4_to_scm, scm_to_datum_int4);
    insert_type_cache_entry(TypenameGetTypid("int8"), datum_int8_to_scm, scm_to_datum_int8);
    insert_type_cache_entry(TypenameGetTypid("float4"), datum_float4_to_scm, scm_to_datum_float4);
    insert_type_cache_entry(TypenameGetTypid("float8"), datum_float8_to_scm, scm_to_datum_float8);
    insert_type_cache_entry(TypenameGetTypid("text"), datum_text_to_scm, scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("bpchar"), datum_text_to_scm, scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("char"), datum_text_to_scm, scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("varchar"), datum_text_to_scm, scm_to_datum_text);
    insert_type_cache_entry(TypenameGetTypid("bytea"), datum_bytea_to_scm, scm_to_datum_bytea);
    insert_type_cache_entry(TypenameGetTypid("timestamp"), datum_timestamptz_to_scm, scm_to_datum_timestamptz);
    insert_type_cache_entry(TypenameGetTypid("timestamptz"), datum_timestamptz_to_scm, scm_to_datum_timestamptz);
    insert_type_cache_entry(TypenameGetTypid("date"), datum_date_to_scm, scm_to_datum_date);
    insert_type_cache_entry(TypenameGetTypid("time"), datum_time_to_scm, scm_to_datum_time);
    insert_type_cache_entry(TypenameGetTypid("interval"), datum_interval_to_scm, scm_to_datum_interval);

    /* Initialize the Guile interpreter */
    scm_init_guile();

    scm_eval_string(scm_from_locale_string((const char *)src_scruple_scm));

    /* Procedures defined by define-record are inlinable, meaning that instead of being
       procedures, they are actually syntax transformers.  In non-call contexts, they refer to
       the original procedure, but in call contexts, they interpolate the body of the
       procedure. The upshot to this is that using scm_c_lookup gets the syntax transfomer
       procedure, not the inlined procedure. To fix this, we just eval the name, inducing
       the syntax transformer to give us proc we need.
     */
    date_day_proc         = scm_eval_string(scm_from_locale_string("date-day"));
    date_hour_proc        = scm_eval_string(scm_from_locale_string("date-hour"));
    date_minute_proc      = scm_eval_string(scm_from_locale_string("date-minute"));
    date_month_proc       = scm_eval_string(scm_from_locale_string("date-month"));
    date_nanosecond_proc  = scm_eval_string(scm_from_locale_string("date-nanosecond"));
    date_second_proc      = scm_eval_string(scm_from_locale_string("date-second"));
    date_year_proc        = scm_eval_string(scm_from_locale_string("date-year"));
    date_zone_offset_proc = scm_eval_string(scm_from_locale_string("date-zone-offset"));
    is_date_proc          = scm_eval_string(scm_from_locale_string("date?"));
    is_time_proc          = scm_eval_string(scm_from_locale_string("time?"));
    make_date_proc        = scm_eval_string(scm_from_locale_string("make-date"));
    make_time_proc        = scm_eval_string(scm_from_locale_string("make-time"));
    time_duration_symbol  = scm_eval_string(scm_from_locale_string("time-duration"));
    time_monotonic_symbol = scm_eval_string(scm_from_locale_string("time-monotonic"));
    time_nanosecond_proc  = scm_eval_string(scm_from_locale_string("time-nanosecond"));
    time_second_proc      = scm_eval_string(scm_from_locale_string("time-second"));

    is_float4_proc        = scm_variable_ref(scm_c_lookup("float4-compatible?"));
    is_float8_proc        = scm_variable_ref(scm_c_lookup("float8-compatible?"));
    is_int2_proc          = scm_variable_ref(scm_c_lookup("int2-compatible?"));
    is_int4_proc          = scm_variable_ref(scm_c_lookup("int4-compatible?"));
    is_int8_proc          = scm_variable_ref(scm_c_lookup("int8-compatible?"));

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

        Oid arg_type = get_fn_expr_argtype(fcinfo->flinfo, i);
        Datum arg = PG_GETARG_DATUM(i);

        SCM scm_arg = datum_to_scm(arg, arg_type);

        arg_list = scm_cons(scm_arg, arg_list);
    }

    result = convert_result_to_datum(scm_apply(proc, arg_list, SCM_EOL), proc_tuple, fcinfo);

    ReleaseSysCache(proc_tuple);

    PG_RETURN_DATUM(result);
}

Datum
convert_result_to_datum(SCM result, HeapTuple proc_tuple, FunctionCallInfo fcinfo) {

    TypeFuncClass typefunc_class;
    Oid rettype_oid;
    TupleDesc tuple_desc;

    typefunc_class = get_call_result_type(fcinfo, &rettype_oid, &tuple_desc);

    elog(NOTICE, "convert_result_to_datum: return type oid: %d", rettype_oid);

    switch (typefunc_class) {

    case TYPEFUNC_SCALAR:	    /* scalar result type                      */
        return scm_to_datum(result, rettype_oid);

    case TYPEFUNC_COMPOSITE:	    /* determinable rowtype result             */
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

    // Look up the function's source code, etc., using the functionOid

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

SCM
datum_to_scm(Datum datum, Oid type_oid) {
    bool found;

    TypeCacheEntry *entry = (TypeCacheEntry *) hash_search(
        typeCache,
        &type_oid,
        HASH_FIND,
        &found);

    if (found && entry->to_scm) {
        return entry->to_scm(datum);
    } else {
        elog(ERROR, "Conversion function for type OID %u not found", type_oid);
        // Unreachable
        return SCM_EOL;
    }
}

Datum
scm_to_datum(SCM scm, Oid type_oid) {
    bool found;

    TypeCacheEntry *entry = (TypeCacheEntry *) hash_search(
        typeCache,
        &type_oid,
        HASH_FIND,
        &found);

    if (found && entry->to_datum) {
        return entry->to_datum(scm);
    } else {
        elog(ERROR, "Conversion function for type OID %u not found", type_oid);
        // Unreachable
        return (Datum) 0;
    }
}

SCM
datum_int2_to_scm(Datum x) {
    return scm_from_short(DatumGetInt16(x));
}

Datum
scm_to_datum_int2(SCM x) {

    if (!is_int2(x)) {
        elog(ERROR, "int2 result expected, not: %s", scm_to_string(x));
    }

    return Int16GetDatum(scm_to_short(x));
}

SCM
datum_int4_to_scm(Datum x) {
    return scm_from_int32(DatumGetInt32(x));
}

Datum
scm_to_datum_int4(SCM x) {

    if (!is_int4(x)) {
        elog(ERROR, "int4 result expected, not: %s", scm_to_string(x));
    }

    return Int32GetDatum(scm_to_int32(x));
}

SCM
datum_int8_to_scm(Datum x) {
    return scm_from_int64(DatumGetInt64(x));
}

Datum
scm_to_datum_int8(SCM x) {

    if (!is_int8(x)) {
        elog(ERROR, "int8 result expected, not: %s", scm_to_string(x));
    }

    return Int64GetDatum(scm_to_int64(x));
}

SCM
datum_float4_to_scm(Datum x) {
    return scm_from_double((double)DatumGetFloat4(x));
}

Datum
scm_to_datum_float4(SCM x) {

    if (!is_float4(x)) {
        elog(ERROR, "float4 result expected, not: %s", scm_to_string(x));
    }

    return Float4GetDatum(scm_to_double(x));
}

SCM
datum_float8_to_scm(Datum x) {
    return scm_from_double(DatumGetFloat8(x));
}

Datum
scm_to_datum_float8(SCM x) {

    if (!is_float8(x)) {
        elog(ERROR, "float8 result expected, not: %s", scm_to_string(x));
    }

    return Float8GetDatum(scm_to_double(x));
}

SCM
datum_text_to_scm(Datum x) {

    char *cstr = text_to_cstring(DatumGetTextP(x));
    SCM scm_str = scm_from_locale_string(cstr);

    pfree(cstr);

    return scm_str;
}

Datum
scm_to_datum_text(SCM x) {

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
datum_bytea_to_scm(Datum x) {

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
scm_to_datum_bytea(SCM x) {

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
datum_timestamptz_to_scm(Datum x) {

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
scm_to_datum_timestamptz(SCM x) {

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
datum_date_to_scm(Datum x) {
    return datum_timestamptz_to_scm(datum_date_to_timestamptz(x));
}

Datum
scm_to_datum_date(SCM x) {
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
datum_time_to_scm(Datum x) {

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
scm_to_datum_time(SCM x) {
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

#define NS_PER_USEC 1000
#define SECS_PER_MONTH (DAYS_PER_MONTH * SECS_PER_DAY)

SCM
datum_interval_to_scm(Datum x)
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
scm_to_datum_interval(SCM x) {

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
datum_void_to_scm(Datum x) {
    return SCM_UNDEFINED;
}

Datum
scm_to_datum_void(SCM x) {
    return (Datum) 0;
}

bool
is_date(SCM x) {
    return scm_is_true(scm_call_1(is_date_proc, x));
}

bool
is_time(SCM x) {
    return scm_is_true(scm_call_1(is_time_proc, x));
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
is_float4(SCM x) {
    return scm_is_true(scm_call_1(is_float4_proc, x));
}

bool
is_float8(SCM x) {
    return scm_is_true(scm_call_1(is_float8_proc, x));
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

    SCM proc = scm_eval_string(scm_from_locale_string("(lambda (x) (with-output-to-string (lambda () (display x))))"));
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
