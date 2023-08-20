#include "libguile.h"

#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "lib/stringinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"

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
    SCM callable;
} FuncCacheEntry;

typedef struct {
    Oid type_oid;
    SCM scm_type;
} TypeCacheEntry;

static HTAB *funcCache;
static HTAB *typeCache;

void _PG_init(void) {

    HASHCTL funcInfo;
    HASHCTL typeInfo;

    /* Initialize hash tables */
    memset(&funcInfo, 0, sizeof(funcInfo));
    funcInfo.keysize = sizeof(Oid);
    funcInfo.entrysize = sizeof(FuncCacheEntry);
    funcCache = hash_create("Scruple Function Cache", 128, &funcInfo, HASH_ELEM | HASH_BLOBS);

    memset(&typeInfo, 0, sizeof(typeInfo));
    typeInfo.keysize = sizeof(Oid);
    typeInfo.entrysize = sizeof(TypeCacheEntry);
    typeCache = hash_create("Scruple Type Cache", 128, &typeInfo, HASH_ELEM | HASH_BLOBS);

    /* Initialize the Guile interpreter */
    scm_init_guile();
}

void _PG_fini(void) {

    /* Clean up Guile interpreter */
    HASH_SEQ_STATUS status;
    FuncCacheEntry *entry;

    hash_seq_init(&status, funcCache);
    while ((entry = (FuncCacheEntry *) hash_seq_search(&status)) != NULL) {
        // Assuming entry->compiled_proc is the protected SCM object
        scm_gc_unprotect_object(entry->callable);
    }

    hash_seq_term(&status);

    /* Clean up hash tables */
    hash_destroy(funcCache);
    hash_destroy(typeCache);
}

Datum scruple_call(PG_FUNCTION_ARGS) {

    /* Handle a call to a Guile function */
    Oid func_oid = fcinfo->flinfo->fn_oid;
    HeapTuple proc_tuple;
    // Form_pg_proc proc_struct;
    Datum prosrc_datum;
    bool isnull;
    char *prosrc;
    SCM lambda;

    elog(NOTICE, "scruple_call: begin");

    // Fetch tuple from pg_proc for the given function Oid
    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "Failed to fetch function details.");

    // Extract information from the tuple
    // proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
    prosrc_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &isnull);
    prosrc = TextDatumGetCString(prosrc_datum);

    // Now you have the function source in prosrc. Use it as required.

    ReleaseSysCache(proc_tuple);

    elog(NOTICE, "%s", prosrc);

    // TODO: check that `SHOW server_encoding` is 'UTF8'

    lambda = scm_eval_string(scm_from_utf8_string(prosrc));
    scm_gc_protect_object(lambda);
    scm_call_0(lambda);
    scm_gc_unprotect_object(lambda);

    PG_RETURN_NULL();
}

Datum scruple_call_inline(PG_FUNCTION_ARGS) {
    /* Handle an inline Guile statement */
    elog(NOTICE, "Hello, world!");
    PG_RETURN_NULL();
}

Datum scruple_compile(PG_FUNCTION_ARGS) {

    Oid func_oid = PG_GETARG_OID(0);
    HeapTuple proc_tuple;
    Datum prosrc_datum;
    char *prosrc;
    bool is_null;

    Form_pg_proc proc_struct;
    //Datum proc_name_datum;

    Datum argnames_datum;
    ArrayType *argnames_array;
    int num_args, i;

    StringInfoData buf;

    elog(NOTICE, "scruple_compile: begin");

    // Look up the function's source code, etc., using the functionOid

    proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));

    if (!HeapTupleIsValid(proc_tuple))
        elog(ERROR, "scruple_compile: Failed to fetch function details.");

    // Extract information from the tuple
    prosrc_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prosrc, &is_null);
    prosrc = TextDatumGetCString(prosrc_datum);

    // TODO check is_null

    // Now you have the function source in prosrc. Use it as required.

    proc_struct = (Form_pg_proc) GETSTRUCT(proc_tuple);
    //proc_name_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proname, &is_null);
    SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proname, &is_null);

    initStringInfo(&buf);

    // char *string_for_guile = buf.data; // This is the char* you can pass to Guile

    if (!is_null) {
        char *proc_name;
        proc_name = NameStr(proc_struct->proname);
        elog(NOTICE, "scruple_compile: function name: %s", proc_name);

        appendStringInfo(&buf, "(define (%s", proc_name);
    }

    argnames_datum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargnames, &is_null);

    if (!is_null) {
        argnames_array = DatumGetArrayTypeP(argnames_datum);
        num_args = ARR_DIMS(argnames_array)[0];
        elog(NOTICE, "scruple_compile: num args: %d", num_args);
        for (i = 1; i <= num_args; i++) {
            Datum name_datum = array_get_element(argnames_datum, 1, &i, -1, -1, false, 'i', &is_null);
            if (!is_null) {
                char *name = TextDatumGetCString(name_datum);
                elog(NOTICE, "scruple_compile: parameter name: %s", name);
                appendStringInfo(&buf, " %s", name);

                // name contains the parameter name
            }
            else {
                elog(NOTICE, "scruple_compile: %d: name_datum is null", i);
            }
        }
    }
    else {
        elog(NOTICE, "argnames_datum is null");
    }

    appendStringInfo(&buf, ")\n%s)", prosrc);
    scm_eval_string(scm_from_locale_string(buf.data));
    //SCM foo_function = scm_variable_ref(scm_c_lookup("foo"));

    elog(NOTICE, "%s", buf.data);

    // TODO: check that `SHOW server_encoding` is 'UTF8'

    // Perform syntax checks, compilation, etc., specific to scruple

    // If something's wrong, report an error
    /* if (error_condition) { */
    /*     ereport(ERROR, (errmsg("Something went wrong in scruple_compile"))); */
    /* } */

    elog(NOTICE, "scruple_compile: complete");

    ReleaseSysCache(proc_tuple);

    // Non-zero result indicates that the compilation was successful.
    return 1;
}
