#include "postgres.h"
#include "fmgr.h"
#include "libguile.h"

PG_MODULE_MAGIC;

PGDLLEXPORT Datum scruple_call_handler(PG_FUNCTION_ARGS);
PGDLLEXPORT Datum scruple_inline_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(scruple_call_handler);
PG_FUNCTION_INFO_V1(scruple_inline_handler);

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
    /* Initialize the Guile interpreter */
    scm_init_guile();
}

void _PG_fini(void) {
    /* Clean up Guile interpreter */
}

Datum scruple_call_handler(PG_FUNCTION_ARGS) {
    /* Handle a call to a Guile function */
    elog(NOTICE, "Hello, world!");
    PG_RETURN_NULL();
}

Datum scruple_inline_handler(PG_FUNCTION_ARGS) {
    /* Handle an inline Guile statement */
    elog(NOTICE, "Hello, world!");
    PG_RETURN_NULL();
}
