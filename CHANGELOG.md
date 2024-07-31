# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.21.24] - 2024-07-31

This version renames the module management functions in the plguile3
schema not to have the `guile3_` prefix. It also fixes a memory error
that occurred when loading module source text from the module table.

## [0.21.23] - 2024-07-30

This version removes `define-public-module` and related code.  It is
simpler to provide only `define-module` and have a second procedure
which publishes it.  Additionally, this version adds functions to read
modules from the file system. It also changes the type of module name
parameters from `text[]` to `text` (space delimited), as that should
be more convenient.  Lastly, it provides functions to control whether
or not a module is public or private to a user.

## [0.21.22] - 2024-07-28

This version adds some management functions to make a user module
public and to make a public module belong to a user.

## [0.21.21] - 2024-07-27

This version modifies plguile3_eval to return the text representation
of the last form evaluated.

## [0.21.20] - 2024-07-26

This version adds an eval function to enable module creation outside
of a DO statements. DO statements are restrictive in that they are
syntactically limited to literal strings for code.

## [0.21.19] - 2024-07-25

This version contains numerous fixes for issues found during ad hoc
testing.

## [0.21.18] - 2024-07-18

This version improves output to elog via textual ports. It's now
line-buffered and unquoted.

## [0.21.17] - 2024-07-17

This version implements `use-modules` for the trusted environment.

## [0.21.16] - 2024-07-15

This version removes module validation and related diagnostic code.
Currently the remaining obvious isses with the module system and
related work are the lack of a proper `use-modules` wrapper, and the
need to replace the current simple output port to elog adapter with a
line buffering adapter.

## [0.21.15] - 2024-07-15

This version includes some partial fixes for module validation, a lot
of diagnostic support code, and a few small refinements.  The module
validation code is misguided and will be removed in the next version.
This version exists solely to memorialize it in its final state in the
history.

## [0.21.14] - 2024-07-09

This version contains fixes from initial ad hoc testing.  There are
still significant bugs in the simplest uses.

## [0.21.13] - 2024-07-04

This version adds administrative functions for getting module source
and deleting modules. Module support is now code complete but untested
(and it definitely has bugs).

## [0.21.12] - 2024-07-03

This version completes module functionality.  Included are module
definitions for curated builtin modules, fresh module allocation for
inline block evaluations and function definitions, and the addition of
@, define-module, define-public-module, and use-module to the sandbox.
Remaining work on modules includes a set of CRUD functions will be
defined to manage modules and a suite of tests.

## [0.21.11] - 2024-07-02

This version includes several small changes:

- adds default ports which send their output to Postgres' elog function
- flushes all module and compiled code caches in all backends when a
  module changes
- module loading in both `#:use-module` parameters to `define-module`
  and `use-module` forms will now load from storage when the module
  name begins with `'trusted`.

## [0.21.10] - 2024-06-30

This version factors out primitive definitions into a separate module
and installs default ports that redirect to ereport.

## [0.21.9] - 2024-06-27

This version includes a first pass at a curated list of built-in
module bindings.

## [0.21.8] - 2024-06-22

This version completes the module validation logic.  There are no
tests for these changes.

## [0.21.7] - 2024-06-20

This version includes another portion of the module validation logic,
loading module names from the modules table.  It also includes
completed logic for the outer loop over the modules.  There are no
tests for these changes.

## [0.21.6] - 2024-06-19

This version includes a sketch of validating a user's module source
prior to committing it to storage.  There are no tests for these
changes.

## [0.21.5] - 2024-06-17

This version adds support for saving module source to the module
table.  There are no tests for these changes.

## [0.21.4] - 2024-06-16

This version replaces "preamble" with "prelude" in the definition and
management of default and user-specific scheme environments.

## [0.21.3] - 2024-06-16

This version includes the remainder of `define-public-module`.  There
are no tests for these changes.

## [0.21.2] - 2024-06-15

This version includes preliminaries for supporting
`define-public-module`.  There are no tests for these changes.

## [0.21.1] - 2024-06-14

This version includes the logic to resolve module names for use in
`(use-modules ...)` forms and `#:use-module` parameters to
`define-module`.  There are no tests for these changes.

## [0.21.0] - 2024-06-11

This version provides some preliminaries for a trusted module system.
When complete, this will provide define-module, use-module, @, and @@
with identical semantics, though restricted to only trusted modules.
There are no tests for these changes.

## [0.20.0] - 2024-03-18

This version ensures that row trigger responses are allocated in the
context of the top transaction.

## [0.19.19] - 2024-03-17

This version cleans up usage of memory contexts.  No additional
functionality or tests.

## [0.19.18] - 2024-03-15

This version fixes statement-level triggers and adds support for
transition relations in `(execute ...)` during triggers.  See
`REFERENCING NEW AS ...`  in the `CREATE TRIGGER` documentation.

## [0.19.17] - 2024-03-14

This version fixes a defect in the compiled function cache, where
equality was not correctly evaluated between the source hash of the
current version of the function body and the source hash in the cache.

## [0.19.16] - 2024-03-13

This version fixes null `'()` arguments in the arguments list of
`execute` and adds some tests around error handling of bad parameters
to that procedure.

## [0.19.15] - 2024-03-12

This version allows nonatomic queries to be executed with `(execute
"command" ...)`.  It also unifies parameter handling between
applications of `execute` with a receiver and without.

## [0.19.14] - 2024-03-11

This version adds support for `start-transaction`, `commit`,
`commit-and-chain`, `rollback`, and `rollback-and-chain`.

## [0.19.13] - 2024-03-10

This version guards against pathological function definitions,
ensuring that the defining lambda is the only readable value in the
source assembled from the function body and its formal parameters.

## [0.19.12] - 2024-03-10

This version adds `notice` and `warning` primitives to Guile as an
easy way to `(execute "do $$ begin raise notice, '...'; end $$")`.  It
also adds a compilation check on update preamble, so that errors are
reported promptly, and not at function compile time when the cause of
the error would be more obscure.

## [0.19.11] - 2024-03-07

This version adds tests for user isolation and user environments,
verifying the correct behavior of per-role preambles and call time and
allocation limits.  With this verification, the plguile3 is now a
trusted language extension.  It also includes an improved local test
script which allows modifications of the files watched and tests run
without restarting the test container.

## [0.19.10] - 2024-03-01

This version adds the basic structure for testing behavior between
concurrent database sessions.

## [0.19.9] - 2024-02-27

This version supports immutable and stable functions and prohibits
them from directly calling INSERT, UPDATE, or DELETE using the
`read_only` option to the underlying SPI method.

## [0.19.8] - 2024-02-25

This version checks that all parameters and result conversions which
expect lists actually receive proper lists, and it logs an error or
raises an exception as appropriate when that is not true.

## [0.19.7] - 2024-02-19

This version enables jsonpath validation.

## [0.19.6] - 2024-02-18

This version ensures that all calls into Scheme (using `scm_call_x` or
`scm_apply_x`) are guarded with error handlers.  This should eliminate
most of the current risk of crashing.

## [0.19.5] - 2024-02-18

This version includes a minor refactor of evaluating scheme code in
the sandbox, or applying a procedure with space and time limits, and
includes proper error handling for failures to dereference symbols in
the base module.

## [0.19.4] - 2024-02-17

This version adds PG_TRY handling to the SPI execution code path.

## [0.19.3] - 2024-02-10

This version hardens the call path to SPI from Scheme and adds
`with-exception-handler` to the sandbox environment.

## [0.19.2] - 2024-02-05

This version adds backtraces to exception handling, though it's not
yet clear that this is the correct way to do it.

## [0.19.1] - 2024-02-04

This version catches exceptions raised in Scheme code and relays the
error to Postgres using `elog(ERROR, ...)`. It also wraps the
primitive constructor for `decimal` record types to ensure that only
valid values can be created.

## [0.19.0] - 2024-02-03

This version includes some minor test refinement and cleanup.

## [0.18.0] - 2024-01-24

This version adds per-role environments and per-role call limits. It
also ensures that caches of sandbox modules are flushed when
environments change, and that caches of compiled procedures are
flushed when changes to the environment, source, or ownership of a
function changes. This change is extensive and passes current tests,
but needs further testing.

## [0.17.4] - 2024-01-17

This version updates the test tooling so that postgres is built
into the container image, instead of built at container start.

## [0.17.3] - 2024-01-14

This version provides no functional changes. It is prep work for
supporting per-role evaluation environments.

## [0.17.2] - 2024-01-09

This version provides sandboxed execution of scheme code suitable for
a trusted language extension.

## [0.17.1] - 2024-01-09

This versions fills in the exports from the `(plguile3 base)` module
and provides a much more complete binding list for the sandbox.

## [0.17.0] - 2024-01-08

This version adds a roughed-in sandbox for scheme code and two server
configs to support it: `plguile3.call_time_limit` and
`plguile3.call_allocation_limit`.

## [0.16.0] - 2024-01-06

This version adds support for "do" statements.

## [0.14.0] - 2024-01-05

This version adds a test for event triggers and changes the default of
the `read-only` parameter to `#f` for the SPI integration functions
available in scheme.

## [0.13.2] - 2024-01-04

This version adds support for event trigger handlers. Untested.

## [0.13.1] - 2024-01-03

This version adds another trigger test and simplifies the calling
code.

## [0.13.0] - 2024-01-02

This version adds roughed-in support for triggers.

## [0.12.0] - 2023-12-23

This version adds supports for cursors.

## [0.11.0] - 2023-12-21

This version adds support for iterative command result handling via
`execute-with-receiver`.

## [0.10.0] - 2023-12-15

This version reworks jsonb support so that jsonb is represented in
Scheme such that objects map to alists with string keys, arrays map to
vectors, `true` maps to `#t`, `false` maps to `#f`, `null` maps to
`'null`, strings map to strings, and numbers map to numbers.

## [0.8.0] - 2023-12-12

This version adds support for multirange types and completes support
for all Postgres types (up to the rewrite of JSONB handling in
versions 0.9.x).

## [0.7.6] - 2023-12-10

This version adds support for range types and stubs some of the support for
multirange types. ChatGPT was helpful in writing `find_range_type_oid`.

## [0.7.5] - 2023-12-09

This version adds support for domain types.

## [0.7.4] - 2023-12-08

This version completes support for jsonpath.

## [0.7.3] - 2023-11-24

This version includes partial support for jsonpath. Values are loaded
into scheme from Postgres. The reverse direction is not done yet.

## [0.7.2] - 2023-11-20

This version adds support for tsquery translation to and from scheme.

## [0.7.1] - 2023-11-19

This version adds support for tsvector translation to and from scheme.
It also includes better error handling for evaluating scheme strings,
and for calling scheme procedures which accept 1 argument.

## [0.7.0] - 2023-11-12

This version adds scheme structures to support representation of
tsvector and tsquery values.

## [0.6.0] - 2023-11-12

This version provides basic support for SPI, enabling execution of
parameterized queries and translation of values from Postgres to
Scheme and back.

## [0.5.16] - 2023-11-10

The enum representation was changed from a symbol representing the
value to a pair encoding the type and the value, both symbols deriving
from the name of the Postgres type and the enum value. The conversion
from Scheme will now also convert 2-vectors to points as a convenience.

## [0.5.15] - 2023-11-5

This add support for parameterized queries where arguments are
interpolated into the command string by Postgres itself. Testing this
capability is not complete.

## [0.5.14] - 2023-11-4

This adds support for set returning functions for unspecified record
types, and it also changes the type used to represent a set of rows
from a vector to a list.

## [0.5.13] - 2023-10-31

This adds support for set returning functions, but only when it's a
composite type.

## [0.5.12] - 2023-10-29

This adds support for set returning functions, but only when it's a
set of a simple type. This is rough and will be refactored when
handling for composite types, records, etc is added. ChatGPT was only
modestly helpful here, and perhaps that's a measure of how complex or
esoteric a subject is.

## [0.5.11] - 2023-10-25

This adds a `table` record type in Scheme to represent the return
value of `SPI_execute` and updates the C code to use it. This also
includes a few utility procedures for working with tables and records
in Scheme.

## [0.5.10] - 2023-10-23

This adds support for functions returning `record`. It handles the
case where each of the values in the returned tuple is a scalar
type.

## [0.5.9] - 2023-10-22

This adds support for composite types as function parameters. It also
includes initial support for record-returning functions.

## [0.5.8] - 2023-10-21

This adds support for translating Postgres arrays to scheme vectors.

## [0.5.7] - 2023-10-15

Support is now in place for translating scheme vectors to Postgres
arrays as return values of functions. ChatGPT wrote the bulk of
`scm_to_datum_array`, although it needed a number of corrections.

## [0.5.6] - 2023-10-14

The interface for `execute` was overly complex with the `tuple-table`
and `tuple-desc` record types. Mostly callers will want just the rows,
so returning a values expression provides that by default with the
option of more complete information by using `call-with-values` or
some more convenient macro sugar.

## [0.5.5] - 2023-10-14

This adds one test for `SPI_execute` and an off-by-one fix to get it
to work.  The ergonomics aren't great. Considering switching to
returning a several values: the rows vector, the type ids vector, and
the rows affected count.

## [0.5.4] - 2023-10-14

Function definitions were reformatted to conform to "Stroustroup"
style.  The 0.4.0 value passing behavior was restored -- ie. Postgres
values are now eagerly converted to Scheme values, rather than wrapped
in `boxed-datum` record.  The more I thought about that, the worse the
idea seemed.  The big change here is the completion of `spi_execute`,
though it remains untested.

## [0.5.3] - 2023-10-10

Use tabs for indentation, continue to use spaces for alignment.

## [0.5.2] - 2023-10-10

Ads a rough sketch of the `spi-execute` primitive. No additional code,
just some commented-out broken stuff.

## [0.5.1] - 2023-10-08

This adds `unbox-datum` to the Scheme environment and adjusts the
function protocol tests to verify basic operation. The rest of the
tests are left as-is, since the goal is that they will work again once
code analysis is finished and variable references are appropriately
wrapped in `unbox-datum` expressions.

## [0.5.0] - 2023-10-08

This version lays some of the groundwork for integrating with the
Server Programming Interface. In particular, instead of eagerly
converting Datum values to SCM values, they are now wrapped by a
record type, `boxed-datum`, to be converted as needed within Scheme
code. This, of course, breaks many tests.

## [0.4.0] - 2023-09-20

This version includes more tests of the conversion of values between
Postgres and Guile. There are still code paths which will cause a
crash and connection reset due to an unexpected value type returned by
the scheme code, but rather than handling these explicitly in C, a
later version of plguile3 will handle them generically by capturing
Guile exceptions and reporting those instead. Additionally, the module
plguile3.scm will be improved to prevent the formation of records with
invalid fields.

## [0.3.4] - 2023-09-17

This version includes support for `timetz` and `jsonb` types.

## [0.3.3] - 2023-09-16

This version includes support for most of the rest of the built-in
scalar types. Notably `timetz` and `jsonb` are still in progress.

This so far is about 50 hours of work.

## [0.3.2] - 2023-09-10

This version includes support for `bytea`, `timestamptz`, `timestamp`,
`time`, `date`, and `interval`.

ChatGPT has been generally helpful in roughly sketching solutions,
though the details are never completely correct, and although it's
definitely a benefit and a time saver, the entire experience feels
clumsy. At it's best, I describe my intent and I get nearly working
code, but this is rare and only for the simplest requests.

This is so far about 35 hours of work.

## [0.3.1] - 2023-09-09

This version includes support for `char(n)`, `varchar(n)` and `text`
types.

I spent some time working on the `numeric` type, and created an srfi-9
record type for its scheme representation and wrote out the from
Postgres to Guile conversion. Only when I went to compile it did I
realize that ChatGPT had given me instructions from the non-public C
file, not the public interface, so it was all for naught.  That said,
it's help made the string types quick to support.

This is so far about 26 hours of work.

## [0.3.0] - 2023-09-09

This version introduces a file to hold auxiliary scheme code used in
type conversions -- `src/plguile3.scm` -- and the tooling to build
that into `plguile3.so` as a static string.  It also includes support
for floating point types `float4` / `real` and `float8` / `double
precision`.  The `build-monitor` tool was updated to include the new
file and to watch the make file as well.

ChatGPT for some reason thought that `xxd` as part of package
`vim-common` when in fact it is in its own package.  It was very
helpful in finding that tool and setting up the make file to build the
auxiliary scheme code into the extension's library file.

This is so far about 24 hours of work.

## [0.2.0] - 2023-09-09

This version includes a modest test suite that covers the function
call protocol. The increment in the minor version signifies a stable
point, here regarding only the mechanincs of defining and calling
Guile functions from Postgres. Although the changelog format doesn't
specify that even numbered minor versions are stable, that is a common
usage, and despite that the sub 1.0.0 versions are more or less
unstructured, I'll adopt the practice now to get in the habit.

This is so far about 22 hours of work.

## [0.1.2] - 2023-09-05

This version includes docker-based local test tooling. To set up

    test/build-container-image

To start a test monitor running Postgres 14

    test/start-test-container 14 && docker logs -f plguile3-test-14

That runs `pg-start` builds and installs `pgTAP` and `plguile3` and
then enters an infinite loop containing `inotifywait` watching
`src/plguile3.c` and `test/tests.sql`. When either of those changes,
it rebuilds and reinstalls `plguile3` and then calls `pg_prove` on the
tests in `test/tests.sql`.

ChatGPT helped in some minor bash and docker nuances and led me to
`pgTAP` and `pgxn/pgxn-tools`.

This is so far about 20 hours of work.

## [0.1.1] - 2023-09-04

The implementation of the call handler's result marshalling was
completely rewritten.  While ChatGPT helped me write the initial
version, it was needlessly long and did not take advantage of the
features of `funcapi.h`.

Also included in this version is a partial implementation of some test
tooling.  It is based on the `pgxn/pgxn-tools` dockerfile, but
extended to include the build dependencies for guile and pgtap.  Yet
to be done is the integration with `pg-start` to install pgtap, and
with `pg_prove` to do setup/teardown of plguile3.

This is so far about 18 hours of work.

## [0.1.0] - 2023-09-02

This version includes a minor version increment to mark the completion
of the call handler interaction protocol between Postgres and Guile.
While it does not yet handle all types, it handles everything you can
do with types in the context of `CREATE FUNCTION`: it properly handles
all combinations of in, out, in/out, parameters and returns an
appropriately-typed tuple, if there is more than one out or in/out
parameter.

ChatGPT hallucinated a `deform_array` function in Postgres, but with
further questioning, it generated `deconstruct_array`. It also
suggested I use `scm_call_with_values`, a function that was removed
from the Guile source code in March of 2001.  It also couldn't
identify the appropriate header file for `get_call_result_type`.  It's
significantly helpful, but the whole workflow seems clumsy.

This is so far about 13 hours of work.

## [0.0.3] - 2023-08-29

The type cache has been modified to include to/from functions and is
now initialized to convert int2, int4, and int8 values. This includes
type and range checking.

The function cache is now filled during the validate handler and
during the call handler if a compiled version of the function is not
yet in the cache.  The function compiler now omits out-only
parameters. The function call handler converts argument values from
Postgres `Datum` values to Guile `SCM` values, and converts `SCM` on
the return.  Add hoc testing has confirmed that argument handling
works with defaults and specifying arguments by name, eg `foo(x =>
42)`.

ChatGPT hallucinated a C function `scm_with_output_to_string`. Totally
understandable, since I think I've done the same thing before (or the
same kind of thing, if not with that exact function).

This so far is about 11 hours of work.

## [0.0.2] - 2023-08-20

Hash tables have been added to cache SCM values corresponding to
Postgres function oids, and for mapping type oids to SCM types.
Neither is used as yet -- just the setup and teardown are present.

There is now a validator for the language extension. It assembles a
simple define form from the information in the CREATE FUNCTION
statement, and then evaluates that form.

ChatGPT hallucinated a `scm_finalize` function, and in the iteration
of function parameters, it started the index at 0, but Postgres arrays
are 1-based. Other than that it was accurate. Curiously, so far it has
produced, `isnull`, `is_null`, and `isNull` for the name of a boolean
passed to Postgres.

This is so far about 8 hours of work.

## [0.0.1] - 2023-08-13

Stubs, setup, scripts, boilerplate, license, build are all in
place. It's little more that "Hello, World!" expressed as a Postgres
extension. Cumulative work time on this is so far about 6 hours.
