# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

This release lays some of the groundwork for integrating with the
Server Programming Interface. In particular, instead of eagerly
converting Datum values to SCM values, they are now wrapped by a
record type, `boxed-datum`, to be converted as needed within Scheme
code. This, of course, breaks many tests.

## [0.4.0] - 2023-09-20

This release includes more tests of the conversion of values between
Postgres and Guile. There are still code paths which will cause a
crash and connection reset due to an unexpected value type returned by
the scheme code, but rather than handling these explicitly in C, a
later version of scruple will handle them generically by capturing
Guile exceptions and reporting those instead. Additionally, the module
scruple.scm will be improved to prevent the formation of records with
invalid fields.

## [0.3.4] - 2023-09-17

This release includes support for `timetz` and `jsonb` types.

## [0.3.3] - 2023-09-16

This release includes support for most of the rest of the built-in
scalar types. Notably `timetz` and `jsonb` are still in progress.

This so far is about 50 hours of work.

## [0.3.2] - 2023-09-10

This release includes support for `bytea`, `timestamptz`, `timestamp`,
`time`, `date`, and `interval`.

ChatGPT has been generally helpful in roughly sketching solutions,
though the details are never completely correct, and although it's
definitely a benefit and a time saver, the entire experience feels
clumsy. At it's best, I describe my intent and I get nearly working
code, but this is rare and only for the simplest requests.

This is so far about 35 hours of work.

## [0.3.1] - 2023-09-09

This release includes support for `char(n)`, `varchar(n)` and `text`
types.

I spent some time working on the `numeric` type, and created an srfi-9
record type for its scheme representation and wrote out the from
Postgres to Guile conversion. Only when I went to compile it did I
realize that ChatGPT had given me instructions from the non-public C
file, not the public interface, so it was all for naught.  That said,
it's help made the string types quick to support.

This is so far about 26 hours of work.

## [0.3.0] - 2023-09-09

This release introduces a file to hold auxiliary scheme code used in
type conversions -- `src/scruple.scm` -- and the tooling to build that
into `scruple.so` as a static string.  It also includes support for
floating point types `float4` / `real` and `float8` / `double
precision`.  The `build-monitor` tool was updated to include the new
file and to watch the make file as well.

ChatGPT for some reason thought that `xxd` as part of package
`vim-common` when in fact it is in its own package.  It was very
helpful in finding that tool and setting up the make file to build the
auxiliary scheme code into the extension's library file.

This is so far about 24 hours of work.

## [0.2.0] - 2023-09-09

This release includes a modest test suite that covers the function
call protocol. The increment in the minor version signifies a stable
point, here regarding only the mechanincs of defining and calling
Guile functions from Postgres. Although the changelog format doesn't
specify that even numbered minor versions are stable, that is a common
usage, and despite that the sub 1.0.0 versions are more or less
unstructured, I'll adopt the practice now to get in the habit.

This is so far about 22 hours of work.

## [0.1.2] - 2023-09-05

This release includes docker-based local test tooling. To set up

    test/build-container-image

To start a test monitor running Postgres 14

    test/start-test-container 14 && docker logs -f scruple-test-14

That runs `pg-start` builds and installs `pgTAP` and `scruple` and
then enters an infinite loop containing `inotifywait` watching
`src/scruple.c` and `test/tests.sql`. When either of those changes, it
rebuilds and reinstalls `scruple` and then calls `pg_prove` on the
tests in `test/tests.sql`.

ChatGPT helped in some minor bash and docker nuances and led me to
`pgTAP` and `pgxn/pgxn-tools`.

This is so far about 20 hours of work.

## [0.1.1] - 2023-09-04

The implementation of the call handler's result marshalling was
completely rewritten.  While ChatGPT helped me write the initial
version, it was needlessly long and did not take advantage of the
features of `funcapi.h`.

Also included in this release is a partial implementation of some test
tooling.  It is based on the `pgxn/pgxn-tools` dockerfile, but
extended to include the build dependencies for guile and pgtap.  Yet
to be done is the integration with `pg-start` to install pgtap, and
with `pg_prove` to do setup/teardown of scruple.

This is so far about 18 hours of work.

## [0.1.0] - 2023-09-02

This release includes a minor version update to mark the completion of
the call handler interaction protocol between Postgres and Guile.
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
