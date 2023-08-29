# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
