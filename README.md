# Guile Scheme PostgreSQL Language Extension (Scruple)

## Status

Demoable. Scruple supports creating scheme functions which can accept
and return nearly all built-in Postgres types.

## Overview

This project provides a PostgreSQL extension that enables the creation
of database functions using Guile Scheme.

## Features

- Define and run PostgreSQL functions in Guile Scheme
- Comprehensive testing suite

## Roadmap

The scope of what might constitute a 1.0.0 release isn't clear yet.
At a mimimum, it will include support for all built-in types, for
executing both scheme functions and inline scheme code, for the
ability to build, prepare, and execute queries, and for global
settings managing the Guile environment.  Postgres has a complex
security and authorization model, and a bit of dynamic scoping in that
the value of the `search_path` setting can alter the behavior of
`plpgsql` functions.  A v1 release should isolate function bodies from
each other in a way that does not sabotage security -- at the very
least -- and possibly allows the same sort of scoping changes as in
`plpgsql` using `search_path`, even though that is of questionable
utility. A v1 release should also include sufficient documentation to
make the project useful.

For now, work will continue to add new type support in 0.3.x versions,
culminating in the 0.4.0 version with fully-tested implementations of
all scalar types described in Postgres' documentation [Chapter 8: Data
Types](https://www.postgresql.org/docs/current/datatype.html). Support
for `jsonb` type translation is not yet complete, and only happy-path
testing is currently present for nearly all type transformations.

The 0.5.x versions will provide Guile primitives for accessing
Postgres' Server Programming Interface so that queries may be made
from scheme code. In addition, support for arrays, records, and set
returning functions will be added.

The 0.7.x versions will add support for triggers.

The 0.9.x versions will provide function isolation, and initialization
and configuration settings.

## Requirements

- PostgreSQL 14
- Guile 3.0
- Ubuntu 22.04

## Development Setup

Install docker.  To create the test image

    test/build-container-image

To start a test monitor running Postgres 14

    test/start-test-container 14 && docker logs -f scruple-test-14

That runs `pg-start` builds and installs `pgTAP` and `scruple` and
then enters an infinite loop containing `inotifywait` watching a few
key dependencies. When one of those changes, it rebuilds and
reinstalls `scruple` and then calls `pg_prove` on the tests in
`test/tests.sql`.

## Installation

For now, it's just a source install.

```bash
git clone https://github.com/imofftoseethewizard/scruple.git
cd scruple
make
sudo make install
```

I'll put this on `pgxn` at some point in the future.

## Usage

Once installed, you can use the extension within PostgreSQL by
creating functions in Guile Scheme. See the `doc/` directory for
detailed documentation and examples.

## Testing

Test code, scripts, and data are located in the `test/` directory. To
run the tests, execute:

```bash
make test
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact

For questions, comments, or suggestions, please open an issue on GitHub.

## History

This project is a test of using ChatGPT on a green-field programming
task. It was started in early August of 2023.
