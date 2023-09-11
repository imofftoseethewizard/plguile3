# Guile Scheme PostgreSQL Language Extension (Scruple)

## Status

Demoable. Scruple supports creating scheme functions which accept and
return most numeric argument types, all string types, and most date
and time types -- notably not `time with time zone` since that's
conceptually problematic, and I do not condone it.

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
settings managing the Guile environment.

For now, work will continue to add new type support in 0.3.x versions,
culminating in the 0.4.0 version with fully-tested implementations of
all scalar types described in Postgres' documentation [Chapter 8: Data
Types](https://www.postgresql.org/docs/current/datatype.html).


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
then enters an infinite loop containing `inotifywait` watching
`src/scruple.c` and `test/tests.sql`. When either of those changes, it
rebuilds and reinstalls `scruple` and then calls `pg_prove` on the
tests in `test/tests.sql`.

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
