# Guile Scheme PostgreSQL Language Extension (Scruple)

## Status

Unuseless. It can accept integer arguments and return integers.

## Overview

This project provides a PostgreSQL extension that enables the creation
of database functions using Guile Scheme. It allows developers to
write database functions in Scheme and execute them within the
PostgreSQL server.

## Features

- Define and run PostgreSQL functions in Guile Scheme
- Execute SQL queries from within Scheme code
- Comprehensive testing suite

## Roadmap

The scope of what might constitute a 1.0.0 release isn't clear yet.
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

1. Clone the repository:
```bash
git clone https://github.com/imofftoseethewizard/scruple.git
```

2. Navigate to the project directory:
```bash
cd scruple
```

3. Build the extension:
```bash
make
```

4. Install the extension:
```bash
sudo make install
```

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
task. It was started in early August of 2023.  So far, about 6 hours
of time has gone into this, and it's just a complicated version of
"Hello, World!" at this point.  The one useful bit currently preset is
that it could be used as a starting point for any Postgres extension.
