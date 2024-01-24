# Scheme PostgreSQL Language Extension (Scruple)

This project joins my favorite database to my favorite programming
language.

## Status

Early alpha. Scruple is feature-complete. Current stable(ish) release
is 0.18.0, though be warned that anything off of the knife edge of the
happy path is likely to cause a crash.

## Overview

This project provides a PostgreSQL extension that enables the creation
of database functions using Guile Scheme 3.0.

## Features

- Define and run PostgreSQL functions in Guile Scheme
- Comprehensive testing suite

## Roadmap

The scope of the 1.0 release is detailed below.

The 0.19.x versions will normalize error messages and properly trap
and report errors from Guile. Testing will be expanded from just the
happy path to include corner, edge, and adversarial cases.

At version 0.20.0 the extension will be functionally complete. This
will be the alpha release.

The 0.21.x versions will refine and refactor scruple.scm.

The 0.23.x versions will include an extensive test campaign covering
load, concurrency, session isolation, memory correctness, fuzzing,
etc.

Version 0.24.0 will be the pre-1.0 beta release.

The 0.24.x versions will include

- documentation and examples
- support for Postgres 12, 13, 15, and 16
- packaging for pgxn, deb, rpm, and apk.

The 0.25.x releases will include extensions and usability updates
after use in at least one large project.

Post 1.0 improvements:

The 1.1.x versions will adapt Racket's SQL interface.

The 1.3.x versions will add support for the foreign data wrapper
interface.

The ?.?.x versions will add support for parse tree representation
in event trigger handlers.

## Requirements

- PostgreSQL 14
- Guile 3.0
- Ubuntu 22.04

## Development Setup

Install docker.  To create the test image for Postgres 14

    test/build-container-image 14

To start a test monitor running that image

    test/start-test-container 14 && docker logs -f plg3-test-14

That starts the Postgres cluster, builds and installs `pgTAP`
and `scruple` and then enters an infinite loop containing
`inotifywait` watching a few key dependencies. When one of those
changes, it rebuilds and reinstalls `scruple` and then calls
`pg_prove` on the tests in `test/tests.sql`.

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
