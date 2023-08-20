-- scruple--1.0.sql

CREATE FUNCTION scruple_call()
RETURNS language_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION scruple_call_inline(internal)
RETURNS language_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION scruple_compile(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE LANGUAGE scruple
    HANDLER scruple_call
    INLINE scruple_call_inline
    VALIDATOR scruple_compile;

-- Add any additional SQL objects needed for the extension here.
