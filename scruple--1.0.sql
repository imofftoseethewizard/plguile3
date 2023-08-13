-- scruple--1.0.sql

CREATE FUNCTION scruple_call_handler()
RETURNS language_handler
AS 'MODULE_PATHNAME', 'scruple_call_handler'
LANGUAGE C STRICT;

CREATE FUNCTION scruple_inline_handler(internal)
RETURNS language_handler
AS 'MODULE_PATHNAME', 'scruple_inline_handler'
LANGUAGE C STRICT;

CREATE LANGUAGE scruple
    HANDLER scruple_call_handler
    INLINE scruple_inline_handler;

-- Add any additional SQL objects needed for the extension here.
