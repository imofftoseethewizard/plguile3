begin;

select plan(37);

create extension scruple;

--------------------------------------------------------------------------------
--
-- Function call protocol tests
--

create function f_void() returns void as ' ''() ' language scruple;
select lives_ok('select f_void()', 'protocol: void return does not raise');

create function f_const() returns int as '1' language scruple;
select is(f_const(), 1, 'protocol: no arguments scalar constant result');

create function f_inc(a int) returns int as '(+ a 1)' language scruple;
select is(f_inc(2), 3, 'protocol: increment int');

create function f_inc_out(a int, b out int) returns int as '(+ a 1)' language scruple;
select is(f_inc_out(4), 5, 'protocol: increment int out 1');

create function f_sum(a int, b int) returns int as '(+ a b)' language scruple;
select is(f_sum(6, 7), 13, 'protocol: sum of arguments');

create function f_sum_default(a int, b int = 8) returns int as '(+ a b)' language scruple;
select is(f_sum_default(9), 17, 'protocol: sum of arguments using default');

create function f_two_out(a int, b out int, c out int) returns record as
'(values (+ a 1) (* a 2))' language scruple;
select is(f_two_out(10), (11, 20), 'protocol: two outputs');

create function f_double_in_out(a inout int, b inout int) returns record as
'(values (- a b) (* a b))' language scruple;
select is(f_double_in_out(12, 13), (-1, 156), 'protocol: inout params');

--------------------------------------------------------------------------------
--
-- Type smallint/int2
--

create function f_small_sum(a smallint, b smallint) returns smallint as '(+ a b)'
language scruple;

select is(f_small_sum(1::smallint, 2::smallint), 3::smallint, 'int2: simple sum');
select throws_ok(
  'select f_small_sum(32767::smallint, 1::smallint)',
  'int2 result expected, not: 32768',
  'int2: overflow check');

select throws_ok(
  'select f_small_sum(-32767::smallint, -2::smallint)',
  'int2 result expected, not: -32769',
  'int2: underflow check');

--------------------------------------------------------------------------------
--
-- Type int/int4
--

select throws_ok(
  'select f_sum(2147483647, 1)',
  'int4 result expected, not: 2147483648',
  'int4: overflow check');

select throws_ok(
  'select f_sum(-2147483648, -1)',
  'int4 result expected, not: -2147483649',
  'int4: underflow check');

--------------------------------------------------------------------------------
--
-- Type bigint/int8
--

create function f_big_sum(a bigint, b bigint) returns bigint as '(+ a b)'
language scruple;

select is(f_big_sum(1::bigint, 2::bigint), 3::bigint, 'int8: simple sum');
select throws_ok(
  'select f_big_sum(9223372036854775807::bigint, 1::bigint)',
  'int8 result expected, not: 9223372036854775808',
  'int8: overflow check');

select throws_ok(
  'select f_big_sum(-9223372036854775807::bigint, -2::bigint)',
  'int8 result expected, not: -9223372036854775809',
  'int8: underflow check');

--------------------------------------------------------------------------------
--
-- Type real/float4
--

create function f_real(a real, b real) returns real as '(* a b)' language scruple;

select is(f_real(2.0::real, 1.5::real), 3.0::real, 'float4: simple sum');
select is(f_real(3.402823e38::real, 2.0::real), 'inf'::real, 'float4: inf overflow');
select is(f_real(1.4e-45::real, 0.5::real), 0.0::real, 'float4: 0 underflow');
select is(f_real(-3.402823e38::real, 2.0::real), '-inf'::real, 'float4: negative inf overflow');
select is(f_real(-1.4e-45::real, 0.5::real), 0.0::real, 'float4: negative 0 underflow');
select is(f_real('nan'::real, 0.5::real), 'nan'::real, 'float4: nan propagation');

--------------------------------------------------------------------------------
--
-- Type double precision/float8
--

create function f_dp(a double precision, b double precision) returns double precision as
'(* a b)' language scruple;

select is(f_dp(2.0, 1.5), 3.0::double precision, 'float8: simple sum');
select is(f_dp(1.7976931348623157e308, 2.0), 'inf', 'float8: inf overflow');
select is(f_dp(5e-324, 0.5), 0.0::double precision, 'float8: 0 underflow');
select is(f_dp(-1.7976931348623157e308, 2.0), '-inf', 'float8: negative inf overflow');
select is(f_dp(-5e-324, 0.5), 0.0::double precision, 'float8: negative 0 underflow');
select is(f_dp('nan', 0.5), 'nan'::double precision, 'float8: nan propagation');

--------------------------------------------------------------------------------
--
-- Type text
--

create function f_text_in(a text) returns int as '(string-length a)' language scruple;
create function f_text_out(a int) returns text as '(format #f "~d" a)' language scruple;
create function f_text_error(a int) returns text as 'a' language scruple;

select is(f_text_in('Hello, World!'), 13, 'text: text input');
select is(f_text_out(13), '13', 'text: text output');
select throws_ok(
       'select f_text_error(13)',
       'string result expected, not: 13',
       'text: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type char
--

create function f_char_in(a char) returns int as '(string-length a)' language scruple;
create function f_char_out(a int) returns char as '(format #f "~d" a)' language scruple;
create function f_char_error(a int) returns char as 'a' language scruple;

select is(f_char_in('Hello, World!'), 13, 'char: char input');
select is(f_char_out(13), '13', 'char: char output');
select throws_ok(
       'select f_char_error(13)',
       'string result expected, not: 13',
       'char: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type varchar
--

create function f_varchar_in(a varchar) returns int as '(string-length a)' language scruple;
create function f_varchar_out(a int) returns varchar as '(format #f "~d" a)' language scruple;
create function f_varchar_error(a int) returns varchar as 'a' language scruple;

select is(f_varchar_in('Hello, World!'), 13, 'varchar: varchar input');
select is(f_varchar_out(13), '13', 'varchar: varchar output');
select throws_ok(
       'select f_varchar_error(13)',
       'string result expected, not: 13',
       'varchar: wrong return type check');

select * from finish();

rollback;
