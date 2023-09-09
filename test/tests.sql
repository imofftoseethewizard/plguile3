begin;

select plan(16);

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

select * from finish();

rollback;
