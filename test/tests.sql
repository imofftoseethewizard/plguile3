begin;

select plan(245);

select lives_ok('create extension if not exists plg3', 'install (if not exists)');
select lives_ok('drop extension plg3', 'de-install');
select lives_ok('create extension plg3', 'reinstall');

--------------------------------------------------------------------------------
--
-- Function call protocol tests
--

create function f_void() returns void as ' ''() ' language guile3;
select lives_ok('select f_void()', 'protocol: void return does not raise');

create function f_const() returns int as '1' language guile3;
select is(f_const(), 1, 'protocol: no arguments scalar constant result');

create function f_inc(a int) returns int as '(+ a 1)' language guile3;
select is(f_inc(2), 3, 'protocol: increment int');

create function f_inc_out(a int, b out int) returns int as '(+ a 1)' language guile3;
select is(f_inc_out(4), 5, 'protocol: increment int out 1');

create function f_sum(a int, b int) returns int as '(+ a b)' language guile3;
select is(f_sum(6, 7), 13, 'protocol: sum of arguments');

create function f_sum_default(a int, b int = 8) returns int as '(+ a b)' language guile3;
select is(f_sum_default(9), 17, 'protocol: sum of arguments using default');

create function f_two_out(a int, b out int, c out int) returns record as
'(values (+ a 1) (* a 2))' language guile3;
select is(f_two_out(10), (11, 20), 'protocol: two outputs');

create function f_double_in_out(a inout int, b inout int) returns record as
'(values (- a b) (* a b))' language guile3;
select is(f_double_in_out(12, 13), (-1, 156), 'protocol: inout params');


--------------------------------------------------------------------------------
--
-- Type smallint/int2
--

create function f_small_sum(a smallint, b smallint) returns smallint as '(+ a b)'
language guile3;

create function f_x_int2_rational() returns smallint as '1/2'
language guile3;

create function f_x_int2_string() returns smallint as '"not an integer"'
language guile3;

select is(f_small_sum(1::smallint, 2::smallint), 3::smallint, 'int2: simple sum');
select throws_ok(
  'select f_small_sum(32767::smallint, 1::smallint)',
  'out of range for int2: 32768',
  'int2: overflow check');

select throws_ok(
  'select f_small_sum(-32767::smallint, -2::smallint)',
  'out of range for int2: -32769',
  'int2: underflow check');

select throws_ok(
  'select f_x_int2_rational()',
  'wrong type, integer expected: 1/2',
  'int2: exact check');

select throws_ok(
  'select f_x_int2_string()',
  'wrong type, integer expected: "not an integer"',
  'int2: wrong type check');

--------------------------------------------------------------------------------
--
-- Type int/int4
--

create function f_x_int4_rational() returns int4 as '1/2'
language guile3;

create function f_x_int4_string() returns int4 as '"not an integer"'
language guile3;

select throws_ok(
  'select f_sum(2147483647, 1)',
  'out of range for int4: 2147483648',
  'int4: overflow check');

select throws_ok(
  'select f_sum(-2147483648, -1)',
  'out of range for int4: -2147483649',
  'int4: underflow check');

select throws_ok(
  'select f_x_int4_rational()',
  'wrong type, integer expected: 1/2',
  'int4: exact check');

select throws_ok(
  'select f_x_int4_string()',
  'wrong type, integer expected: "not an integer"',
  'int4: wrong type check');

--------------------------------------------------------------------------------
--
-- Type bigint/int8
--

create function f_big_sum(a bigint, b bigint) returns bigint as '(+ a b)'
language guile3;

create function f_x_int8_string() returns int8 as '"not an integer"'
language guile3;

select is(f_big_sum(1::bigint, 2::bigint), 3::bigint, 'int8: simple sum');
select throws_ok(
  'select f_big_sum(9223372036854775807::bigint, 1::bigint)',
  'out of range for int8: 9223372036854775808',
  'int8: overflow check');

select throws_ok(
  'select f_big_sum(-9223372036854775807::bigint, -2::bigint)',
  'out of range for int8: -9223372036854775809',
  'int8: underflow check');

select throws_ok(
  'select f_x_int8_string()',
  'wrong type, integer expected: "not an integer"',
  'int8: wrong type check');

--------------------------------------------------------------------------------
--
-- Type real/float4
--

create function f_real(a real, b real) returns real as '(* a b)' language guile3;
create function f_int_as_real() returns real as '1' language guile3;
create function f_rational_as_real() returns real as '1/2' language guile3;
create function f_x_text_as_real() returns real as '"not a number"' language guile3;
create function f_decimal_as_real() returns real as '(make-decimal 31415 4)' language guile3;
create function f_decimal_inf_as_real() returns real as '(make-decimal "Infinity" 0)' language guile3;
create function f_decimal_neg_inf_as_real() returns real as '(make-decimal "-Infinity" 0)' language guile3;
create function f_decimal_nan_as_real() returns real as '(make-decimal "NaN" 0)' language guile3;

select is(f_real(2.0::real, 1.5::real), 3.0::real, 'float4: simple sum');
select is(f_real(3.402823e38::real, 2.0::real), 'inf'::real, 'float4: inf overflow');
select is(f_real(1.4e-45::real, 0.5::real), 0.0::real, 'float4: 0 underflow');
select is(f_real(-3.402823e38::real, 2.0::real), '-inf'::real, 'float4: negative inf overflow');
select is(f_real(-1.4e-45::real, 0.5::real), 0.0::real, 'float4: negative 0 underflow');
select is(f_real('nan'::real, 0.5::real), 'nan'::real, 'float4: nan propagation');

select is(f_int_as_real(), 1.0::real, 'float4: coerce int');
select is(f_rational_as_real(), 0.5::real, 'float4: auto convert from rational');

select throws_ok(
  'select f_x_text_as_real()',
  'wrong type, number expected: "not a number"',
  'float4: wrong type check');

select is(f_decimal_as_real(), 3.1415::real, 'float4: auto convert from decimal');
select is(f_decimal_inf_as_real(), 'inf'::real, 'float4: auto convert from decimal -- infinity');
select is(f_decimal_neg_inf_as_real(), '-inf'::real, 'float4: auto convert from decimal -- negative infinity');
select is(f_decimal_nan_as_real(), 'nan'::real, 'float4: auto convert from decimal -- nan');

--------------------------------------------------------------------------------
--
-- Type double precision/float8
--

create function f_dp(a double precision, b double precision) returns double precision as
'(* a b)' language guile3;
create function f_int_as_dp() returns double precision as '1' language guile3;
create function f_rational_as_dp() returns double precision as '1/2' language guile3;
create function f_x_text_as_dp() returns double precision as '"not a number"' language guile3;
create function f_decimal_as_dp() returns double precision as '(make-decimal 31415 4)' language guile3;
create function f_decimal_inf_as_dp() returns double precision as '(make-decimal "Infinity" 0)' language guile3;
create function f_decimal_neg_inf_as_dp() returns double precision as '(make-decimal "-Infinity" 0)' language guile3;
create function f_decimal_nan_as_dp() returns double precision as '(make-decimal "NaN" 0)' language guile3;

select is(f_dp(2.0, 1.5), 3.0::double precision, 'float8: simple sum');
select is(f_dp(1.7976931348623157e308, 2.0), 'inf', 'float8: inf overflow');
select is(f_dp(5e-324, 0.5), 0.0::double precision, 'float8: 0 underflow');
select is(f_dp(-1.7976931348623157e308, 2.0), '-inf', 'float8: negative inf overflow');
select is(f_dp(-5e-324, 0.5), 0.0::double precision, 'float8: negative 0 underflow');
select is(f_dp('nan', 0.5), 'nan'::double precision, 'float8: nan propagation');

select is(f_int_as_dp(), 1.0::double precision, 'float8: coerce int');
select is(f_rational_as_dp(), 0.5::double precision, 'float8: coerce rational');

select throws_ok(
  'select f_x_text_as_dp()',
  'wrong type, number expected: "not a number"',
  'float8: wrong type check');

select is(f_decimal_as_dp(), 3.1415::double precision, 'float8: auto convert from decimal');
select is(f_decimal_inf_as_dp(), 'inf'::double precision, 'float8: auto convert from decimal -- infinity');
select is(f_decimal_neg_inf_as_dp(), '-inf'::double precision, 'float8: auto convert from decimal -- negative infinity');
select is(f_decimal_nan_as_dp(), 'nan'::double precision, 'float8: auto convert from decimal -- nan');

--------------------------------------------------------------------------------
--
-- Type decimal/numeric
--

create function f_numeric_id(a numeric) returns numeric as 'a' language guile3;
create function f_int_as_numeric() returns numeric as '5' language guile3;
create function f_nan_as_numeric() returns numeric as '+nan.0' language guile3;
create function f_inf_as_numeric() returns numeric as '+inf.0' language guile3;
create function f_neg_inf_as_numeric() returns numeric as '-inf.0' language guile3;
create function f_rational_as_numeric() returns numeric as '1/2' language guile3;
create function f_large_real_as_numeric() returns numeric as '6.02e23' language guile3;
create function f_x_string_as_numeric() returns numeric as '"not a number"' language guile3;
create function f_x_bad_decimal_1() returns numeric as '(define (r) (make-decimal "a" 0)) (r)' language guile3;
create function f_x_bad_decimal_2() returns numeric as '(make-decimal 0.5 0)' language guile3;
create function f_x_bad_decimal_3() returns numeric as '(make-decimal 5 -1)' language guile3;
create function f_x_bad_decimal_4() returns numeric as '(make-decimal 5 0.2)' language guile3;
create function f_x_bad_decimal_5() returns numeric as '(make-decimal 5 "2")' language guile3;

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test -- positive finite')
from (select '3.1415'::numeric(5,3)) t(v);

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test -- negative finite')
from (select '-2'::numeric(5, 0)) t(v);

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test -- infinity')
from (select 'infinity'::numeric) t(v);

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test -- negative infinity')
from (select '-Infinity'::numeric) t(v);

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test -- not a number')
from (select 'Nan'::numeric) t(v);

select is(f_int_as_numeric(), 5::numeric, 'decimal: integer auto conv');
select is(f_nan_as_numeric(), 'NaN'::numeric, 'decimal: real auto conv -- nan');
select is(f_inf_as_numeric(), 'Inf'::numeric, 'decimal: real auto conv -- inf');
select is(f_neg_inf_as_numeric(), '-Inf'::numeric, 'decimal: real auto conv -- negative infinity');
select is(f_rational_as_numeric(), 0.5::numeric, 'decimal: real auto conv -- rational');
select is(f_large_real_as_numeric(), 6.02e23::numeric, 'decimal: large real auto conv');

select throws_ok(
  'select f_x_string_as_numeric()',
  'decimal result expected, not: "not a number"',
  'decimal: wrong type check, string');

select throws_ok(
  'select f_x_bad_decimal_1()',
  '%exception: ((invalid-decimal #:digits "a" #:scale 0))',
  'decimal: bad decimal 1');

select throws_ok(
  'select f_x_bad_decimal_2()',
  '%exception: ((invalid-decimal #:digits 0.5 #:scale 0))',
  'decimal: bad decimal 2');

select throws_ok(
  'select f_x_bad_decimal_3()',
  '%exception: ((invalid-decimal #:digits 5 #:scale -1))',
  'decimal: bad decimal 3');

select throws_ok(
  'select f_x_bad_decimal_4()',
  '%exception: ((invalid-decimal #:digits 5 #:scale 0.2))',
  'decimal: bad decimal 4');

select throws_ok(
  'select f_x_bad_decimal_5()',
  '%exception: ((invalid-decimal #:digits 5 #:scale "2"))',
  'decimal: bad decimal 5');

--------------------------------------------------------------------------------
--
-- Type money
--

create function f_money_id(a money) returns money as 'a' language guile3;
create function f_x_money_inexact() returns money as '1.5' language guile3;
create function f_x_money_rational() returns money as '13/5' language guile3;
create function f_x_money_string() returns money as '"$1.25"' language guile3;

select is(f_money_id(t.v), t.v, 'money: identity mapping test')
from (select '2.28'::money) t(v);

select throws_ok(
  'select f_x_money_inexact()',
  'wrong type, integer expected: 1.5',
  'money: wrong type check, inexact');

select throws_ok(
  'select f_x_money_rational()',
  'wrong type, integer expected: 13/5',
  'money: wrong type check, rational');

select throws_ok(
  'select f_x_money_string()',
  'wrong type, integer expected: "$1.25"',
  'money: wrong type check, string');

--------------------------------------------------------------------------------
--
-- Type text
--

create function f_text_in(a text) returns int as '(string-length a)' language guile3;
create function f_text_out(a int) returns text as '(number->string a)' language guile3;
create function f_text_error(a int) returns text as 'a' language guile3;

select is(f_text_in('Hello, World!'), 13, 'text: text input');
select is(f_text_out(13), '13', 'text: text output');
select throws_ok(
       'select f_text_error(13)',
       'wrong type, string expected: 13',
       'text: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type char
--

create function f_char_in(a char) returns int as '(string-length a)' language guile3;
create function f_char_out(a int) returns char as '(number->string a)' language guile3;
create function f_char_error(a int) returns char as 'a' language guile3;

select is(f_char_in('Hello, World!'), 13, 'char: char input');
select is(f_char_out(13), '13', 'char: char output');
select throws_ok(
       'select f_char_error(13)',
       'wrong type, string expected: 13',
       'char: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type varchar
--

create function f_varchar_in(a varchar) returns int as '(string-length a)' language guile3;
create function f_varchar_out(a int) returns varchar as '(number->string a)' language guile3;
create function f_varchar_error(a int) returns varchar as 'a' language guile3;

select is(f_varchar_in('Hello, World!'), 13, 'varchar: varchar input');
select is(f_varchar_out(13), '13', 'varchar: varchar output');
select throws_ok(
       'select f_varchar_error(13)',
       'wrong type, string expected: 13',
       'varchar: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type bytea
--

create function f_bytea_in(a bytea) returns int as '(bytevector-length a)' language guile3;
create function f_bytea_out(a int) returns bytea as '(make-bytevector a 42)' language guile3;
create function f_bytea_error(a int) returns bytea as 'a' language guile3;

select is(f_bytea_in('\x7730307421'::bytea), 5, 'bytea: bytea input');
select is(f_bytea_out(2), '\x2a2a'::bytea, 'bytea: bytea output');
select throws_ok(
       'select f_bytea_error(13)',
       'bytea result expected, not: 13',
       'bytea: wrong return type check');

--------------------------------------------------------------------------------
--
-- Type timestamptz
--

create function f_tz_id(a timestamptz) returns timestamptz as 'a' language guile3;
create function f_tz_to_text(a timestamptz) returns text as '(date->string a "~Y-~m-~d ~H:~M:~f ~z")' language guile3;

select is(f_tz_id(t), t, 'timestamptz: identity mapping test')
from current_timestamp t;

select is(f_tz_to_text(t)::timestamptz, t, 'timestamptz: to scheme value check')
from current_timestamp t;

set time zone 'America/Los_Angeles';

select is(f_tz_id(t), t, 'timestamptz: identity mapping test non UTC')
from current_timestamp t;

set time zone 'UTC';

--------------------------------------------------------------------------------
--
-- Type timestamp
--

create function f_ts_id(a timestamp) returns timestamp as 'a' language guile3;
create function f_ts_to_text(a timestamp) returns text as '(date->string a "~Y-~m-~d ~H:~M:~f ~z")' language guile3;

select is(f_ts_id(t::timestamp), t::timestamp, 'timestamp: identity mapping test')
from current_timestamp t;

select is(f_ts_to_text(t::timestamp)::timestamp, t::timestamp, 'timestamp: to scheme value check')
from current_timestamp t;

--------------------------------------------------------------------------------
--
-- Type date
--

create function f_dt_id(a date) returns date as 'a' language guile3;
create function f_dt_to_text(a date) returns text as '(date->string a "~Y-~m-~d")' language guile3;

select is(f_dt_id(t::date), t::date, 'date: identity mapping test')
from current_date t;

select is(f_dt_to_text(t::date)::date, t::date, 'date: to scheme value check')
from current_date t;

--------------------------------------------------------------------------------
--
-- Type time
--

create function f_tm_id(a time) returns time as 'a' language guile3;
create function f_tm_to_text(a time) returns text as '(date->string (time-monotonic->date a) "~H:~M:~f")' language guile3;

select is(f_tm_id(t::time), t::time, 'time: identity mapping test')
from current_time t;

select is(f_tm_to_text(t::time)::time, t::time, 'time: to scheme value check')
from current_time t;

--------------------------------------------------------------------------------
--
-- Type timetz
--

create function f_tmtz_id(a timetz) returns timetz as 'a' language guile3;
create function f_tmtz_to_text(a timetz) returns text as '(date->string (time-monotonic->date a) "~H:~M:~f")' language guile3;

select is(f_tmtz_id(t), t, 'timetz: identity mapping test')
from current_time t;

select is(f_tmtz_to_text(t)::timetz, t, 'timetz: to scheme value check')
from current_time t;

set time zone 'America/Los_Angeles';

select is(f_tmtz_id(t), t, 'timetz: identity mapping test non UTC')
from current_time t;

set time zone 'UTC';

--------------------------------------------------------------------------------
--
-- Type interval
--

create function f_itv_id(a interval) returns interval as 'a' language guile3;

select is(f_itv_id(t.itv), t.itv, 'interval: identity mapping test')
from (select current_timestamp - current_date) t(itv);

--------------------------------------------------------------------------------
--
-- Type boolean
--

create function f_bool(a boolean) returns int as '(if a 1 0)' language guile3;
create function f_to_bool(a int) returns boolean as '(zero? a)' language guile3;

select is(f_bool(true), 1, 'boolean: true argument');
select is(f_bool(false), 0, 'boolean: false argument');
select is(f_to_bool(0), true, 'boolean: true return');
select is(f_to_bool(1), false, 'boolean: false return');

--------------------------------------------------------------------------------
--
-- Enum Types
--

do $$ begin
if exists (select 1 from pg_type where typname = 'fruit') then
  drop type fruit;
end if;
create type fruit as enum ('apple', 'acorn', 'olive');
end $$ language plpgsql;

create function f_fruit_id(a fruit) returns fruit as 'a' language guile3;

select is(f_fruit_id('apple'::fruit), 'apple'::fruit, 'enum: identity mapping test 1');
select is(f_fruit_id('acorn'::fruit), 'acorn'::fruit, 'enum: identity mapping test 2');
select is(f_fruit_id('olive'::fruit), 'olive'::fruit, 'enum: identity mapping test 3');

--------------------------------------------------------------------------------
--
-- Type point
--

create function f_point_id(a point) returns point as 'a' language guile3;
create function f_point_vec() returns point as '#(1 2)' language guile3;

select ok(f_point_id(t.point) ~= t.point, 'point: identity mapping test')
from (select point '(3.14, 2.72)') t(point);

select ok(f_point_vec() ~= point '(1, 2)', 'point: vector point');

--------------------------------------------------------------------------------
--
-- Type line
--

create function f_line_id(a line) returns line as 'a' language guile3;

select ok(f_line_id(t.line) ?|| t.line and f_line_id(t.line) <-> t.line = 0.0,
          'line: identity mapping test')
from (select line '((3.14, 2.72), (14, 42))') t(line);

--------------------------------------------------------------------------------
--
-- Type lseg
--

create function f_lseg_id(a lseg) returns lseg as 'a' language guile3;

select ok(f_lseg_id(t.lseg) ?|| t.lseg and f_lseg_id(t.lseg) <-> t.lseg = 0.0,
          'lseg: identity mapping test')
from (select lseg '((3.14, 2.72), (14, 42))') t(lseg);

--------------------------------------------------------------------------------
--
-- Type box
--

create function f_box_id(a box) returns box as 'a' language guile3;

select ok(f_box_id(t.box) ~= t.box, 'box: identity mapping test')
from (select box '((3.14, 2.72), (14, 42))') t(box);

--------------------------------------------------------------------------------
--
-- Type path
--

create function f_path_id(a path) returns path as 'a' language guile3;

select ok(f_path_id(t.path) = t.path, 'path: identity mapping test')
from (select path '((3.14, 2.72), (14, 42), (0, 0))') t(path);

--------------------------------------------------------------------------------
--
-- Type polygon
--

create function f_polygon_id(a polygon) returns polygon as 'a' language guile3;

select ok(f_polygon_id(t.polygon) ~= t.polygon, 'polygon: identity mapping test')
from (select polygon '((3.14, 2.72), (14, 42), (1, 1), (3.14, 2.72))') t(polygon);

--------------------------------------------------------------------------------
--
-- Type circle
--

create function f_circle_id(a circle) returns circle as 'a' language guile3;

select ok(f_circle_id(t.circle) ~= t.circle, 'circle: identity mapping test')
from (select circle '((3.14, 2.72), 42)') t(circle);

--------------------------------------------------------------------------------
--
-- Type inet
--

create function f_inet_id(a inet) returns inet as 'a' language guile3;

select ok(f_inet_id(t.inet) = t.inet, 'inet: identity mapping test ipv4')
from (select inet '192.168.100.128/25') t(inet);

select ok(f_inet_id(t.inet) = t.inet, 'inet: identity mapping test ipv6')
from (select inet '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128') t(inet);

--------------------------------------------------------------------------------
--
-- Type cidr
--

create function f_cidr_id(a cidr) returns cidr as 'a' language guile3;

select ok(f_cidr_id(t.cidr) = t.cidr, 'cidr: identity mapping test ipv4')
from (select cidr '192.168.100.128/25') t(cidr);

select ok(f_cidr_id(t.cidr) = t.cidr, 'cidr: identity mapping test ipv6')
from (select cidr '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128') t(cidr);

--------------------------------------------------------------------------------
--
-- Type macaddr
--

create function f_macaddr_id(a macaddr) returns macaddr as 'a' language guile3;

select ok(f_macaddr_id(t.macaddr) = t.macaddr, 'macaddr: identity mapping test')
from (select macaddr '08:00:2b:01:02:03') t(macaddr);

--------------------------------------------------------------------------------
--
-- Type macaddr8
--

create function f_macaddr8_id(a macaddr8) returns macaddr8 as 'a' language guile3;

select ok(f_macaddr8_id(t.macaddr8) = t.macaddr8, 'macaddr8: identity mapping test')
from (select macaddr8 '08:00:2b:01:02:03:04:05') t(macaddr8);

--------------------------------------------------------------------------------
--
-- Type bit
--

create function f_bit_id(a bit(6)) returns bit(6) as 'a' language guile3;

select ok(f_bit_id(t.bit) = t.bit, 'bit: identity mapping test')
from (select B'101010') t(bit);

--------------------------------------------------------------------------------
--
-- Type bit varying
--

create function f_varbit_id(a bit varying) returns bit varying as 'a' language guile3;

select ok(f_varbit_id(t.varbit) = t.varbit, 'bit varying: identity mapping test')
from (select B'101010111010011101011011010110000101') t(varbit);

--------------------------------------------------------------------------------
--
-- Type tsvector
--

create function f_tsvector_id(a tsvector) returns tsvector as 'a' language guile3;

select ok(f_tsvector_id(t.tsvector) = t.tsvector, 'tsvector: identity mapping test')
from (select 'a:1A fat:2B,4C cat:5D'::tsvector) t(tsvector);

--------------------------------------------------------------------------------
--
-- Type tsquery
--

create function f_tsquery_id(a tsquery) returns tsquery as 'a' language guile3;

select ok(f_tsquery_id(t.tsquery) = t.tsquery, 'tsquery: identity mapping test')
from (select 'fat:a & (rat:bc | cat:*d) <6> mat'::tsquery) t(tsquery);

--------------------------------------------------------------------------------
--
-- Type uuid
--

create function f_uuid_id(a uuid) returns uuid as 'a' language guile3;

select ok(f_uuid_id(t.uuid) = t.uuid, 'uuid: identity mapping test')
from (select gen_random_uuid()) t(uuid);

--------------------------------------------------------------------------------
--
-- Type xml
--

create function f_xml_id(a xml) returns xml as 'a' language guile3;

select ok(xpath('/foo/text()', f_xml_id(t.xml))::text = '{bar}', 'xml: identity mapping test')
from (select xml '<foo>bar</foo>') t(xml);

--------------------------------------------------------------------------------
--
-- Type json
--

create function f_json_id(a json) returns json as 'a' language guile3;

select ok(f_json_id(t.json)->'foo'->>0 = 'bar', 'json: identity mapping test')
from (select '{"foo": ["bar", 42]}'::json) t(json);

--------------------------------------------------------------------------------
--
-- Type jsonb
--

create function f_jsonb_id(a jsonb) returns jsonb as 'a' language guile3;

select ok(f_jsonb_id(t.jsonb)->'foo'->>0 = 'bar', 'jsonb: identity mapping test')
from (select '{"foo": ["bar", 42]}'::jsonb) t(jsonb);

create function f_x_jsonb_bad_object_1() returns jsonb as $$
'("a") ;; '
$$ language guile3;

select throws_ok(
  'select f_x_jsonb_bad_object_1()',
  'jsonb object expression must be an alist with string keys: ("a")',
  'jsonb: bad object, wrong length');

create function f_x_jsonb_bad_object_2() returns jsonb as $$
'(2 "a") ;; '
$$ language guile3;

select throws_ok(
  'select f_x_jsonb_bad_object_2()',
  'jsonb object expression must be an alist with string keys: (2 "a")',
  'jsonb: bad object, bad key type');

create function f_x_jsonb_bad_object_3() returns jsonb as $$
(cons "a" 2)
$$ language guile3;

select throws_ok(
  'select f_x_jsonb_bad_object_3()',
  'jsonb object expression must be an alist with string keys: ("a" . 2)',
  'jsonb: bad object, improper list');

--------------------------------------------------------------------------------
--
-- Type jsonpath
--

create function f_jsonpath_id(a jsonpath) returns jsonpath as 'a' language guile3;

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*].foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.*.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.**.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.**{4}.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.**{2 to 6}.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.**{1 to last}.foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '1'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[1,2,3].foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[1 to 5,2,3].foo.bar'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$."$gizmo"'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$gizmo'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '+ $gizmo'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '- $[*]'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '6 + $gizmo'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select 'true'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.dt.datetime()'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.dt.datetime("fmt")'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$.dt.datetime("fmt").type()'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@ == "a")'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@ >= $.x + 2)'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@ >= $.x - 2)'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@.job == null).name'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? ((@ > 0) is unknown)'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@ like_regex "^ab.*c" flag "i")'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select '$[*] ? (@ starts with "John")'::jsonpath) t(v);

select ok(f_jsonpath_id(t.v)::text = t.v::text, 'jsonpath: identity mapping test')
from (select 'strict $ ? (exists (@.name)) .name'::jsonpath) t(v);

--------------------------------------------------------------------------------
--
-- Arrays
--

create function f_array_arg(a text[]) returns text as '(vector-ref a 0)' language guile3;
create function f_ret_array() returns text[] as '#("a" "b")' language guile3;

select is(f_array_arg('{foo,bar}'::text[]), 'foo', 'array: 2 item parameter');
select is(f_ret_array(), '{"a", "b"}'::text[], 'array: return 2 item text');

--------------------------------------------------------------------------------
--
-- Records
--

create type simple_record as (name text, count int, weight float8);

create function f_record_arg_by_name(r simple_record) returns text as '(record-ref r ''name)' language guile3;
create function f_record_arg_by_number(r simple_record) returns numeric as '(record-ref r 2)' language guile3;
create function f_record_arg_type_name(r simple_record) returns text as '(symbol->string (car (record-types r)))' language guile3;
create function f_ret_record() returns record as '(make-record ''(text int4 float8) #("a" 98 2.99792458e8) ''(s c v) #f)' language guile3;

select is(f_record_arg_by_name(row('foo', 5, 1.41)::simple_record), 'foo', 'record: argument by name');
select is(f_record_arg_by_number(row('foo', 5, 1.41)::simple_record), 1.41, 'record: argument by number');
select is(f_record_arg_type_name(row('foo', 5, 1.41)::simple_record), 'text', 'record: argument type name');

select ok(f_ret_record() = t, 'record: simple return test')
from (select 'a', 98, 2.99792458e8::float8) t(text, int, float8);

--------------------------------------------------------------------------------
--
-- Ranges
--

create function f_int4range_id(a int4range) returns int4range as 'a' language guile3;

select ok(f_int4range_id(t.v) = t.v, 'int4range: int4 identity mapping test')
from (select '[0, 60]'::int4range) t(v);

--------------------------------------------------------------------------------
--
-- Multiranges
--

create function f_int4multirange_id(a int4multirange) returns int4multirange as 'a' language guile3;

select ok(f_int4multirange_id(t.v) = t.v, 'int4multirange: identity mapping test')
from (select '{[3,7), [8,9)}'::int4multirange) t(v);

create function f_x_bad_multirange_1() returns int4multirange as $$
(make-multirange (cons 1 2))
$$ language guile3;

create function f_x_bad_multirange_2() returns int4multirange as $$
(make-multirange #f)
$$ language guile3;

select throws_ok(
  'select f_x_bad_multirange_1()',
  'multirange must be a list of ranges: (1 . 2)',
  'int4multirange: bad list 1');

select throws_ok(
  'select f_x_bad_multirange_2()',
  'multirange must be a list of ranges: #f',
  'int4multirange: bad list 2');

--------------------------------------------------------------------------------
--
-- Domains
--

create domain posint as integer check (value > 0);

create function f_posint_id(a posint) returns posint as 'a' language guile3;

select ok(f_posint_id(t.v) = t.v, 'domain type: identity mapping test')
from (select 42::posint) t(v);

--------------------------------------------------------------------------------
--
-- Set Returning Functions
--

create function f_setof_text() returns setof text as $$(list "one" "two")$$ language guile3;

create function f_setof_int_array() returns setof int4[] as $$(list #(0 -1) #(1 0))$$ language guile3;

create function f_setof_record() returns setof record as $$
(let ((types '(int4 text float8))) ; '
  (make-table types
              (list (make-record types #(1 "one" 1.0) '() #f) ; '
                    (make-record types #(2 "two" 2.0) '() #f)); '
              '(a b c) ;'
              #f))
$$
language guile3;

select ok(array_agg(t) = '{one,two}'::text[], 'setof: text')
from f_setof_text() t(text);

select ok(array_agg(t) = '{{0,-1},{1,0}}'::int[], 'setof: int4[]')
from f_setof_int_array() t;

select ok(t.name = 'two', 'setof: record')
from f_setof_record() t(id int, name text, weight float8)
where t.id = 2;

-- force call in context where columns are not defined to exercise
-- convert_result_to_setof_record_datum; otherwise takes code path through
-- convert_result_to_setof_composite_datum.

select throws_ok(
  'select f_setof_record()',
  'function returning setof record called in context that cannot accept type record',
  'setof: record');

create function f_x_bad_record_types_1() returns setof record as $$
(make-table (cons 'int4 0) ;; '
            '() ;; '
            '(a b) ;; '
            #f)
$$
language guile3;

select throws_ok(
  'select f_x_bad_record_types_1()',
  'Type description must be a list of symbols: (int4 . 0)',
  'setof: x record bad types');

create function f_x_bad_record_types_2() returns setof record as $$
(make-table '(int4 0) ;; '
            '() ;; '
            '(a b) ;; '
            #f)
$$
language guile3;

select throws_ok(
  'select f_x_bad_record_types_2()',
  'Type description must be a list of symbols: (int4 0)',
  'setof: x record bad types');

--------------------------------------------------------------------------------
--
-- SPI Integration
--

create function f_execute_simple() returns int4 as '(scalar (execute "select 1"))'
language guile3;

create function f_execute_with_args_int2() returns int2 as '(scalar (execute "select $1" ''(3)))'
language guile3;

create function f_execute_with_args_int4() returns int4 as '(scalar (execute "select $1" ''(3)))'
language guile3;

create function f_execute_with_args_int8() returns int8 as '(scalar (execute "select $1" ''(3)))'
language guile3;

create function f_execute_with_args_float4() returns float4 as '(scalar (execute "select $1" ''(3.14)))'
language guile3;

create function f_execute_with_args_float8() returns float8 as '(scalar (execute "select $1" ''(3.14)))'
language guile3;

create function f_execute_with_args_rat_float4() returns float4 as '(scalar (execute "select $1" ''(314/100)))'
language guile3;

create function f_execute_with_args_rat_float8() returns float8 as '(scalar (execute "select $1" ''(314/100)))'
language guile3;

create function f_execute_with_args_large_numeric() returns numeric as '(scalar (execute "select $1" ''(123456789012345678901234567890)))'
language guile3;

create function f_execute_with_args_rat_numeric() returns numeric as '(scalar (execute "select $1" ''(314/100)))'
language guile3;

create function f_execute_with_args_decimal() returns numeric as '(scalar (execute "select $1" `(,(make-decimal 314159 5))))'
language guile3;

create function f_execute_with_args_money() returns money as '(scalar (execute "select $1" ''(1234)))'
language guile3;

create function f_execute_with_args_text() returns text as '(scalar (execute "select $1" ''("hello")))'
language guile3;

create function f_execute_with_args_bytea() returns bytea as '(scalar (execute "select $1" ''(#u8(#x80 #x40 #x20))))'
language guile3;

create function f_execute_with_args_timestamp() returns timestamp as '(scalar (execute "select $1" `(,(make-date 123456789 56 34 12 1 1 1970 0))))'
language guile3;

create function f_execute_with_args_timestamptz() returns timestamptz as '(scalar (execute "select $1" `(,(make-date 123456789 56 34 12 1 1 1970 0))))'
language guile3;

create function f_execute_with_args_date() returns date as '(scalar (execute "select $1" `(,(make-date 123456789 56 34 12 1 1 1970 0))))'
language guile3;

create function f_execute_with_args_time() returns time as '(scalar (execute "select $1" `(,(make-time time-monotonic 123456789 (+ 56 (* 60 (+ 34 (* 60 12))))))))'
language guile3;

create function f_execute_with_args_timetz() returns timetz as '(scalar (execute "select $1" `(,(make-time time-monotonic 123456789 (+ 56 (* 60 (+ 34 (* 60 12))))))))'
language guile3;

create function f_execute_with_args_boolean(x int) returns boolean as '(scalar (execute "select $1" `(,(> x 0))))'
language guile3;

create function f_execute_with_args_point() returns point as '(scalar (execute "select $1" `(,(make-point 1 2))))'
language guile3;

create function f_execute_with_args_line() returns line as '(scalar (execute "select $1" `(,(make-line 3 2 1))))'
language guile3;

create function f_execute_with_args_lseg() returns lseg as '(scalar (execute "select $1" `(,(make-lseg (make-point 0 1) (make-point 2 3)))))'
language guile3;

create function f_execute_with_args_box() returns box as '(scalar (execute "select $1" `(,(make-box (make-point 0 1) (make-point 2 3)))))'
language guile3;

create function f_execute_with_args_path() returns path as '(scalar (execute "select $1" `(,(make-path #f (list->vector (list (make-point 0 1) (make-point 2 3) (make-point 4 1)))))))'
language guile3;

create function f_execute_with_args_fruit() returns fruit as $$
(scalar (execute "select $1" '((fruit . apple)))) ; '
$$ language guile3;

create function f_execute_with_args_polygon() returns polygon as $$
(scalar (execute "select $1"
                 `(,(make-polygon
                     (make-box (make-point 0 0) (make-point 2 2))
                     (list->vector (list (make-point 0 1)
                                         (make-point 1 2)
                                         (make-point 2 1)
                                         (make-point 1 0)
                                         (make-point 0 1)))))))
$$
language guile3;

create function f_execute_with_args_inet() returns inet as $$
(scalar (execute "select $1"
                 `(,(make-inet 'inet 32 #u8(192 168 1 42))))) ;; '
$$
language guile3;

create function f_execute_with_args_inet6() returns inet as $$
(scalar (execute "select $1"
                 `(,(make-inet 'inet6 128 #u8(#x20 #x01 #x04 #xf8  ;; '
                                              #x00 #x03 #x00 #xba
                                              #x02 #xe0 #x81 #xff
                                              #xfe #x22 #xd1 #xf1)))))
$$
language guile3;

create function f_execute_with_args_cidr() returns cidr as $$
(scalar (execute "select $1"
                 `(,(make-inet 'inet 8 #u8(10 0 0 0))))) ;; '
$$
language guile3;

create function f_execute_with_args_macaddr() returns macaddr as $$
(scalar (execute "select $1"
                 `(,(make-macaddr #u8(#x08 #x00 #x2b #x01 #x02 #x03)))))
$$
language guile3;

create function f_execute_with_args_macaddr8() returns macaddr8 as $$
(scalar (execute "select $1"
                 `(,(make-macaddr8 #u8(#x08 #x00 #x2b #x01 #x02 #x03 #x04 #x05)))))
$$
language guile3;

create function f_execute_with_args_bits() returns bit(6) as $$
(scalar (execute "select $1"
                 `(,(make-bit-string #u8(#xa9) 8))))
$$
language guile3;

create function f_execute_with_args_tsvector() returns tsvector as $$
(scalar (execute "select $1"
                 `(,(make-tsvector (list (make-tslexeme "foo"
                                                        (list (make-tsposition 1 3))))))))
$$
language guile3;

create function f_execute_with_args_tsquery() returns tsquery as $$
(scalar (execute "select $1"
                 `(,(make-tsquery '(and (value "fat" 8 #f)  ;; '
                                        (phrase (or (value "rat" 6 #f) (value "cat" 1 #t))
                                                (value "mat" 0 #f)
                                                6))))))
$$
language guile3;

create function f_execute_with_args_uuid() returns uuid as $$
(scalar (execute "select $1::uuid" '("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))) ;; '
$$
language guile3;

create function f_execute_with_args_xml() returns xml as $$
(scalar (execute "select $1::xml" '("<foo>bar</foo>"))) ;; '
$$
language guile3;

create function f_execute_with_args_json() returns json as $$
(scalar (execute "select $1::json" '("{\"foo\": [\"bar\", 42]}"))) ;; '
$$
language guile3;

create function f_execute_with_args_jsonb() returns jsonb as $$
(scalar (execute "select $1::jsonb" '("{\"foo\": [\"bar\", 42]}"))) ;; '
$$
language guile3;

create function f_execute_with_args_jsonpath() returns jsonpath as $$
(scalar (execute "select $1::jsonpath" '("strict $ ? (exists (@.name)) .name"))) ;; '
$$
language guile3;

create function f_execute_with_args_text_array() returns text[] as $$
(scalar (execute "select $1" '(#("one" "two" "three")))) ;; '
$$
language guile3;

create function f_execute_with_args_int_array() returns int[] as $$
(scalar (execute "select $1" '(#(1 65537)))) ;; '
$$
language guile3;

create function f_execute_with_args_simple_record() returns record as $$
(scalar (execute "select row('foo', 5, 1.41)::simple_record"))
$$
language guile3;

create function f_execute_with_args_int4range() returns int4range as $$
(scalar (execute "select '[0, 60)'::int4range"))
$$
language guile3;

create function bool_diff(a bool, b bool) returns float8 as $$
  select case when a = b then 0.0 when a and not b then 1.0 else -1.0 end
$$ language sql immutable;

create type boolrange as range (subtype = bool, subtype_diff = bool_diff);

create function f_execute_with_args_boolrange() returns boolrange as $$
(scalar (execute "select '[false, true]'::boolrange"))
$$
language guile3;

create function f_execute_with_args_int4multirange() returns int4multirange as $$
(scalar (execute "select '{[3,7), [8,9)}'::int4multirange"))
$$
language guile3;

create function f_execute_with_args_posint() returns posint as $$
(scalar (execute "select 14::posint"))
$$
language guile3;

create function f_execute_wrong_type_command() returns text as $$
(with-exception-handler
  (lambda (exc) "bad command")
  (lambda () (execute 5))
  #:unwind? #t)
$$
language guile3;

create function f_execute_wrong_type_args_1() returns text as $$
(with-exception-handler
  (lambda (exc) "bad args")
  (lambda () (execute "select 1" 5))
  #:unwind? #t)
$$
language guile3;

create function f_execute_wrong_type_args_2() returns text as $$
(with-exception-handler
  (lambda (exc) "bad args")
  (lambda () (execute "select 1" (cons 1 2)))
  #:unwind? #t)
$$
language guile3;

select is(f_execute_with_args_int2(), 3::int2, 'execute: with args int2');
select is(f_execute_with_args_int4(), 3::int4, 'execute: with args int4');
select is(f_execute_with_args_int8(), 3::int8, 'execute: with args int8');
select is(f_execute_with_args_float4(), 3.14::float4, 'execute: with args float4');
select is(f_execute_with_args_float8(), 3.14::float8, 'execute: with args float8');
select is(f_execute_with_args_rat_float4(), 3.14::float4, 'execute: with args rational to float4');
select is(f_execute_with_args_rat_float8(), 3.14::float8, 'execute: with args rational to float8');
select is(f_execute_with_args_large_numeric(), 123456789012345678901234567890::numeric, 'execute: with args large numeric');
select is(f_execute_with_args_rat_numeric(), 3.14::numeric, 'execute: with args rational to numeric');
select is(f_execute_with_args_decimal(), 3.14159::numeric, 'execute: with args decimal');
select is(f_execute_with_args_money(), 12.34::money, 'execute: with args money');
select is(f_execute_with_args_text(), 'hello'::text, 'execute: with args text');
select is(f_execute_with_args_bytea(), '\x804020'::bytea, 'execute: with args bytea');
select is(f_execute_with_args_timestamp(), '1970-01-01 12:34:56.123456'::timestamp, 'execute: with args timestamp');
select is(f_execute_with_args_timestamptz(), '1970-01-01 12:34:56.123456'::timestamptz, 'execute: with args timestamptz');
select is(f_execute_with_args_date(), '1970-01-01'::date, 'execute: with args date');
select is(f_execute_with_args_time(), '12:34:56.123456'::time, 'execute: with args time');
select is(f_execute_with_args_timetz(), '12:34:56.123456'::timetz, 'execute: with args timetz');
select is(f_execute_with_args_boolean(1), true, 'execute: with args boolean true');
select is(f_execute_with_args_boolean(0), false, 'execute: with args boolean false');
select is(f_execute_with_args_fruit(), 'apple'::fruit, 'execute: with args fruit');
select is(f_execute_with_args_point()::text, '(1,2)', 'execute: with args point');
select is(f_execute_with_args_line()::text, '{3,2,1}', 'execute: with args line');
select is(f_execute_with_args_lseg()::text, '[(0,1),(2,3)]', 'execute: with args lseg');
select is(f_execute_with_args_box()::text, '(0,1),(2,3)', 'execute: with args box');
select is(f_execute_with_args_path()::text, '[(0,1),(2,3),(4,1)]', 'execute: with args path');
select is(f_execute_with_args_polygon()::text, '((0,1),(1,2),(2,1),(1,0),(0,1))', 'execute: with args polygon');
select is(f_execute_with_args_inet(), inet '192.168.1.42', 'execute: with args inet');
select is(f_execute_with_args_inet6(), inet '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128', 'execute: with args inet6');
select is(f_execute_with_args_cidr(), cidr '10.0.0.0/8', 'execute: with args cidr');
select is(f_execute_with_args_macaddr(), macaddr '08:00:2b:01:02:03', 'execute: with args macaddr');
select is(f_execute_with_args_macaddr8(), macaddr8 '08:00:2b:01:02:03:04:05', 'execute: with args macaddr8');
select is(f_execute_with_args_tsvector(), 'foo:1A'::tsvector, 'execute: with args tsvector');
select is(f_execute_with_args_tsquery(), 'fat:a & (rat:bc | cat:*d) <6> mat'::tsquery, 'execute: with args tsquery');
select is(f_execute_with_args_uuid(), uuid 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'execute: with args uuid');
select is(f_execute_with_args_xml()::text, '<foo>bar</foo>', 'execute: with args xml');
select is(f_execute_with_args_json()::text, '{"foo": ["bar", 42]}', 'execute: with args json');
select is(f_execute_with_args_json()->'foo'->>0, 'bar', 'execute: with args json unpacked');
select is(f_execute_with_args_jsonb()::text, '{"foo": ["bar", 42]}', 'execute: with args jsonb');
select is(f_execute_with_args_jsonb()->'foo'->>0, 'bar', 'execute: with args jsonb unpacked');
select is(f_execute_with_args_jsonpath()::text, 'strict $ ? (exists (@.name)) .name'::jsonpath::text, 'execute: with args jsonpath');
select is(f_execute_with_args_text_array(), '{one,two,three}', 'execute: with args text array');
select is(f_execute_with_args_int_array(), '{1,65537}', 'execute: with args int array');

select ok(f_execute_with_args_simple_record() = t, 'execute: with args simple return')
from (select 'foo', 5, 1.41::float8) t(text, int, float8);

select is(f_execute_with_args_int4range(), '[0, 60)'::int4range, 'execute: with args int4range');
select is(f_execute_with_args_posint(), 14::posint, 'execute: with args posint');

select is(f_execute_with_args_boolrange(), '[false, true]'::boolrange, 'execute: with args boolrange');
select is(f_execute_with_args_int4multirange(), '{[3,7), [8,9)}'::int4multirange, 'execute: with args int4multirange');

select is(f_execute_wrong_type_command(), 'bad command', 'execute: wrong_type_command');
select is(f_execute_wrong_type_args_1(), 'bad args', 'execute: wrong_type_args_1');
select is(f_execute_wrong_type_args_2(), 'bad args', 'execute: wrong_type_args_2');

create table things (id int, name text);
insert into things values (1, 'foo'), (2, 'bar');

create function f_x_execute_insert_immutable() returns int as $$
(scalar (execute "insert into things values (3, 'fish') returning id"))
$$ language guile3 immutable;

create function f_x_execute_insert_stable() returns int as $$
(scalar (execute "insert into things values (3, 'fish') returning id"))
$$ language guile3 stable;

select throws_ok(
  'select f_x_execute_insert_immutable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute: immutable cannot change the db');

select throws_ok(
  'select f_x_execute_insert_stable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute: stable cannot change the db');

create function f_x_execute_with_args_insert_immutable() returns int as $$
(scalar (execute "insert into things values (3, $1) returning id" '("fish"))) ;; '
$$ language guile3 immutable;

create function f_x_execute_with_args_insert_stable() returns int as $$
(scalar (execute "insert into things values (3, $1) returning id" '("fish"))) ;; '
$$ language guile3 stable;

select throws_ok(
  'select f_x_execute_with_args_insert_immutable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with args: immutable cannot change the db');

select throws_ok(
  'select f_x_execute_with_args_insert_stable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with args: stable cannot change the db');

create function f_x_execute_with_receiver_insert_immutable() returns int as $$
(scalar (execute "insert into things values (3, 'fish') returning id"
                 #:receiver (lambda (id) id)))
$$ language guile3 immutable;

create function f_x_execute_with_receiver_insert_stable() returns int as $$
(scalar (execute "insert into things values (3, 'fish') returning id"
                 #:receiver (lambda (id) id)))
$$ language guile3 stable;

select throws_ok(
  'select f_x_execute_with_receiver_insert_immutable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with receiver: immutable cannot change the db');

select throws_ok(
  'select f_x_execute_with_receiver_insert_stable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with receiver: stable cannot change the db');

create function f_x_cursor_open_insert_immutable() returns int as $$
(let ((c (cursor-open "insert into things values (3, 'fish') returning id")))
  (record-ref (car (fetch c)) 'id)) ;; '
$$ language guile3 immutable;

create function f_x_cursor_open_insert_stable() returns int as $$
(let ((c (cursor-open "insert into things values (3, 'fish') returning id")))
  (record-ref (car (fetch c)) 'id)) ;; '
$$ language guile3 stable;

select throws_ok(
  'select f_x_cursor_open_insert_immutable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with receiver: immutable cannot change the db');

select throws_ok(
  'select f_x_cursor_open_insert_stable()',
  'execute-error: ("0A000" "INSERT is not allowed in a non-volatile function" #f #f)',
  'execute with receiver: stable cannot change the db');

create function f_execute_with_receiver_simple() returns int as $$
(length (execute "select * from things order by id"
                 #:receiver (lambda (id name)
                              (if (= id 2)
                                  (stop-command-execution)
                                  name))))
$$ language guile3;

select is(f_execute_with_receiver_simple(), 1, 'execute_with_receiver: simple');

create function f_x_execute_with_receiver_wrong_type_command() returns int as $$
(length (execute 5
                 #:receiver (lambda (id name) (stop-command-execution))))
$$ language guile3;

create function f_x_execute_with_receiver_wrong_type_args_1() returns int as $$
(length (execute "select * from things order by id" #f
                 #:receiver (lambda (id name) (stop-command-execution))))
$$ language guile3;

create function f_x_execute_with_receiver_wrong_type_args_2() returns int as $$
(length (execute "select * from things order by id" (cons 1 2)
                 #:receiver (lambda (id name) (stop-command-execution))))
$$ language guile3;

create function f_x_execute_with_receiver_wrong_type_receiver() returns int as $$
(length (execute "select * from things order by id"
                 #:receiver 'moo-cow)) ;; '
$$ language guile3;

select throws_ok(
  'select f_x_execute_with_receiver_wrong_type_command()',
  'wrong-argument-type: (command "string" 5)',
  'execute_with_receiver: wrong_type_command');

select throws_ok(
  'select f_x_execute_with_receiver_wrong_type_args_1()',
  'wrong-argument-type: (args "list" #f)',
  'execute_with_receiver: wrong_type_args_1');

select throws_ok(
  'select f_x_execute_with_receiver_wrong_type_args_2()',
  'wrong-argument-type: (args "list" (1 . 2))',
  'execute_with_receiver: wrong_type_args_2');

select throws_ok(
  'select f_x_execute_with_receiver_wrong_type_receiver()',
  'wrong-argument-type: (receiver "procedure" moo-cow)',
  'execute_with_receiver: wrong_type_receiver');

create function f_cursor_simple() returns text as $$
(let ((c (cursor-open "select * from things order by id")))
  (record-ref (car (fetch c)) 'name)) ;; '
$$ language guile3;

select is(f_cursor_simple(), 'foo', 'cursor-open: simple');

create function f_x_cursor_wrong_type_command() returns text as $$
(let ((c (cursor-open 5)))
  (record-ref (car (fetch c)) 'name)) ;; '
$$ language guile3;

create function f_x_cursor_wrong_type_args_1() returns text as $$
(let ((c (cursor-open "select * from things order by id" #f)))
  (record-ref (car (fetch c)) 'name)) ;; '
$$ language guile3;

create function f_x_cursor_wrong_type_args_2() returns text as $$
(let ((c (cursor-open "select * from things order by id" (cons 1 2))))
  (record-ref (car (fetch c)) 'name)) ;; '
$$ language guile3;

select throws_ok(
  'select f_x_cursor_wrong_type_command()',
  'wrong-argument-type: (command "string" 5)',
  'cursor_open: wrong_type_command');

select throws_ok(
  'select f_x_cursor_wrong_type_args_1()',
  'wrong-argument-type: (args "list" #f)',
  'cursor_open: wrong_type_args_1');

select throws_ok(
  'select f_x_cursor_wrong_type_args_2()',
  'wrong-argument-type: (args "list" (1 . 2))',
  'cursor_open: wrong_type_args_2');

create function f_tr_new() returns trigger as 'new' language guile3;
create trigger tr_things_before_insert before insert on things
  for each row execute function f_tr_new();

create function insert_thing(id int, name text) returns int as $$
declare _result int;
begin
  insert into things values (id, name) returning things.id into _result;
  return _result;
end
$$ language plpgsql;

select is(insert_thing(4, 'more'), 4, 'simple trigger test');

create function f_tr_modify_id() returns trigger as $$
(record-set! new 'id (+ 1 (record-ref new 'id)))
new
$$ language guile3;

drop trigger tr_things_before_insert on things;
create trigger tr_things_before_insert before insert on things
  for each row execute function f_tr_modify_id();

select is(insert_thing(5, 'again'), 6, 'modifying before trigger test');

create table ddl_record (event text, tag text);

create function f_e_tr() returns event_trigger as $$
  (execute "insert into ddl_record values ('$1', '$2')" (list event tag))
$$ volatile language guile3;

create event trigger et_basic on ddl_command_start execute function f_e_tr();

create table extra_stuff (id int, name text);

select ok((select count(*) from ddl_record) = 1, 'basic event trigger test');

create table do_stuff (id int, label text);

do $$
  (execute "insert into do_stuff values (42, 'Don''t Panic.')")
$$ language guile3;

select ok((select count(*) from do_stuff) = 1, 'basic inline call test');

create function f_x_nested() returns int as $$
 (execute "do 'begin raise exception ''nested error''; end'")
$$ language guile3;

select throws_ok(
  'select f_x_nested()',
  'execute-error: ("P0001" "nested error" #f #f)',
  'nested error handling');

rollback;
