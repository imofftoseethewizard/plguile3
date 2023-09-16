begin;

select plan(77);

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
-- Type decimal/numeric
--

create function f_numeric_id(a numeric) returns numeric as 'a' language scruple;

select is(f_numeric_id(t.v), t.v, 'decimal: identity mapping test')
from (select '3.1415'::numeric(5,3)) t(v);

--------------------------------------------------------------------------------
--
-- Type money
--

create function f_money_id(a money) returns money as 'a' language scruple;

select is(f_money_id(t.v), t.v, 'money: identity mapping test')
from (select '2.28'::money) t(v);

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

--------------------------------------------------------------------------------
--
-- Type bytea
--

create function f_bytea_in(a bytea) returns int as '(bytevector-length a)' language scruple;
create function f_bytea_out(a int) returns bytea as '(make-bytevector a 42)' language scruple;
create function f_bytea_error(a int) returns bytea as 'a' language scruple;

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

create function f_tz_id(a timestamptz) returns timestamptz as 'a' language scruple;
create function f_tz_to_text(a timestamptz) returns text as '(date->string a "~Y-~m-~d ~H:~M:~f ~z")' language scruple;

select is(f_tz_id(t), t, 'timestamptz: identity mapping test')
from current_timestamp t;

select is(f_tz_to_text(t)::timestamptz, t, 'timestamptz: to scheme value check')
from current_timestamp t;

--------------------------------------------------------------------------------
--
-- Type timestamp
--

create function f_ts_id(a timestamp) returns timestamp as 'a' language scruple;
create function f_ts_to_text(a timestamp) returns text as '(date->string a "~Y-~m-~d ~H:~M:~f ~z")' language scruple;

select is(f_ts_id(t::timestamp), t::timestamp, 'timestamp: identity mapping test')
from current_timestamp t;

select is(f_ts_to_text(t::timestamp)::timestamp, t::timestamp, 'timestamp: to scheme value check')
from current_timestamp t;

--------------------------------------------------------------------------------
--
-- Type date
--

create function f_dt_id(a date) returns date as 'a' language scruple;
create function f_dt_to_text(a date) returns text as '(date->string a "~Y-~m-~d")' language scruple;

select is(f_dt_id(t::date), t::date, 'date: identity mapping test')
from current_date t;

select is(f_dt_to_text(t::date)::date, t::date, 'date: to scheme value check')
from current_date t;

--------------------------------------------------------------------------------
--
-- Type time
--

create function f_tm_id(a time) returns time as 'a' language scruple;
create function f_tm_to_text(a time) returns text as '(date->string (time-monotonic->date a) "~H:~M:~f")' language scruple;

select is(f_tm_id(t::time), t::time, 'time: identity mapping test')
from current_time t;

select is(f_tm_to_text(t::time)::time, t::time, 'time: to scheme value check')
from current_time t;

--------------------------------------------------------------------------------
--
-- Type interval
--

create function f_itv_id(a interval) returns interval as 'a' language scruple;

select is(f_itv_id(t.itv), t.itv, 'interval: identity mapping test')
from (select current_timestamp - current_date) t(itv);

--------------------------------------------------------------------------------
--
-- Type boolean
--

create function f_bool(a boolean) returns int as '(if a 1 0)' language scruple;
create function f_to_bool(a int) returns boolean as '(zero? a)' language scruple;

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

create function f_fruit_id(a fruit) returns fruit as 'a' language scruple;

select is(f_fruit_id('apple'::fruit), 'apple'::fruit, 'enum: identity mapping test 1');
select is(f_fruit_id('acorn'::fruit), 'acorn'::fruit, 'enum: identity mapping test 2');
select is(f_fruit_id('olive'::fruit), 'olive'::fruit, 'enum: identity mapping test 3');

--------------------------------------------------------------------------------
--
-- Type point
--

create function f_point_id(a point) returns point as 'a' language scruple;

select ok(f_point_id(t.point) ~= t.point, 'point: identity mapping test')
from (select point '(3.14, 2.72)') t(point);

--------------------------------------------------------------------------------
--
-- Type line
--

create function f_line_id(a line) returns line as 'a' language scruple;

select ok(f_line_id(t.line) ?|| t.line and f_line_id(t.line) <-> t.line = 0.0,
          'line: identity mapping test')
from (select line '((3.14, 2.72), (14, 42))') t(line);

--------------------------------------------------------------------------------
--
-- Type lseg
--

create function f_lseg_id(a lseg) returns lseg as 'a' language scruple;

select ok(f_lseg_id(t.lseg) ?|| t.lseg and f_lseg_id(t.lseg) <-> t.lseg = 0.0,
          'lseg: identity mapping test')
from (select lseg '((3.14, 2.72), (14, 42))') t(lseg);

--------------------------------------------------------------------------------
--
-- Type box
--

create function f_box_id(a box) returns box as 'a' language scruple;

select ok(f_box_id(t.box) ~= t.box, 'box: identity mapping test')
from (select box '((3.14, 2.72), (14, 42))') t(box);

--------------------------------------------------------------------------------
--
-- Type path
--

create function f_path_id(a path) returns path as 'a' language scruple;

select ok(f_path_id(t.path) = t.path, 'path: identity mapping test')
from (select path '((3.14, 2.72), (14, 42), (0, 0))') t(path);

--------------------------------------------------------------------------------
--
-- Type polygon
--

create function f_polygon_id(a polygon) returns polygon as 'a' language scruple;

select ok(f_polygon_id(t.polygon) ~= t.polygon, 'polygon: identity mapping test')
from (select polygon '((3.14, 2.72), (14, 42), (1, 1), (3.14, 2.72))') t(polygon);

--------------------------------------------------------------------------------
--
-- Type circle
--

create function f_circle_id(a circle) returns circle as 'a' language scruple;

select ok(f_circle_id(t.circle) ~= t.circle, 'circle: identity mapping test')
from (select circle '((3.14, 2.72), 42)') t(circle);

--------------------------------------------------------------------------------
--
-- Type inet
--

create function f_inet_id(a inet) returns inet as 'a' language scruple;

select ok(f_inet_id(t.inet) = t.inet, 'inet: identity mapping test ipv4')
from (select inet '192.168.100.128/25') t(inet);

select ok(f_inet_id(t.inet) = t.inet, 'inet: identity mapping test ipv6')
from (select inet '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128') t(inet);

--------------------------------------------------------------------------------
--
-- Type cidr
--

create function f_cidr_id(a cidr) returns cidr as 'a' language scruple;

select ok(f_cidr_id(t.cidr) = t.cidr, 'cidr: identity mapping test ipv4')
from (select cidr '192.168.100.128/25') t(cidr);

select ok(f_cidr_id(t.cidr) = t.cidr, 'cidr: identity mapping test ipv6')
from (select cidr '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128') t(cidr);

--------------------------------------------------------------------------------
--
-- Type macaddr
--

create function f_macaddr_id(a macaddr) returns macaddr as 'a' language scruple;

select ok(f_macaddr_id(t.macaddr) = t.macaddr, 'macaddr: identity mapping test')
from (select macaddr '08:00:2b:01:02:03') t(macaddr);

--------------------------------------------------------------------------------
--
-- Type macaddr8
--

create function f_macaddr8_id(a macaddr8) returns macaddr8 as 'a' language scruple;

select ok(f_macaddr8_id(t.macaddr8) = t.macaddr8, 'macaddr8: identity mapping test')
from (select macaddr8 '08:00:2b:01:02:03:04:05') t(macaddr8);

--------------------------------------------------------------------------------
--
-- Type bit
--

create function f_bit_id(a bit(6)) returns bit(6) as 'a' language scruple;

select ok(f_bit_id(t.bit) = t.bit, 'bit: identity mapping test')
from (select B'101010') t(bit);

--------------------------------------------------------------------------------
--
-- Type bit varying
--

create function f_varbit_id(a bit varying) returns bit varying as 'a' language scruple;

select ok(f_varbit_id(t.varbit) = t.varbit, 'bit varying: identity mapping test')
from (select B'101010111010011101011011010110000101') t(varbit);

--------------------------------------------------------------------------------
--
-- Type uuid
--

create function f_uuid_id(a uuid) returns uuid as 'a' language scruple;

select ok(f_uuid_id(t.uuid) = t.uuid, 'uuid: identity mapping test')
from (select gen_random_uuid()) t(uuid);

--------------------------------------------------------------------------------
--
-- Type xml
--

create function f_xml_id(a xml) returns xml as 'a' language scruple;

select ok(xpath('/foo/text()', f_xml_id(t.xml))::text = '{bar}', 'xml: identity mapping test')
from (select xml '<foo>bar</foo>') t(xml);

--------------------------------------------------------------------------------
--
-- Type json
--

create function f_json_id(a json) returns json as 'a' language scruple;

select ok(f_json_id(t.json)->'foo'->>0 = 'bar', 'json: identity mapping test')
from (select '{"foo": ["bar", 42]}'::json) t(json);

--------------------------------------------------------------------------------
--
-- Type jsonb
--

create function f_jsonb_id(a jsonb) returns jsonb as 'a' language scruple;

do $$ begin
raise notice '%', f_jsonb_id('{"foo": ["bar", 42]}'::jsonb)->'foo';
end $$;

select ok(f_jsonb_id(t.jsonb)->'foo'->>0 = 'bar', 'jsonb: identity mapping test')
from (select '{"foo": ["bar", 42]}'::jsonb) t(jsonb);

select * from finish();

rollback;
