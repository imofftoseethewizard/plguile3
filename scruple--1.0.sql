-- scruple--1.0.sql

create schema plg3;

set search_path to plg3;

create table call_limit (
  role_id oid primary key,
  time float4,
  allocation bigint);

create table preamble (
  id bigserial primary key,
  src text not null);

create table eval_env (
  role_id oid primary key,
  preamble_id bigint references preamble (id) on delete cascade);

--   initial defaults are no preamble, a 1 second call limit, and a
--   1 MiB call allocation limt.
insert into call_limit values (0, 1.0, pow(2, 20));

create function get_role_preamble(id oid) returns text as $$
  select p.src
  from plg3.eval_env e, plg3.preamble p
  where
    e.role_id = id
    and e.preamble_id = p.id;
$$
language sql;

create function set_role_preamble(id oid, src text) returns void as $$
  delete from plg3.preamble
    where id = (select preamble_id from plg3.eval_env where role_id = id);

  -- delete cascades to eval_env, ensuring no conflict in this insert
  insert into plg3.eval_env
    values (id, (insert into plg3.preamble (src) values (src) returning id));
$$
language sql;

create function remove_role_preamble(id oid, src text) returns void as $$
  delete from plg3.preamble
    where id = (select preamble_id from plg3.eval_env where role_id = id);
  -- delete cascades to eval_env
$$
language sql;

create function set_role_call_time_limit(id oid, time_limit float4) returns void as $$
  insert into plg3.call_limit (role_id, time) values (id, time_limit)
    on conflict (role_id) do
      update set
        time = time_limit;
$$
language sql;

create function set_role_call_allocation_limit(id oid, allocation_limit float4) returns void as $$
  insert into plg3.call_limit (role_id, allocation) values (id, allocation_limit)
    on conflict (role_id) do
      update set
        allocation = allocation_limit;
$$
language sql;

create function plg3_call()
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function plg3_call_inline(internal)
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function plg3_compile(oid)
returns void
as 'MODULE_PATHNAME'
language c strict;

create language guile3
    handler plg3_call
    inline plg3_call_inline
    validator plg3_compile;

set search_path to public;

create function plg3_get_default_preamble() returns text as $$
  select plg3.get_role_preamble(0);
$$
language sql security definer;

create function plg3_get_default_call_time_limit() returns float4 as $$
  select plg3.get_role_call_time_limit(0);
$$
language sql security definer;

create function plg3_get_default_call_allocation_limit() returns bigint as $$
  select plg3.get_role_call_allocation_limit(0);
$$
language sql security definer;

create function plg3_set_default_preamble(src text) returns void as $$
  select plg3.set_role_preamble(0, src);
$$
language sql security definer;

create function plg3_set_default_call_time_limit(time_limit float4) returns void as $$
  select plg3.set_role_call_time_limit(0, time_limit);
$$
language sql security definer;

create function plg3_set_default_call_allocation_limit(allocation_limit bigint) returns void as $$
  select plg3.set_role_call_allocation_limit(0, allocation_limit);
$$
language sql security definer;

create function plg3_get_role_preamble(role_name text) returns text as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.get_role_preamble(u.usesysid) from u;
$$
language sql security definer;

create function plg3_set_role_preamble(role_name text, src text) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.set_role_preamble(u.usesysid, src) from u;
$$
language sql security definer;

create function plg3_remove_role_preamble(role_name text, src text) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.remove_role_preamble(u.usesysid, src) from u;
$$
language sql security definer;

create function plg3_set_role_call_time_limit(role_name text, time_limit float4) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.set_role_call_time_limit(u.usesysid, time_limit) from u;
$$
language sql security definer;

create function plg3_set_role_call_allocation_limit(role_name text, allocation_limit bigint) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.set_role_call_allocation_limit(u.usesysid, allocation_limit) from u;
$$
language sql security definer;
