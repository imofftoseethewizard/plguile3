-- plg3--1.0.sql

create schema plguile3;

set search_path to plguile3;

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

create function get_role_preamble(role_id oid) returns text as $$
begin
  return (
    select p.src
    from plguile3.eval_env e, plguile3.preamble p
    where
      e.role_id = get_role_preamble.role_id
      and e.preamble_id = p.id
  );
end
$$
language plpgsql stable;

create function set_role_preamble(id oid, src text) returns void as $$
declare preamble_id bigint;
begin

  perform plguile3.remove_role_preamble(id);

  if not (src is null) then

    insert into plguile3.preamble (src)
           values (src)
           returning plguile3.preamble.id into preamble_id;

    insert into plguile3.eval_env values (id, preamble_id);

  end if;

end
$$
language plpgsql;

create function remove_role_preamble(id oid) returns void as $$

  delete from plguile3.preamble p
    where p.id = (
      select preamble_id
      from plguile3.eval_env
      where role_id = remove_role_preamble.id
    );
  -- delete cascades to eval_env

$$
language sql;

create function set_role_call_time_limit(id oid, time_limit float4) returns void as $$
  insert into plguile3.call_limit (role_id, time) values (id, time_limit)
    on conflict (role_id) do
      update set
        time = time_limit;
$$
language sql;

create function get_role_call_allocation_limit(id oid) returns bigint as $$
  select allocation from plguile3.call_limit where role_id = id;
$$
language sql stable;

create function get_role_call_time_limit(id oid) returns float4 as $$
  select time from plguile3.call_limit where role_id = id;
$$
language sql;

create function set_role_call_allocation_limit(id oid, allocation_limit bigint) returns void as $$
  insert into plguile3.call_limit (role_id, allocation) values (id, allocation_limit)
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

create trusted language guile3
    handler plg3_call
    inline plg3_call_inline
    validator plg3_compile;

set search_path to public;

create function g3_get_default_preamble() returns text as $$
  select plguile3.get_role_preamble(0);
$$
language sql security definer stable;
revoke all on function g3_get_default_preamble from public;

create function g3_get_default_call_time_limit() returns float4 as $$
  select plguile3.get_role_call_time_limit(0);
$$
language sql security definer stable;
revoke all on function g3_get_default_call_time_limit from public;

create function g3_get_default_call_allocation_limit() returns bigint as $$
  select plguile3.get_role_call_allocation_limit(0);
$$
language sql security definer stable;
revoke all on function g3_get_default_call_allocation_limit from public;

create function g3_set_default_preamble(src text) returns void as $$
  select plguile3.set_role_preamble(0, src);
$$
language sql security definer;
revoke all on function g3_set_default_preamble from public;

create function g3_set_default_call_time_limit(time_limit float4) returns void as $$
  select plguile3.set_role_call_time_limit(0, time_limit);
$$
language sql security definer;
revoke all on function g3_set_default_call_time_limit from public;

create function g3_set_default_call_allocation_limit(allocation_limit bigint) returns void as $$
  select plguile3.set_role_call_allocation_limit(0, allocation_limit);
$$
language sql security definer;
revoke all on function g3_set_default_call_allocation_limit from public;

create function g3_get_role_preamble(role_name text) returns text as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_preamble(r.oid) from r;
$$
language sql security definer stable;
revoke all on function g3_get_role_preamble from public;

create function g3_get_role_call_allocation_limit(role_name text) returns bigint as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_call_allocation_limit(r.oid) from r;
$$
language sql security definer stable;
revoke all on function g3_get_role_call_allocation_limit from public;

create function g3_get_role_call_time_limit(role_name text) returns float4 as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_call_time_limit(r.oid) from r;
$$
language sql security definer stable;
revoke all on function g3_get_role_call_time_limit from public;

create function g3_set_role_preamble(role_name text, src text) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_preamble(r.oid, src) from r;
$$
language sql security definer;
revoke all on function g3_set_role_preamble from public;

create function g3_remove_role_preamble(role_name text) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.remove_role_preamble(r.oid) from r;
$$
language sql security definer;
revoke all on function g3_remove_role_preamble from public;

create function g3_set_role_call_time_limit(role_name text, time_limit float4) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_call_time_limit(r.oid, time_limit) from r;
$$
language sql security definer;
revoke all on function g3_set_role_call_time_limit from public;

create function g3_set_role_call_allocation_limit(role_name text, allocation_limit bigint) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_call_allocation_limit(r.oid, allocation_limit) from r;
$$
language sql security definer;
revoke all on function g3_set_role_call_allocation_limit from public;

create function g3_get_preamble() returns text as $$
  select coalesce(
    g3_get_session_user_preamble(),
    g3_get_default_preamble()
  );
$$
language sql security definer stable;
revoke all on function g3_get_preamble from public;

create function g3_get_call_allocation_limit() returns bigint as $$
  select coalesce(
    g3_get_session_user_call_allocation_limit(),
    g3_get_default_call_allocation_limit()
  );
$$
language sql security definer stable;
revoke all on function g3_get_call_allocation_limit from public;

create function g3_get_call_time_limit() returns float4 as $$
  select coalesce(
    g3_get_session_user_call_time_limit(),
    g3_get_default_call_time_limit()
  );
$$
language sql security definer stable;
revoke all on function g3_get_call_time_limit from public;

create function g3_get_session_user_preamble() returns text as $$
  select g3_get_role_preamble(session_user);
$$
language sql security definer stable;
revoke all on function g3_get_session_user_preamble from public;

create function g3_get_session_user_call_allocation_limit() returns bigint as $$
  select g3_get_role_call_allocation_limit(session_user);
$$
language sql security definer stable;
revoke all on function g3_get_session_user_call_allocation_limit from public;

create function g3_get_session_user_call_time_limit() returns float4 as $$
  select g3_get_role_call_time_limit(session_user);
$$
language sql security definer stable;
revoke all on function g3_get_session_user_call_time_limit from public;

create function g3_set_session_user_preamble(src text) returns void as $$
  select g3_set_role_preamble(session_user, src);
$$
language sql security definer;
revoke all on function g3_set_session_user_preamble from public;

create function g3_remove_session_user_preamble() returns void as $$
  select g3_remove_role_preamble(session_user);
$$
language sql security definer;
revoke all on function g3_remove_session_user_preamble from public;

create function g3_set_session_user_call_time_limit(time_limit float4) returns void as $$
  select g3_set_role_call_time_limit(session_user, time_limit);
$$
language sql security definer;
revoke all on function g3_set_session_user_call_time_limit from public;

create function g3_set_session_user_call_allocation_limit(allocation_limit bigint) returns void as $$
  select g3_set_role_call_allocation_limit(session_user, allocation_limit);
$$
language sql security definer;
revoke all on function g3_set_session_user_call_allocation_limit from public;
