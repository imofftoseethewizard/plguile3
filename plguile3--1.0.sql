-- plguile3--1.0.sql

create schema plguile3;

set search_path to plguile3;

create function plguile3_check_prelude(src text)
returns void
as 'MODULE_PATHNAME'
language c strict;

create table call_limit (
  role_id oid primary key,
  time float4,
  allocation bigint);

create table module (
  id bigserial primary key,
  owner_id oid, -- owner may update and delete (in addition to postgres)
  name text not null,
  src text not null);

create unique index ix_module_owner_id_name on module (owner_id, name);

create index ix_module_name on module (name);

create table create_public_module_permission (
  role_id oid primary key
);

create table prelude (
  id bigserial primary key,
  src text not null);

create table eval_env (
  role_id oid primary key,
  prelude_id bigint references prelude (id) on delete cascade);

--   initial defaults are no prelude, a 1 second call limit, and a
--   1 MiB call allocation limt.
insert into call_limit values (0, 1.0, pow(2, 20));

create function get_role_prelude(role_id oid) returns text as $$
begin
  return (
    select p.src
    from plguile3.eval_env e, plguile3.prelude p
    where
      e.role_id = get_role_prelude.role_id
      and e.prelude_id = p.id
  );
end
$$
language plpgsql stable;

create function set_role_prelude(id oid, src text) returns void as $$
declare prelude_id bigint;
begin

  perform plguile3.plguile3_check_prelude(src);

  perform plguile3.remove_role_prelude(id);

  if not (src is null) then

    insert into plguile3.prelude (src)
           values (src)
           returning plguile3.prelude.id into prelude_id;

    insert into plguile3.eval_env values (id, prelude_id);

  end if;

end
$$
language plpgsql;

create function remove_role_prelude(id oid) returns void as $$

  delete from plguile3.prelude p
    where p.id = (
      select prelude_id
      from plguile3.eval_env
      where role_id = remove_role_prelude.id
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

create function plguile3_call()
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function plguile3_call_inline(internal)
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function plguile3_compile(oid)
returns void
as 'MODULE_PATHNAME'
language c strict;

create trusted language guile3
    handler plguile3_call
    inline plguile3_call_inline
    validator plguile3_compile;

set search_path to public;

create function guile3_get_default_prelude() returns text as $$
  select plguile3.get_role_prelude(0);
$$
language sql security definer stable;
revoke all on function guile3_get_default_prelude from public;

create function guile3_get_default_call_time_limit() returns float4 as $$
  select plguile3.get_role_call_time_limit(0);
$$
language sql security definer stable;
revoke all on function guile3_get_default_call_time_limit from public;

create function guile3_get_default_call_allocation_limit() returns bigint as $$
  select plguile3.get_role_call_allocation_limit(0);
$$
language sql security definer stable;
revoke all on function guile3_get_default_call_allocation_limit from public;

create function guile3_set_default_prelude(src text) returns void as $$
  select plguile3.set_role_prelude(0, src);
$$
language sql security definer;
revoke all on function guile3_set_default_prelude from public;

create function guile3_set_default_call_time_limit(time_limit float4) returns void as $$
  select plguile3.set_role_call_time_limit(0, time_limit);
$$
language sql security definer;
revoke all on function guile3_set_default_call_time_limit from public;

create function guile3_set_default_call_allocation_limit(allocation_limit bigint) returns void as $$
  select plguile3.set_role_call_allocation_limit(0, allocation_limit);
$$
language sql security definer;
revoke all on function guile3_set_default_call_allocation_limit from public;

create function guile3_get_role_prelude(role_name text) returns text as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_prelude(r.oid) from r;
$$
language sql security definer stable;
revoke all on function guile3_get_role_prelude from public;

create function guile3_get_role_call_allocation_limit(role_name text) returns bigint as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_call_allocation_limit(r.oid) from r;
$$
language sql security definer stable;
revoke all on function guile3_get_role_call_allocation_limit from public;

create function guile3_get_role_call_time_limit(role_name text) returns float4 as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.get_role_call_time_limit(r.oid) from r;
$$
language sql security definer stable;
revoke all on function guile3_get_role_call_time_limit from public;

create function guile3_set_role_prelude(role_name text, src text) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_prelude(r.oid, src) from r;
$$
language sql security definer;
revoke all on function guile3_set_role_prelude from public;

create function guile3_remove_role_prelude(role_name text) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.remove_role_prelude(r.oid) from r;
$$
language sql security definer;
revoke all on function guile3_remove_role_prelude from public;

create function guile3_set_role_call_time_limit(role_name text, time_limit float4) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_call_time_limit(r.oid, time_limit) from r;
$$
language sql security definer;
revoke all on function guile3_set_role_call_time_limit from public;

create function guile3_set_role_call_allocation_limit(role_name text, allocation_limit bigint) returns void as $$
  with r as (select * from pg_roles where rolname = role_name)
  select plguile3.set_role_call_allocation_limit(r.oid, allocation_limit) from r;
$$
language sql security definer;
revoke all on function guile3_set_role_call_allocation_limit from public;

create function guile3_get_prelude() returns text as $$
  select coalesce(
    guile3_get_session_user_prelude(),
    guile3_get_default_prelude()
  );
$$
language sql security definer stable;
revoke all on function guile3_get_prelude from public;

create function guile3_get_call_allocation_limit() returns bigint as $$
  select coalesce(
    guile3_get_session_user_call_allocation_limit(),
    guile3_get_default_call_allocation_limit()
  );
$$
language sql security definer stable;
revoke all on function guile3_get_call_allocation_limit from public;

create function guile3_get_call_time_limit() returns float4 as $$
  select coalesce(
    guile3_get_session_user_call_time_limit(),
    guile3_get_default_call_time_limit()
  );
$$
language sql security definer stable;
revoke all on function guile3_get_call_time_limit from public;

create function guile3_get_session_user_prelude() returns text as $$
  select guile3_get_role_prelude(session_user);
$$
language sql security definer stable;
revoke all on function guile3_get_session_user_prelude from public;

create function guile3_get_session_user_call_allocation_limit() returns bigint as $$
  select guile3_get_role_call_allocation_limit(session_user);
$$
language sql security definer stable;
revoke all on function guile3_get_session_user_call_allocation_limit from public;

create function guile3_get_session_user_call_time_limit() returns float4 as $$
  select guile3_get_role_call_time_limit(session_user);
$$
language sql security definer stable;
revoke all on function guile3_get_session_user_call_time_limit from public;

create function guile3_set_session_user_prelude(src text) returns void as $$
  select guile3_set_role_prelude(session_user, src);
$$
language sql security definer;
revoke all on function guile3_set_session_user_prelude from public;

create function guile3_remove_session_user_prelude() returns void as $$
  select guile3_remove_role_prelude(session_user);
$$
language sql security definer;
revoke all on function guile3_remove_session_user_prelude from public;

create function guile3_set_session_user_call_time_limit(time_limit float4) returns void as $$
  select guile3_set_role_call_time_limit(session_user, time_limit);
$$
language sql security definer;
revoke all on function guile3_set_session_user_call_time_limit from public;

create function guile3_set_session_user_call_allocation_limit(allocation_limit bigint) returns void as $$
  select guile3_set_role_call_allocation_limit(session_user, allocation_limit);
$$
language sql security definer;
revoke all on function guile3_set_session_user_call_allocation_limit from public;
