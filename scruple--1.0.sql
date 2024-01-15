-- scruple--1.0.sql

create schema plg3;

set search_path to plg3, public;

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

create table eval_env (
  role_id int primary key,
  forms text[],
  call_time_limit float8,
  call_allocation_limit int);

-- default forms use role_id 0
--   initial defaults have no forms, a 1 second call limit, and a
--   1 MiB call allocation limt.
insert into eval_env values (0, '{}'::text[], 1.0, pow(2, 20));

create function plg3_role_forms_changed(role_id int)
returns void
as 'MODULE_PATHNAME'
language c strict;

create function eval_env_trigger_handler() returns trigger as $$
begin

  if TG_OP in ('INSERT', 'UPDATE') then
    perform plg3.plg3_role_forms_changed(NEW.role_id);
    return NEW;
  end if;

  perform plg3.plg3_role_forms_changed(OLD.role_id);
  return OLD;

end $$ language plpgsql;

create trigger eval_env_trigger
  after insert or update or delete on plg3.eval_env
  for each row execute procedure eval_env_trigger_handler();

create function get_role_forms(id oid) returns text[] as $$
  select forms from plg3.eval_env where role_id = id;
$$
language sql volatile;

create function add_role_forms(id oid, new_forms text[]) returns void as $$
  insert into plg3.eval_env (role_id, forms) values (id, new_forms)
    on conflict (role_id) do
      update set forms = array_cat(plg3.get_role_forms(id), new_forms);
$$
language sql volatile;

create function set_role_forms(id oid, new_forms text[]) returns void as $$
  insert into plg3.eval_env (role_id, forms) values (id, new_forms)
    on conflict (role_id) do
      update set forms = new_forms;
$$
language sql volatile;

create language guile3
    handler plg3_call
    inline plg3_call_inline
    validator plg3_compile;

set search_path to public;

create function plg3_get_default_forms() returns text[] as $$
  select plg3.get_role_forms(0);
$$
language sql volatile;

create function plg3_add_default_forms(new_forms text[]) returns void as $$
  select plg3.add_role_forms(0, new_forms);
$$
language sql volatile;

create function plg3_set_default_forms(new_forms text[]) returns void as $$
  select plg3.set_role_forms(0, new_forms);
$$
language sql volatile;

create function plg3_get_role_forms(role_name text) returns text[] as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.get_role_forms(u.usesysid) from u;
$$
language sql volatile;

create function plg3_add_role_forms(role_name text, new_forms text[]) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.add_role_forms(u.usesysid, new_forms) from u;
$$
language sql volatile;

create function plg3_set_role_forms(role_name text, new_forms text[]) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select plg3.set_role_forms(u.usesysid, new_forms) from u;
$$
language sql volatile;
