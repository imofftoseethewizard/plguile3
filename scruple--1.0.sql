-- scruple--1.0.sql

create function scruple_call()
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function scruple_call_inline(internal)
returns language_handler
as 'MODULE_PATHNAME'
language c strict;

create function scruple_compile(oid)
returns void
as 'MODULE_PATHNAME'
language c strict;

create schema "%scruple";

create table "%scruple".context (
  role_id int primary key,
  forms text[]);

-- default forms use role_id 0
insert into "%scruple".context values (0, '{}'::text[]);

create function scruple_flush_func_cache_for_role(role_id int)
returns void
as 'MODULE_PATHNAME'
language c strict;

create function scruple_context_trigger_handler() returns trigger as $$
begin

  if TG_OP in ('INSERT', 'UPDATE') then
    perform scruple_flush_func_cache_for_role(NEW.role_id);
    return NEW;
  end if;

  perform scruple_flush_func_cache_for_role(OLD.role_id);
  return OLD;

end $$ language plpgsql;

create trigger scuple_context_trigger
  after insert or update or delete on "%scruple".context
  for each row execute procedure scruple_context_trigger_handler();

create function scruple_get_default_forms() returns text[] as $$
  select _scruple_get_role_forms(0);
$$
language sql volatile;

create function scruple_add_default_forms(new_forms text[]) returns void as $$
  select _scruple_add_role_forms(0, new_forms);
$$
language sql volatile;

create function scruple_set_default_forms(new_forms text[]) returns void as $$
  select _scruple_set_role_forms(0, new_forms);
$$
language sql volatile;

create function scruple_get_role_forms(role_name text) returns text[] as $$
  with u as (select * from pg_user where usename = role_name)
  select _scruple_get_role_forms(u.usesysid) from u;
$$
language sql volatile;

create function scruple_add_role_forms(role_name text, new_forms text[]) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select _scruple_add_role_forms(u.usesysid, new_forms) from u;
$$
language sql volatile;

create function scruple_set_role_forms(role_name text, new_forms text[]) returns void as $$
  with u as (select * from pg_user where usename = role_name)
  select _scruple_set_role_forms(u.usesysid, new_forms) from u;
$$
language sql volatile;

create function _scruple_get_role_forms(id oid) returns text[] as $$
  select forms from "%scruple".context where role_id = id;
$$
language sql volatile;

create function _scruple_add_role_forms(id oid, new_forms text[]) returns void as $$
  insert into "%scruple".context values (id, new_forms)
    on conflict (role_id) do
      update set forms = array_cat(_scruple_get_role_forms(id), new_forms);
$$
language sql volatile;

create function _scruple_set_role_forms(id oid, new_forms text[]) returns void as $$
  insert into "%scruple".context values (id, new_forms)
    on conflict (role_id) do
      update set forms = new_forms;
$$
language sql volatile;

create language scruple
    handler scruple_call
    inline scruple_call_inline
    validator scruple_compile;

select scruple_add_role_forms('foo', '{a,b}'::text[]);
