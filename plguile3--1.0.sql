-- plguile3--1.0.sql

create schema plguile3;

set search_path to plguile3;

create table call_limit (
  role_id oid primary key,
  time float4,
  allocation bigint);

create table module (
  id bigserial primary key,
  owner_id oid, -- owner may update and delete (in addition to postgres)
  name text[] not null,
  src text not null);

create unique index ix_module_owner_id_name on module (owner_id, name);

create index ix_module_name on module (name);

insert into module (name, src)
values
  ('{ice-9,and-let-star}',        '(define-module (ice-9 and-let-star))        (re-export-curated-builtin-module (ice-9 and-let-star))'),
  ('{ice-9,arrays}',              '(define-module (ice-9 arrays))              (re-export-curated-builtin-module (ice-9 arrays))'),
  ('{ice-9,atomic}',              '(define-module (ice-9 atomic))              (re-export-curated-builtin-module (ice-9 atomic))'),
  ('{ice-9,binary-ports}',        '(define-module (ice-9 binary-ports))        (re-export-curated-builtin-module (ice-9 binary-ports))'),
  ('{ice-9,calling}',             '(define-module (ice-9 calling))             (re-export-curated-builtin-module (ice-9 calling))'),
  ('{ice-9,common-list}',         '(define-module (ice-9 common-list))         (re-export-curated-builtin-module (ice-9 common-list))'),
  ('{ice-9,control}',             '(define-module (ice-9 control))             (re-export-curated-builtin-module (ice-9 control))'),
  ('{ice-9,copy-tree}',           '(define-module (ice-9 copy-tree))           (re-export-curated-builtin-module (ice-9 copy-tree))'),
  ('{ice-9,curried-definitions}', '(define-module (ice-9 curried-definitions)) (re-export-curated-builtin-module (ice-9 curried-definitions))'),
  ('{ice-9,exceptions}',          '(define-module (ice-9 exceptions))          (re-export-curated-builtin-module (ice-9 exceptions))'),
  ('{ice-9,format}',              '(define-module (ice-9 format))              (re-export-curated-builtin-module (ice-9 format))'),
  ('{ice-9,futures}',             '(define-module (ice-9 futures))             (re-export-curated-builtin-module (ice-9 futures))'),
  ('{ice-9,gap-buffer}',          '(define-module (ice-9 gap-buffer))          (re-export-curated-builtin-module (ice-9 gap-buffer))'),
  ('{ice-9,hash-table}',          '(define-module (ice-9 hash-table))          (re-export-curated-builtin-module (ice-9 hash-table))'),
  ('{ice-9,hcons}',               '(define-module (ice-9 hcons))               (re-export-curated-builtin-module (ice-9 hcons))'),
  ('{ice-9,i18n}',                '(define-module (ice-9 i18n))                (re-export-curated-builtin-module (ice-9 i18n))'),
  ('{ice-9,iconv}',               '(define-module (ice-9 iconv))               (re-export-curated-builtin-module (ice-9 iconv))'),
  ('{ice-9,lineio}',              '(define-module (ice-9 lineio))              (re-export-curated-builtin-module (ice-9 lineio))'),
  ('{ice-9,list}',                '(define-module (ice-9 list))                (re-export-curated-builtin-module (ice-9 list))'),
  ('{ice-9,match}',               '(define-module (ice-9 match))               (re-export-curated-builtin-module (ice-9 match))'),
  ('{ice-9,occam-channel}',       '(define-module (ice-9 occam-channel))       (re-export-curated-builtin-module (ice-9 occam-channel))'),
  ('{ice-9,peg}',                 '(define-module (ice-9 peg))                 (re-export-curated-builtin-module (ice-9 peg))'),
  ('{ice-9,peg,cache}',           '(define-module (ice-9 peg cache))           (re-export-curated-builtin-module (ice-9 peg cache))'),
  ('{ice-9,peg,codegen}',         '(define-module (ice-9 peg codegen))         (re-export-curated-builtin-module (ice-9 peg codegen))'),
  ('{ice-9,peg,simplify-tree}',   '(define-module (ice-9 peg simplify-tree))   (re-export-curated-builtin-module (ice-9 peg simplify-tree))'),
  ('{ice-9,peg,string-peg}',      '(define-module (ice-9 peg string-peg))      (re-export-curated-builtin-module (ice-9 peg string-peg))'),
  ('{ice-9,peg,using-parsers}',   '(define-module (ice-9 peg using-parsers))   (re-export-curated-builtin-module (ice-9 peg using-parsers))'),
  ('{ice-9,poe}',                 '(define-module (ice-9 poe))                 (re-export-curated-builtin-module (ice-9 poe))'),
  ('{ice-9,ports}',               '(define-module (ice-9 ports))               (re-export-curated-builtin-module (ice-9 ports))'),
  ('{ice-9,pretty-print}',        '(define-module (ice-9 pretty-print))        (re-export-curated-builtin-module (ice-9 pretty-print))'),
  ('{ice-9,q}',                   '(define-module (ice-9 q))                   (re-export-curated-builtin-module (ice-9 q))'),
  ('{ice-9,rdelim}',              '(define-module (ice-9 rdelim))              (re-export-curated-builtin-module (ice-9 rdelim))'),
  ('{ice-9,receive}',             '(define-module (ice-9 receive))             (re-export-curated-builtin-module (ice-9 receive))'),
  ('{ice-9,regex}',               '(define-module (ice-9 regex))               (re-export-curated-builtin-module (ice-9 regex))'),
  ('{ice-9,runq}',                '(define-module (ice-9 runq))                (re-export-curated-builtin-module (ice-9 runq))'),
  ('{ice-9,serialize}',           '(define-module (ice-9 serialize))           (re-export-curated-builtin-module (ice-9 serialize))'),
  ('{ice-9,stack-catch}',         '(define-module (ice-9 stack-catch))         (re-export-curated-builtin-module (ice-9 stack-catch))'),
  ('{ice-9,streams}',             '(define-module (ice-9 streams))             (re-export-curated-builtin-module (ice-9 streams))'),
  ('{ice-9,string-fun}',          '(define-module (ice-9 string-fun))          (re-export-curated-builtin-module (ice-9 string-fun))'),
  ('{ice-9,suspendable-ports}',   '(define-module (ice-9 suspendable-ports))   (re-export-curated-builtin-module (ice-9 suspendable-ports))'),
  ('{ice-9,textual-ports}',       '(define-module (ice-9 textual-ports))       (re-export-curated-builtin-module (ice-9 textual-ports))'),
  ('{ice-9,threads}',             '(define-module (ice-9 threads))             (re-export-curated-builtin-module (ice-9 threads))'),
  ('{ice-9,time}',                '(define-module (ice-9 time))                (re-export-curated-builtin-module (ice-9 time))'),
  ('{ice-9,unicode}',             '(define-module (ice-9 unicode))             (re-export-curated-builtin-module (ice-9 unicode))'),
  ('{ice-9,vlist}',               '(define-module (ice-9 vlist))               (re-export-curated-builtin-module (ice-9 vlist))'),
  ('{ice-9,weak-vector}',         '(define-module (ice-9 weak-vector))         (re-export-curated-builtin-module (ice-9 weak-vector))'),
  ('{oop,goops}',                 '(define-module (oop goops))                 (re-export-curated-builtin-module (oop goops))'),
  ('{oop,goops,accessors}',       '(define-module (oop goops accessors))       (re-export-curated-builtin-module (oop goops accessors))'),
  ('{oop,goops,active-slot}',     '(define-module (oop goops active-slot))     (re-export-curated-builtin-module (oop goops active-slot))'),
  ('{oop,goops,composite-slot}',  '(define-module (oop goops composite-slot))  (re-export-curated-builtin-module (oop goops composite-slot))'),
  ('{oop,goops,describe}',        '(define-module (oop goops describe))        (re-export-curated-builtin-module (oop goops describe))'),
  ('{oop,goops,save}',            '(define-module (oop goops save))            (re-export-curated-builtin-module (oop goops save))'),
  ('{oop,goops,simple}',          '(define-module (oop goops simple))          (re-export-curated-builtin-module (oop goops simple))'),
  ('{scheme,char}',               '(define-module (scheme char))               (re-export-curated-builtin-module (scheme char))'),
  ('{scheme,lazy}',               '(define-module (scheme lazy))               (re-export-curated-builtin-module (scheme lazy))'),
  ('{scheme,time}',               '(define-module (scheme time))               (re-export-curated-builtin-module (scheme time))'),
  ('{scheme,write}',              '(define-module (scheme write))              (re-export-curated-builtin-module (scheme write))'),
  ('{srfi,srfi-1}',               '(define-module (srfi srfi-1))               (re-export-curated-builtin-module (srfi srfi-1))'),
  ('{srfi,srfi-2}',               '(define-module (srfi srfi-2))               (re-export-curated-builtin-module (srfi srfi-2))'),
  ('{srfi,srfi-4}',               '(define-module (srfi srfi-4))               (re-export-curated-builtin-module (srfi srfi-4))'),
  ('{srfi,srfi-4,gnu}',           '(define-module (srfi srfi-4 gnu))           (re-export-curated-builtin-module (srfi srfi-4 gnu))'),
  ('{srfi,srfi-6}',               '(define-module (srfi srfi-6))               (re-export-curated-builtin-module (srfi srfi-6))'),
  ('{srfi,srfi-8}',               '(define-module (srfi srfi-8))               (re-export-curated-builtin-module (srfi srfi-8))'),
  ('{srfi,srfi-9}',               '(define-module (srfi srfi-9))               (re-export-curated-builtin-module (srfi srfi-9))'),
  ('{srfi,srfi-9,gnu}',           '(define-module (srfi srfi-9 gnu))           (re-export-curated-builtin-module (srfi srfi-9 gnu))'),
  ('{srfi,srfi-11}',              '(define-module (srfi srfi-11))              (re-export-curated-builtin-module (srfi srfi-11))'),
  ('{srfi,srfi-13}',              '(define-module (srfi srfi-13))              (re-export-curated-builtin-module (srfi srfi-13))'),
  ('{srfi,srfi-14}',              '(define-module (srfi srfi-14))              (re-export-curated-builtin-module (srfi srfi-14))'),
  ('{srfi,srfi-16}',              '(define-module (srfi srfi-16))              (re-export-curated-builtin-module (srfi srfi-16))'),
  ('{srfi,srfi-17}',              '(define-module (srfi srfi-17))              (re-export-curated-builtin-module (srfi srfi-17))'),
  ('{srfi,srfi-18}',              '(define-module (srfi srfi-18))              (re-export-curated-builtin-module (srfi srfi-18))'),
  ('{srfi,srfi-19}',              '(define-module (srfi srfi-19))              (re-export-curated-builtin-module (srfi srfi-19))'),
  ('{srfi,srfi-26}',              '(define-module (srfi srfi-26))              (re-export-curated-builtin-module (srfi srfi-26))'),
  ('{srfi,srfi-27}',              '(define-module (srfi srfi-27))              (re-export-curated-builtin-module (srfi srfi-27))'),
  ('{srfi,srfi-28}',              '(define-module (srfi srfi-28))              (re-export-curated-builtin-module (srfi srfi-28))'),
  ('{srfi,srfi-31}',              '(define-module (srfi srfi-31))              (re-export-curated-builtin-module (srfi srfi-31))'),
  ('{srfi,srfi-34}',              '(define-module (srfi srfi-34))              (re-export-curated-builtin-module (srfi srfi-34))'),
  ('{srfi,srfi-35}',              '(define-module (srfi srfi-35))              (re-export-curated-builtin-module (srfi srfi-35))'),
  ('{srfi,srfi-38}',              '(define-module (srfi srfi-38))              (re-export-curated-builtin-module (srfi srfi-38))'),
  ('{srfi,srfi-39}',              '(define-module (srfi srfi-39))              (re-export-curated-builtin-module (srfi srfi-39))'),
  ('{srfi,srfi-41}',              '(define-module (srfi srfi-41))              (re-export-curated-builtin-module (srfi srfi-41))'),
  ('{srfi,srfi-42}',              '(define-module (srfi srfi-42))              (re-export-curated-builtin-module (srfi srfi-42))'),
  ('{srfi,srfi-43}',              '(define-module (srfi srfi-43))              (re-export-curated-builtin-module (srfi srfi-43))'),
  ('{srfi,srfi-45}',              '(define-module (srfi srfi-45))              (re-export-curated-builtin-module (srfi srfi-45))'),
  ('{srfi,srfi-60}',              '(define-module (srfi srfi-60))              (re-export-curated-builtin-module (srfi srfi-60))'),
  ('{srfi,srfi-64}',              '(define-module (srfi srfi-64))              (re-export-curated-builtin-module (srfi srfi-64))'),
  ('{srfi,srfi-67}',              '(define-module (srfi srfi-67))              (re-export-curated-builtin-module (srfi srfi-67))'),
  ('{srfi,srfi-69}',              '(define-module (srfi srfi-69))              (re-export-curated-builtin-module (srfi srfi-69))'),
  ('{srfi,srfi-71}',              '(define-module (srfi srfi-71))              (re-export-curated-builtin-module (srfi srfi-71))'),
  ('{srfi,srfi-88}',              '(define-module (srfi srfi-88))              (re-export-curated-builtin-module (srfi srfi-88))'),
  ('{srfi,srfi-171}',             '(define-module (srfi srfi-171))             (re-export-curated-builtin-module (srfi srfi-171))'),
  ('{sxml,apply-templates}',      '(define-module (sxml apply-templates))      (re-export-curated-builtin-module (sxml apply-templates))'),
  ('{sxml,fold}',                 '(define-module (sxml fold))                 (re-export-curated-builtin-module (sxml fold))'),
  ('{sxml,match}',                '(define-module (sxml match))                (re-export-curated-builtin-module (sxml match))'),
  ('{sxml,simple}',               '(define-module (sxml simple))               (re-export-curated-builtin-module (sxml simple))'),
  ('{sxml,ssax}',                 '(define-module (sxml ssax))                 (re-export-curated-builtin-module (sxml ssax))'),
  ('{sxml,ssax,input-parse}',     '(define-module (sxml ssax input-parse))     (re-export-curated-builtin-module (sxml ssax input-parse))'),
  ('{sxml,ssax,transform}',       '(define-module (sxml ssax transform))       (re-export-curated-builtin-module (sxml ssax transform))'),
  ('{sxml,xpath}',                '(define-module (sxml xpath))                (re-export-curated-builtin-module (sxml xpath))'),
  ('{texinfo}',                   '(define-module (texinfo))                   (re-export-curated-builtin-module (texinfo))'),
  ('{texinfo,docbook}',           '(define-module (texinfo docbook))           (re-export-curated-builtin-module (texinfo docbook))'),
  ('{texinfo,html}',              '(define-module (texinfo html))              (re-export-curated-builtin-module (texinfo html))'),
  ('{texinfo,indexing}',          '(define-module (texinfo indexing))          (re-export-curated-builtin-module (texinfo indexing))'),
  ('{texinfo,plain-text}',        '(define-module (texinfo plain-text))        (re-export-curated-builtin-module (texinfo plain-text))'),
  ('{texinfo,relection}',         '(define-module (texinfo relection))         (re-export-curated-builtin-module (texinfo relection))'),
  ('{texinfo,serialize}',         '(define-module (texinfo serialize))         (re-export-curated-builtin-module (texinfo serialize))'),
  ('{texinfo,string-utils}',      '(define-module (texinfo string-utils))      (re-export-curated-builtin-module (texinfo string-utils))'),
  ('{web,http}',                  '(define-module (web http))                  (re-export-curated-builtin-module (web http))'),
  ('{web,request}',               '(define-module (web request))               (re-export-curated-builtin-module (web request))'),
  ('{web,response}',              '(define-module (web response))              (re-export-curated-builtin-module (web response))'),
  ('{web,uri}',                   '(define-module (web uri))                   (re-export-curated-builtin-module (web uri))');

create table create_public_module_permission (
  role_id oid primary key
);

create function guile3_grant_create_public_module(role_id oid)
  returns void
  language sql
as $$
  insert into plguile3.create_public_module_permission values (role_id)
  on conflict do nothing;
$$;

create function guile3_grant_create_public_module(role_name text)
  returns void
  language sql
as $$
  select plguile3.guile3_grant_create_public_module(usesysid)
  from pg_user
  where usename = role_name
$$;

create function guile3_revoke_create_public_module(role_id oid)
  returns void
  language sql
as $$
  delete from plguile3.create_public_module_permission where role_id = $1
$$;

create function guile3_revoke_create_public_module(role_name text)
  returns void
  language sql
as $$
  select plguile3.guile3_revoke_create_public_module(usesysid)
  from pg_user
  where usename = role_name
$$;

create function guile3_has_create_public_module_permission(role_id oid)
  returns bool
  language sql
  stable
as $$
  select exists (
    select 1
    from plguile3.create_public_module_permission p
    where p.role_id = role_id
  )
$$;

create function guile3_has_create_public_module_permission(role_name text)
  returns bool
  language sql
  stable
as $$
  select plguile3.guile3_has_create_public_module_permission(usesysid)
  from pg_user
  where usename = role_name
$$;

create function plguile3_notify_module_changed()
  returns void
  language c strict
  as 'MODULE_PATHNAME';

create function guile3_delete_module(module_name text[])
  returns void
  language sql
as $$
  select plguile3.guile3_delete_user_module(usesysid, module_name)
  from pg_user
  where usename = current_user;
$$;

create function guile3_delete_public_module(module_name text[])
  returns void
  language sql
as $$
  select plguile3.guile3_delete_user_module(null, module_name);
$$;

create function guile3_delete_user_module(role_id oid, module_name text[])
  returns void
  language plpgsql
as $$
begin

  delete from plguile3.module
  where
    owner_id = role_id
    and name = module_name;

  perform plguile3.plguile3_notify_module_changed();
end
$$;

create function guile3_get_module(module_name text[])
  returns text
  language sql
  stable
as $$
  select plguile3.guile3_get_user_module(usesysid, module_name)
  from pg_user
  where usename = current_user;
$$;

create function guile3_get_public_module(role_id oid, module_name text[])
  returns text
  language sql
  stable
as $$
  select plguile3.guile3_get_user_module(null, module_name);
$$;

create function guile3_get_user_module(role_id oid, module_name text[])
  returns text
  language sql
  stable
as $$
  select source from plguile3.modules
  where
    owner_id = role_id
    and name = module_name;
$$;

create function plguile3_check_prelude(src text)
  returns void
  language c strict
  as 'MODULE_PATHNAME';

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
