-- -*- mode: sql-mode; indent-tabs-mode: t; -*-

create or replace function wait_for_test_event(target_event text) returns void as $$
declare i int;
begin
  for i in 1..1000 loop
    if exists (select 1 from test_event where event = target_event) then return; end if;
    perform pg_sleep(0.001);
  end loop;
  raise exception 'timed out while waiting for test event "%"', target_event;
end $$ language plpgsql;

create table if not exists test_event (event text primary key, at timestamptz);
truncate test_event;

create extension if not exists plguile3;

create or replace function set_test_event(event text) returns void as $$
begin
  insert into test_event values (event, now());
end $$ language plpgsql;

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'alice') then
    create role alice;
  end if;

  if not exists (select 1 from pg_roles where rolname = 'bob') then
    create role bob;
    grant bob to alice;
  end if;

  if not exists (select 1 from pg_roles where rolname = 'carol') then
    create role carol;
    grant carol to alice;
  end if;

  grant all on all tables in schema public to alice, bob, carol;
  grant all on all sequences in schema public to alice, bob, carol;
  alter default privileges in schema public grant all on tables to alice, bob, carol;
  alter default privileges in schema public grant all on sequences to alice, bob, carol;
  grant usage on language guile3 to alice, bob, carol;
  grant execute on all routines in schema public to alice;

  grant execute
    on routine
      g3_get_call_allocation_limit,
      g3_get_call_time_limit,
      g3_get_default_call_allocation_limit,
      g3_get_default_call_time_limit,
      g3_get_default_preamble,
      g3_get_preamble,
      g3_get_session_user_call_allocation_limit,
      g3_get_session_user_call_time_limit,
      g3_get_session_user_preamble,
      g3_set_session_user_preamble
    to bob, carol;

end
$$;

set session authorization alice;

select plan(24);

	set session authorization bob;

	select plan(21);

		set session authorization carol;

		select plan(19);

select g3_set_default_preamble('(define the-owner "anyone")');

select g3_remove_role_preamble('alice');
select g3_remove_role_preamble('bob');
select g3_remove_role_preamble('carol');

select is(g3_get_default_preamble(), '(define the-owner "anyone")');

create or replace function who_owns_this() returns text as 'the-owner' language guile3;

create or replace function sleep(t float4) returns void as $$
  (execute "select pg_sleep($1)" (list t))
$$ language guile3;

create or replace function alloc(t bigint) returns void as $$
  (make-vector (/ t 8) #f)
$$ language guile3;

select set_test_event('ready');

select is(who_owns_this(), 'anyone', 'alice no preamble');

	select pg_sleep(0.01);

	select wait_for_test_event('ready');

	select is(who_owns_this(), 'anyone', 'bob no preamble');

	select set_test_event('b.1 done');

		select pg_sleep(0.01);

		select wait_for_test_event('ready');

		select is(who_owns_this(), 'anyone', 'carol no preamble');

		select set_test_event('c.1 done');

select wait_for_test_event('b.1 done');
select wait_for_test_event('c.1 done');

select g3_set_default_preamble('(define the-owner "someone")');

select is(who_owns_this(), 'someone', 'alice no preamble 2');

select set_test_event('someone-ready');

	select wait_for_test_event('someone-ready');

	select is(who_owns_this(), 'someone', 'bob no preamble 2');

	select set_test_event('b.2 done');

		select wait_for_test_event('someone-ready');

		select is(who_owns_this(), 'someone', 'carol no preamble 2');

		select set_test_event('c.2 done');

select wait_for_test_event('b.2 done');
select wait_for_test_event('c.2 done');

select g3_set_role_preamble('bob', '(define the-owner "bob")');

select is(g3_get_default_preamble(), '(define the-owner "someone")');
select is(g3_get_role_preamble('bob'), '(define the-owner "bob")');

select is(who_owns_this(), 'someone', 'alice no preamble 3');

select set_test_event('bob-ready');

	select wait_for_test_event('bob-ready');

	select is(g3_get_preamble(), '(define the-owner "bob")');

	select is(who_owns_this(), 'someone', 'bob with preamble');

	select set_test_event('b.3 done');

		select wait_for_test_event('bob-ready');

		select is(who_owns_this(), 'someone', 'carol no preamble 3');

		select set_test_event('c.3 done');


select wait_for_test_event('b.3 done');
select wait_for_test_event('c.3 done');

alter function who_owns_this owner to bob;

select is(who_owns_this(), 'bob', 'alice no preamble 4');

select set_test_event('bob-the-owner');

	select wait_for_test_event('bob-the-owner');

	select is(who_owns_this(), 'bob', 'bob with preamble 2');

	select set_test_event('b.4 done');

		select wait_for_test_event('bob-the-owner');

		select is(who_owns_this(), 'bob', 'carol no preamble 4');

		select set_test_event('c.4 done');


select wait_for_test_event('b.4 done');
select wait_for_test_event('c.4 done');

select g3_set_role_preamble('bob', '(define the-owner "robert")');

select is(who_owns_this(), 'robert', 'alice no preamble 4');

select set_test_event('robert-the-owner');

	select wait_for_test_event('robert-the-owner');

	select is(who_owns_this(), 'robert', 'bob with preamble 3');

	select set_test_event('b.5 done');

		select wait_for_test_event('robert-the-owner');

		select is(who_owns_this(), 'robert', 'carol no preamble 4');

		select set_test_event('c.5 done');

	select throws_ok('g3_set_role_preamble(''carol'', ''bogus'')');

select g3_set_default_call_time_limit(1);
select g3_set_role_call_time_limit('alice', NULL);
select g3_set_role_call_time_limit('bob', NULL);
select g3_set_role_call_time_limit('carol', NULL);

select is(g3_get_default_call_time_limit(), 1.0::float4, 'alice: get default call time limit');
select is(g3_get_call_time_limit(), 1.0::float4, 'alice: get call time limit');
select is(g3_get_session_user_call_time_limit(), NULL, 'alice: get session user call time limit');

select is(g3_get_role_call_time_limit('alice'), NULL, 'alice: get role call time limit 1');
select is(g3_get_role_call_time_limit('bob'), NULL, 'alice: get role call time limit 2');
select is(g3_get_role_call_time_limit('carol'), NULL, 'alice: get role call time limit 3');

select set_test_event('time limit 1');

	select wait_for_test_event('time limit 1');

	select is(g3_get_default_call_time_limit(), 1.0::float4, 'bob: get default call time limit');
	select is(g3_get_call_time_limit(), 1.0::float4, 'bob: get call time limit');
	select is(g3_get_session_user_call_time_limit(), NULL, 'bob: get session user call time limit');

	select set_test_event('bob time limit 1 done');

		select wait_for_test_event('time limit 1');

		select is(g3_get_default_call_time_limit(), 1.0::float4, 'carol: get default call time limit');
		select is(g3_get_call_time_limit(), 1.0::float4, 'carol: get call time limit');
		select is(g3_get_session_user_call_time_limit(), NULL, 'carol: get session user call time limit');

		select set_test_event('carol time limit 1 done');


select wait_for_test_event('bob time limit 1 done');
select wait_for_test_event('carol time limit 1 done');

select g3_set_role_call_time_limit('alice', 10);
select g3_set_role_call_time_limit('bob', 2);
select g3_set_role_call_time_limit('carol', 0.01);

select is(g3_get_role_call_time_limit('alice'), 10::float4, 'alice: get role call time limit @10');
select is(g3_get_session_user_call_time_limit(), 10::float4, 'alice: get session user call time limit @10');

select set_test_event('time limit 2');

	select wait_for_test_event('time limit 2');

	select is(g3_get_default_call_time_limit(), 1.0::float4, 'bob: get default call time limit 2');
	select is(g3_get_call_time_limit(), 2.0::float4, 'bob: get call time limit 2');
	select is(g3_get_session_user_call_time_limit(), 2.0::float4, 'bob: get session user call time limit 2');

	select lives_ok('select sleep(0.05)');

		select wait_for_test_event('time limit 2');

		select is(g3_get_default_call_time_limit(), 1.0::float4, 'carol: get default call time limit 2');
		select is(g3_get_call_time_limit(), 0.01::float4, 'carol: get call time limit 2');
		select is(g3_get_session_user_call_time_limit(), 0.01::float4, 'carol: get session user call time limit 2');

		select throws_ok('select sleep(0.05)');

select g3_set_default_call_allocation_limit(1000000);
select g3_set_role_call_allocation_limit('alice', NULL);
select g3_set_role_call_allocation_limit('bob', NULL);
select g3_set_role_call_allocation_limit('carol', NULL);

select is(g3_get_default_call_allocation_limit(), 1000000::bigint, 'alice: get default call allocation limit');
select is(g3_get_call_allocation_limit(), 1000000::bigint, 'alice: get call allocation limit');
select is(g3_get_session_user_call_allocation_limit(), NULL, 'alice: get session user call allocation limit');

select is(g3_get_role_call_allocation_limit('alice'), NULL, 'alice: get role call allocation limit 1');
select is(g3_get_role_call_allocation_limit('bob'), NULL, 'alice: get role call allocation limit 2');
select is(g3_get_role_call_allocation_limit('carol'), NULL, 'alice: get role call allocation limit 3');

select set_test_event('allocation limit 1');

	select wait_for_test_event('allocation limit 1');

	select is(g3_get_default_call_allocation_limit(), 1000000::bigint, 'bob: get default call allocation limit');
	select is(g3_get_call_allocation_limit(), 1000000::bigint, 'bob: get call allocation limit');
	select is(g3_get_session_user_call_allocation_limit(), NULL, 'bob: get session user call allocation limit');

	select set_test_event('bob allocation limit 1 done');

		select wait_for_test_event('allocation limit 1');

		select is(g3_get_default_call_allocation_limit(), 1000000::bigint, 'carol: get default call allocation limit');
		select is(g3_get_call_allocation_limit(), 1000000::bigint, 'carol: get call allocation limit');
		select is(g3_get_session_user_call_allocation_limit(), NULL, 'carol: get session user call allocation limit');

		select set_test_event('carol allocation limit 1 done');


select wait_for_test_event('bob allocation limit 1 done');
select wait_for_test_event('carol allocation limit 1 done');

select g3_set_role_call_allocation_limit('alice', 10000000);
select g3_set_role_call_allocation_limit('bob', 2000000);
select g3_set_role_call_allocation_limit('carol', 10000);

select is(g3_get_role_call_allocation_limit('alice'), 10000000::bigint, 'alice: get role call allocation limit @10');
select is(g3_get_session_user_call_allocation_limit(), 10000000::bigint, 'alice: get session user call allocation limit @10');

select set_test_event('allocation limit 2');

	select wait_for_test_event('allocation limit 2');

	select is(g3_get_default_call_allocation_limit(), 1000000::bigint, 'bob: get default call allocation limit 2');
	select is(g3_get_call_allocation_limit(), 2000000::bigint, 'bob: get call allocation limit 2');
	select is(g3_get_session_user_call_allocation_limit(), 2000000::bigint, 'bob: get session user call allocation limit 2');

	select lives_ok('select sleep(0.05)');

		select wait_for_test_event('allocation limit 2');

		select is(g3_get_default_call_allocation_limit(), 1000000::bigint, 'carol: get default call allocation limit 2');
		select is(g3_get_call_allocation_limit(), 10000::bigint, 'carol: get call allocation limit 2');
		select is(g3_get_session_user_call_allocation_limit(), 10000::bigint, 'carol: get session user call allocation limit 2');

		select throws_ok('select sleep(0.05)');

-- Coda
	select set_test_event('b all done');

		select set_test_event('c all done');

select wait_for_test_event('b all done');
select wait_for_test_event('c all done');

set session authorization postgres;

drop function who_owns_this;
drop function sleep;
drop function alloc;
drop table test_event;
drop function set_test_event;
drop function wait_for_test_event;

drop extension plguile3 cascade;
