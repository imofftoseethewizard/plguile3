-- -*- mode: sql-mode; indent-tabs-mode: t; -*-

create or replace function wait_for_test_state(target_state text) returns void as $$
declare i int;
begin
  for i in 1..1000 loop
    if exists (select 1 from test_state where state = target_state) then return; end if;
    perform pg_sleep(0.001);
  end loop;
  raise exception 'timed out while waiting for test state "%"', target_state;
end $$ language plpgsql;

create table if not exists test_state (state text primary key, at timestamptz);

	select pg_sleep(0.01);
	insert into test_state values ('b-done', now());

		select pg_sleep(0.01);
		select wait_for_test_state('b-done');
		insert into test_state values ('done', now());

select wait_for_test_state('done');
drop table test_state;
drop function wait_for_test_state;
