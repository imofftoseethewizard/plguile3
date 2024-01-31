# Using Plg3

It doesn't do much yet.

``` sql
create function dummy() returns void as $$ $$ language plg3;
select dummy();
```

This will emit `Hello, world!` into Postgres' log.
