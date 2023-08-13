# Using Scruple

It doesn't do much yet.

``` sql
create function dummy() returns void as $$ $$ language scruple;
select dummy();
```

This will emit `Hello, world!` into Postgres' log.
