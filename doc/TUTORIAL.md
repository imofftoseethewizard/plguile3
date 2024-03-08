# Using Plguile3

It doesn't do much yet.

``` sql
create function dummy() returns void as $$ $$ language guile3;
select dummy();
```

This will emit `Hello, world!` into Postgres' log.
