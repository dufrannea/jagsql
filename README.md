# Description

A command-line, local SQL engine.

Inspiration:
- Powershell that can pipe structured objects
- The amazing [LogParser](https://en.wikipedia.org/wiki/Logparser) that allows querying log files with sql

If you are looking something more serious, look into [nushell](https://www.nushell.sh/) (with relies on [polars](https://pola.rs/)) and [duckdb](https://duckdb.org/), they are amazing.

This is only a coding experiment to pursue several goals:
- Play with Scala 3
- Play with Shapeless 3
- Play with [Recursion schemes](https://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.41.125)
- Build a rudimentary SQL engine, from scratch, because why not

# Example

It is intended to run queries like:

```sql
SELECT
    regex_struct(
        /(?<user>[^ ]+)\s+(?<pid>[^ ]+)\s+(?<command>[^ ]+)/,
        col_0) as user
FROM RUN('ps axo user:20,pid,comm') as ps
```

and

```sql
SELECT
    regex_struct(
        /(?:[^ ]+\s+){2}(?<user>[^ ]+)\s+(?:[^ ]+\s+)(?<size>[^ ]+)\s+(?:[^ ]+\s+){3}(?<filename>[^ ]+)/,
        col_0) as user
FROM RUN('ls -la') as ls
```

but also really useful queries like

```sql
SELECT
  1 AS one
  , 1 * 42 AS three
  ,a.b + 1 AS four
  ,a.d -1 AS five
  ,a.d -1 + 1 AS six
  ,max(a.c - 1) + 1 AS seven
FROM
  (SELECT 1 AS b, 1 AS d, 1 AS c) AS a
GROUP BY a.b, a.d - 1
```

# Structure

- A minimal [sql parser](core/src/main/scala/duff/jagsql/cst/cst.scala) with very precise error reporting
- A minimal [sql analyzer with types](core/src/main/scala/duff/jagsql/ast) with very precise error reporting
- Eval based on [catamorphism](core/src/main/scala/duff/jagsql/eval/eval.scala)
- A minimal [runner](core/src/main/scala/duff/jagsql/runner/runner.scala) and [planner](core/src/main/scala/duff/jagsql/planner/planner.scala)
- Token-level error reporting

```sql
ERROR: Expressions in GROUP BY clause should not contain aggregation functions

SELECT 1 FROM (SELECT 1 AS bar, 2 AS baz) AS foo GROUP BY foo.bar, array(max(foo.bar))
                                                                         ^^^^^^^^^^^^
```

# Interesting
- [Replayable Fs2 topic allowing late subscriptions](core/src/main/scala/duff/jagsql/std/PersistentTopic.scala)
- A queue allowing to [dequeue with predicates](core/src/main/scala/duff/jagsql/std/FilteredQueue.scala)
- Populate (and typecheck) objects based on [regex groups naming](core/src/test/scala/duff/jagsql/std/RegexWrapperSpec.scala)

# Esoteric
- [FunctorK implementation with scala3 derivation](core/src/main/scala/duff/jagsql/std/FunctorK.scala)
