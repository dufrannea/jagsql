With inputSource pipe from stdin

```text
INFO: something nice happened at 00:00
ERROR: jobId 4242 failed
```

SELECT /ERROR/ implicitly

- Should we auto filter if a clause does not match ? no
- accept where clause with regex filtering

# General goals:

- easily operate on not well formatted text (group features such a cut, extract regex group from regex syntax)
- easily provide means of integrating things together by calling shell functions from a result

# Use cases:

- find all lines that match /a/ and /b/
- convert file to csv using operators

```sql
SELECT
    // have a flag to fail if no match
    // have a flag to have matching only
    // have a flag to coalesce
    f/(P<criticity>[A-Z]*):\s+(.*?)/
```

Regex is one way, we can probably have other syntaxes

```sql
SELECT from CSV(stdin, false (for headers))
# or
SELECT -- FROM stdin
```

SELECT /didier:(.*)/ ==>

difficulty because regex is at the same time a where and a projection

select /didier/

TODO:

- ~~there should be only one reading from stdin or they should be put in common, currently there is only one possible~~
  ~~stream reading (the others will never return anything)~~
- ~~fix additional line at end of stream~~
- ~~implement regex expansion~~
- ~~implement DUAL~~
- ~~implement FILE (maybe file literal)~~
- ~~implement regex evaluationA~~
- ~~Link validation errors to position in code (parse with positions)A~~
- Select *
- unify sources (stdin, dual etc. should really be relevant in the running phase)
- implement dirs
- Have error codes
- Format
- Spread operator
- "Split" operator like cut -d offering a struct
- INSERT INTO with formats
- DESC files to auto infer format
- Whitelist shell commands
- Fast eval

THOUGHTS:

- recursive CTE support ?
- windows (tumble, etc.)

```sql
DESC file'/some/file'

can understand file format and make it usable as a table

SHOW TABLES
-- populated with many tables 
CREATE TABLE processes (
    pid NUMBER
    owner STRING
    open_files ARRAY[STRING],
    open_ports ARRAY[NUMBER]
)
```

- Can we pipe results from one call to another using Apache arrow ?
- Use amonite REPL

# On regex expansion
```sql
SELECT
 *
-- this gets expanded as reading from stdin ?
FROM /(?P<lol>didier)/ 

UDTF(//, FILE(""))

SELECT
 -- apply regex with one group
 /qsmldkjfsldf/(lol)
 
```
- One could interpret regexes with single column table


-- implicit syntax, regex will expand on the first column
-- unless you specify it
-- So regex literal are actually functions

```sql
SELECT
  /(lol)/,
  /(didier)/(col_0),
  -- this is the "expansion"
  -- it will create columns in the projection
  /(P<lol>.*?):(?P<didier>some_regex)/ AS lol
  /(P<lol>.*?):(?P<didier>some_regex)/ AS didier
FROM STDIN
```

in Hive there is _INLINE_ that can explode a struct

Why not investigate the ...(object) construct from javascript and "expand" structs this way ?

# On the build system, environment and stuff

## Have local views in a directory

```sql
-- file ls.sql

select * from run('ls')

```

can be used from jagsql with

> select * from ls

## Storing data ?

In a directory somewhere, store the results of queries.

Idea being, if it changed rebuild it

> jagsql build

Updates all the data that needs to be updated

We need to include cacheable / non cacheable tasks then.

### how to support defining all the iterations

daily, $day // support arbitrary expressions, probably using integration with an existing language like python

select * from ftp('/qsdlfkj/qsldkfjlj/')

# On table transformations

json('/path/remotefile') => { file : '/local/file' }

ftp('lol') |> json
