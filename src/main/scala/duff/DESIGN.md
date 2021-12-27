With inputSource pipe from stdin

```text
INFO: something nice happened at 00:00
ERROR: jobId 4242 failed
```

SELECT /ERROR/
implicitly
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

difficulty because regex is at the same time
a where and a projection

select /didier/


TODO:
- there should be only one reading from stdin or they should be put in common, currently there is only one possible stream reading (the others will never return anything)
- fix additional line at end of stream
- implement regex evaluation and expansion
- unify sources (stdin, dual etc. should really be relevant in the running phase)
- implement DUAL
- implement FILE (maybe file literal)
- implement dirs
- Have error codes
- Link validation errors to position in code (parse with positions)
- Format


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