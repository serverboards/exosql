# ExoSQL

[![Build Status](https://travis-ci.org/serverboards/exosql.svg?branch=master)](https://travis-ci.org/serverboards/exosql)

Universal SQL engine for Elixir.

This library implements the SQL engine to perform queries on user provided
databases using a simple interface based on Foreign Data Wrappers from
PostgreSQL.

This allows to use SQL on your own data and virtual tables.

For example it includes a CSV reader and an HTTP client, so that you can
do queries as:

```SQL
SELECT url, status_code
  FROM urls
  INNER JOIN request
  ON urls.url = request.url
```

There is a simple repl to be able to test ExoSQL:

```elixir
iex> ExoSQL.repl()
exosql> SELECT m, SUM(price) FROM generate_series(10) as m LEFT JOIN (SELECT width_bucket(price, 0, 200, 10) AS n, price FROM products) ON n = m GROUP BY m
tmp.m.m | tmp.tmp.col_2
--------------------------
1       | 31
2       | 30
3       | 0
4       | 0
5       | 0
6       | 0
7       | 0
8       | 0
9       | 0
10      | 0
```

## Origin

The origin of the library is as a SQL layer to all the services connected to you
[Serverboards](https://serverboards.io).

Each service can export tables to be accessed via SQL and then can show the data
in the Dashboards, the notebook, or used in the rules.

## Installation

The package can be installed by adding `exosql` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:exosql, "~> 0.2"}
  ]
end
```

## Features

* **SELECT over external databases (CSV, HTTP endpoints... Programmable)**
* `SELECT` over several tables
* `WHERE`
* `INNER JOIN`
* `LEFT JOIN`
* `RIGHT JOIN`
* `GROUP BY`
* `ORDER BY`
* `OFFSET` and `LIMIT`
* `DISTINCT` and `DISTINCT ON`
* `LIKE` and `ILIKE`
* `CASE` `WHEN` `THEN` `ELSE` `END` / `IF` `THEN` `ELIF` `ELSE` `END`.
* `UNION` and `UNION ALL`.
* table and column alias with `AS`
* nested `SELECT`: At `FROM`, `SELECT`, `WHERE`...
* `generate_series` function tables
* Aggregation functions: `COUNT`, `SUM`, `AVG`...
* Builtin functions and operators: * / + - || `or` `and` `in` `not`; `round` `concat`... [See all](#builtins).
* Builtin `format`, `strftime`, `regex` and more string and time formatting functions.
* Basic Reflection over `self.tables`
* JSON support via [json pointer](#jp).
* Array support: `[1, 2, 3, 4]`
* Variables

Check the tests for current available features.

## Variables

Variables can be passed as a dictionary at `__vars__` inside the context, and
referenced as `$name` at the SQL expression. This may change in the future
to streamline it more with standard SQL (no need for `$`).

## INNER JOIN

Because some columns may need to be autogenerated depending on the query,
if you want to access those columns you may need to use INNER JOINS. This
way the planner asks for those specific column values.

For example:

```SQL
SELECT * FROM request
```

does not know to which URL you want to access, but:

```SQL
SELECT * FROM request WHERE url = 'http://serverboards.io'
```

knows the URL and can get the data.

Then same way, on INNER JOINS this can be used to access to auto generated data:

```SQL
SELECT url, status_code
  FROM urls
  INNER JOIN request
  ON urls.url = request.url
```

## Builtins

### String operations

#### `format(format_str, args...)`

Formats a String using C sprintf-like parameters. Known placeholders are:

* `%s` -- String
* `%d` -- Number
* `%f` -- Float
* `%.2f` -- Float with precision
* `%k` -- Metric System suffix: k, M, G, T. Try to show most relevant information.
* `%.2k` -- Metric System suffix with precision
* `%,2k` -- Metric System, using `.` to separate thousands and `,` for decimals. Follow Spanish numbering system.

#### `lower(str)`

Lower case a full string

#### `join(str, sep=",")`

Joins all elements from a list into a string, using the given separator.

```sql
join([1,2,3,4], "/")
"1/2/3/4"
```

#### `split(str, sep=[", ", ",", " "])`

Splits a string into a list using the given separator.

```sql
split("1, 2,3 4")
["1", "2", "3", "4"]
```


#### `substr(str, start, end=10000)` / `substr(str, end)`

Extracts a substring from the first argument.

Can use negative indexes to start to count from the end.

```sql
substr('#test#', 1, -1)
"test"
```

#### `to_string(arg)`

Converts the given argument into a string.

```sql
to_string(1)
"1"
```

#### `upper(str)`

Upper cases a full string

### Date time functions

#### `now()`

Returns current datetime.

#### `strftime(datetime, format_str)`

Convert a datetime to a string. Can be used also to extract some parts of a
date, as the day, year and so on.

Normally `strftime` can be used directly with a string or an integer as it does
the conversion to datetime implicitly.

It is based on [Timex](https://github.com/bitwalker/timex)
[formatting](https://hexdocs.pm/timex/Timex.Format.DateTime.Formatters.Strftime.html).

Most common markers:

* `%Y` -- Year four digits
* `%y` -- Year two digits
* `%m` -- Month number
* `%d` -- Day of month
* `%H` -- Hour
* `%M` -- Minute
* `%S` -- Second
* `%V` -- ISO Week (01-53)
* `%s` -- Unix time
* `%F` -- ISO year: yyyy-mm-dd
* `%H` -- Time: HH:MM:SS

#### `to_datetime(str | int, mod \\ nil)`

Converts the given string or integer to a date.

The string must be in ISO8859 sub string format:

* `YYYY-mm-dd`
* `YYYY-mm-ddTHH:MM`
* `YYYY-mm-dd HH:MM`
* `YYYY-mm-ddTHH:MM:SS`
* `YYYY-mm-dd HH:MM:SS`
* or an Unix epoch integer.

This is called implicitly on `strftime` calls, and normally is not needed.

If `mod` is given it is a duration modifier as defined by
[ISO8601](https://en.wikipedia.org/wiki/ISO_8601#Durations), with the following
changes:

* Initial `P` is optional
* Can start with a sign to donte subtraction: `-`

For example:

* Subtract one month `to_datetime(NOW(), "-1M")`
* Add 30 minutes: `to_datetime(NOW(), "T30M")`
* One year and a half and 6 minutes ago: `to_datetime(NOW(), "-1Y1MT6M")`

### Boolean functions

#### `bool(arg)`

Converts to boolean. Equivalent to `NOT NOT arg`

### Aggregation functions

#### `avg(expr)`

Calculates the average of the calculated expression on the group rows.
Equivalent to `sum(expr) / count(expr)`.

If no rows, returns `NULL`.

#### `count(*)`

Counts the number of rows of the aggregates expression.

#### `max(expr)`

Returns the maximum value of the given expression for the group.

#### `min`

Returns the minimum value of the given expression for the group.

#### `sum(expr)`

For each of the grouped rows, calculates the expression and returns the sum. If
there are no rows, returns 0.

### Miscellaneous functions

#### `generate_series(end)` / `generate_series(start, end, step=0)`

This function generates a virtual table with one column and on each row a value of the series.

Can be reverse with a larger start than end and negative step.

It can be used to for example fill all holes in a temporal serie:

```sql
SELECT month, SUM(value)
  FROM generate_series(12) AS month
LEFT JOIN purchases
  ON strftime(purchases.datetime, "%m") == month
GROUP BY month
```

This will return 0 for empty months on the purchases table.

#### `jp(json, selector)`

Does [JSON Pointer](https://tools.ietf.org/html/rfc6901) selection:

* Use / to navigate through the object keys or array indexes.
* If no data found, return `NULL`

#### `RANDOM()`

Return a random float between 0 and 1.

#### `RANDINT(max)` / `RANDINT(min, max)`

Returns a random integer between `min` and `max`.

#### `regex(str, regex, query \\ nil)`

Performs a regex search on the string.

It uses elixir regex, so use it as reference.

Can use groups and named groups for matching and it will return a list of a map
with the result. It can optionally use directly JSON pointer queries. See
`jp` function.

If matches the result will be "trueish" (or "falsy" if doesn't) so can be used
as a boolean.

#### `round(number, precision=0)`

Returns the number rounded to the given precission. May be convert to integer if precission is 0.

#### `urlparse(string, sel="")`

Parses an URL and returns a JSON.

If selector is given it does the equivalent of callong `jp` with that selector.

#### `width_bucket(n, start, end, buckets)`

Given a `n` value it is assigned a bucket between 0 and `buckets`, that correspond to the full width between `start` and `end`.

If a value is out of bounds it is set either to 0 or to `buckets - 1`.

This helper eases the generation of histograms.

For example an histogram of prices:

```sql
SELECT n, SUM(price)
  FROM (SELECT width_bucket(price, 0, 200, 10) AS n, price
          FROM products)
  GROUP BY n
```

or more complete, with filling zeroes:

```sql
SELECT m, SUM(price)
  FROM generate_series(10) AS m
  LEFT JOIN (
        SELECT width_bucket(price, 0, 200, 10) AS n, price
          FROM products
    )
    ON n = m
 GROUP BY m
```

## Included extractors

ExoSQL has been developed with the idea of connecting to Serverboards services,
and as such it does not provide more than some test extractors:

* CSV files
* HTTP requests

Creating new ones is a very straightforward process. The HTTP example can be
followed.

This is not intended a full database system, but to be embedded into other
Elixir programs and accessible from them by end users. As such it does contain
only some basic extractors that are needed for proper testing.

## Using ExoSQL

There is no formal documentation yet, but you can check the `esql_test.exs` file
to get an idea of how to use ExoSQL.

Example:

```elixir
context = %{
  "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  "B" => {ExoSQL.HTTP, []}.
  "__vars__" => %{ "start" => "2018-01-01" }
}
{:ok, result} = ExoSQL.query("
  SELECT urls.url, request.status_code
    FROM urls
   INNER JOIN request
      ON urls.url = request.url
", context)
```

```elixir
%ExoSQL.Result{
  columns: [{"A", "urls", "url"}, {"B", "request", "status_code"}],
  rows: [
    ["https://serverboards.io/e404", 404],
    ["http://www.facebook.com", 302],
    ["https://serverboards.io", 200],
    ["http://www.serverboards.io", 301],
    ["http://www.google.com", 302]
  ]}
```

A Simple extractor can be:
```elixir
defmodule MyExtractor do
  def schema(_config), do: {:ok, ["week"]}
  def schema(_config, "week"), do: {:ok, %{ columns: ["id", "nr", "name", "weekend"] }}
  def execute(_config, "week", _quals, _columns) do
    {:ok, %{
      columns: ["id", "nr", "name", "weekend"],
      rows: [
        [1, 0, "Sunday", true],
        [2, 1, "Monday", false],
        [3, 2, "Tuesday", false],
        [4, 3, "Wednesday", false],
        [5, 4, "Thursday", false],
        [6, 5, "Friday", false],
        [7, 6, "Saturday", true],
      ]
    }}
  end
end
```

And then a simple query:

```elixir
{:ok, res} = ExoSQL.query("SELECT * FROM week WHERE weekend", %{ "A" => {MyExtractor, []}})               
ExoSQL.format_result(res)
```

|A.week.id | A.week.nr | A.week.name | A.week.weekend|
|----------|-----------|-------------|---------------|
|1         | 0         | Sunday      | true          |
|7         | 6         | Saturday    | true          |

## Related libraries

There are other implementations of this very same idea:

* [Postgres Foreign Data Wrappers] (FDW). Integrates any external
  source with a postgres database. Can be programmed in C and Python. Postgres
  FDW gave me the initial inspiration for ExoSQL.
* [Apache Foundation's Drill]. Integrates NoSQL database and SQL databases.
* [Apache Foundation's Calcite]. Java based library, very similar to ExoSQL,
  with many many adapters. Many projects use parts of calcite, for example
  Drill uses the SQL parser.

If you know any other, please ping me and I will add it here.

I develop ExoSQL as I needed an elixir solution for an existing project, and
to learn how to create an SQL engine. ExoSQL is currently used in
[Serverboards] KPI.

[Postgres Foreign Data Wrappers]: https://wiki.postgresql.org/wiki/Foreign_data_wrappers
[Apache Foundation's Drill]: https://drill.apache.org
[Apache Foundation's Calcite]: https://calcite.apache.org
[Serverboards]: https://serverboards.io

## Known BUGS

* When doing `ORDER BY [column id], [column name]`, it reverses the order. To
  avoid use one or the other, dont mix order by column name and result column
  position.

  This is because the planner does the ordering on column name first, then
  the select which limits the columns and reorder them and then the ordering
  by column position.

* Can not use variables inside aggregation functions.
