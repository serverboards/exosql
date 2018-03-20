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
* table and column alias with `AS`
* nested `SELECT` at `FROM`
* `generate_series` function tables
* Aggregation functions: COUNT, SUM, AVG
* Builtin functions and operators: * / + - || or and; round concat...
* Builtin `format`, `strftime` and more string and time formatting functions.
* Basic Reflection over `self.tables`
* Variables

Check the tests for current features available.

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

## Included extractors

ExoSQL has been developed with the idea of connecting to Serverboards services,
and as such it does not provide more than some test extractors:

* CSV files
* HTTP requests

Creating new ones is a very straightforward process. The HTTP example can be
followed.

## Using ExoSQL

There is no formal documentation yet, but you can check the `esql_test.exs` file
to get an idea of how to use ExoSQL.

Example:

```elixir
context = %{
  "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  "B" => {ExoSQL.HTTP, []}
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

There are other implemetnations of this very same idea:

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

* There is no operator priority, so all your expressions should be surrounded
  by parenthesis when there is ambiguity.

* Can not use variables inside aggregation functions.
