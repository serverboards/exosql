require Logger

defmodule ExoSQLTest do
  use ExUnit.Case
  doctest ExoSQL
  doctest ExoSQL.Expr
  @moduletag :capture_log

  test "Simple parse SQL" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = ExoSQL.parse("SELECT A.products.name, A.products.price FROM A.products", context)
    {:ok, plan} = ExoSQL.Planner.plan(query)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)
  end

  test "No from" do
    context = %{}

    {:ok, query} = ExoSQL.parse("SELECT 1 + '1'", context)
    {:ok, plan} = ExoSQL.Planner.plan(query, context)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)
    assert result.rows == [[2]]

    {:ok, query} = ExoSQL.parse("SELECT 'test'", context)
    {:ok, plan} = ExoSQL.Planner.plan(query, context)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)
    assert result.rows == [["test"]]

    {:ok, query} = ExoSQL.parse("SELECT upper(\"test\")", context)
    {:ok, plan} = ExoSQL.Planner.plan(query, context)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)
    assert result.rows == [["TEST"]]

  end

  test "Simple WHERE" do
    context = %{
      "A" => {ExoSQL.Node, []}
    }
    {:ok, result} = ExoSQL.query("SELECT A.passwd.uid, A.passwd.user, A.passwd.home FROM A.passwd WHERE A.passwd.uid >= 1001", context)
    Logger.debug("\n#{ExoSQL.format_result(result)}")
  end

  test "Select * from" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = ExoSQL.parse("SELECT * FROM A.users", context)
    Logger.debug("Query is #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.Planner.plan(query, context)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)

    assert result == %ExoSQL.Result{
      columns: [
        {"A", "users", "id"},
        {"A", "users", "name"},
        {"A", "users", "email"}
        ],
      rows: [
        ["1", "David", "dmono@example.org"],
        ["2", "Javier", "javier@example.org"],
        ["3", "Patricio", "patricio@example.org"]
      ]}
  end

  test "Multiples tables" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    ExoSQL.explain("""
      SELECT A.products.name, A.users.name
        FROM A.products, A.purchases, A.users
       WHERE (A.products.id = A.purchases.product_id)
         AND (A.purchases.user_id = A.users.id)
      """, context)
    {:ok, result} = ExoSQL.query("""
      SELECT A.products.name, A.users.name
        FROM A.products, A.purchases, A.users
       WHERE (A.products.id = A.purchases.product_id)
         AND (A.purchases.user_id = A.users.id)
      """, context)
    Logger.debug(ExoSQL.format_result result)
  end

  test "Do some expression at select" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    {:ok, result} = ExoSQL.query("SELECT A.products.name, (A.products.price || ' €'), ROUND( A.products.price * 0.21, 2 ) FROM A.products",  context)

    Logger.debug(ExoSQL.format_result result)
  end

  test "Very simple aggregate" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    {:ok, parsed} = ExoSQL.parse("""
      SELECT user_id, COUNT(A.purchases.user_id)
        FROM A.purchases
       GROUP BY user_id
    """, context)
    Logger.debug("Parsed: #{inspect parsed, pretty: true}")
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)

    Logger.debug(ExoSQL.format_result result)
    assert result.rows == [
      ["1",3],
      ["2",2],
      ["3",1]
    ]
  end

  test "Aggregates" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, parsed} = ExoSQL.parse("""
      SELECT A.products.name, COUNT(*), AVG(A.products.price * 1.21)
        FROM A.products, A.purchases
       WHERE A.products.id = A.purchases.product_id
       GROUP BY A.products.name
    """, context)
    Logger.debug("Parsed: #{inspect parsed, pretty: true}")
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)

    Logger.debug(ExoSQL.format_result result)
  end

  test "Aggregates no group" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, parse} = ExoSQL.parse("""
      SELECT COUNT(*), AVG(A.products.price)
        FROM A.products
    """, context)
    Logger.debug("Parsed: #{inspect parse, pretty: true}")
    {:ok, plan} = ExoSQL.Planner.plan(parse)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)


    Logger.debug(ExoSQL.format_result result)
    assert result == %ExoSQL.Result{columns: ["?NONAME", "?NONAME"], rows: [[4, 16.0]]}
  end


  test "Get schema data" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, tables} = ExoSQL.schema("A", context)
    Logger.debug("Schema data: #{inspect tables}")

    for t <- tables do
      {:ok, table} = ExoSQL.schema("A", t, context)
      Logger.debug("Table data A.#{inspect table}")
    end
  end

  test "Resolve table and column from partial name" do
    import ExoSQL.Parser, only: [resolve_table: 2, resolve_column: 3]

    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    assert {"A", "products"} ==
      resolve_table({:table, {nil, "products"}}, context)

    assert {"A", "products"} ==
      resolve_table({:table, {"A", "products"}}, context)


    try do
      resolve_table({:table, {nil, "prioducts"}}, context)
    catch
      {:not_found, "prioducts"} -> :ok
      other -> flunk(inspect other)
    end

    try do
      resolve_table({:table, {nil, "products"}}, %{
        "A" => {ExoSQL.Csv, path: "test/data/csv"},
        "B" => {ExoSQL.Csv, path: "test/data/csv"},
        })
    catch
      {:ambiguous_table_name, "products"} -> :ok
      other -> flunk(inspect other)
    end


    assert {:column, {"A", "products", "price"}} ==
      resolve_column({:column, {nil, nil, "price"}},
        [
          {"A", "users"},
          {"A", "purchases"},
          {"A", "products"}],
        context)

    assert {:column, {"A", "products", "price"}} ==
      resolve_column({:column, {nil, "products", "price"}},
        [
          {"A", "users"},
          {"A", "purchases"},
          {"A", "products"}],
        context)
    assert {:column, {"A", "products", "price"}} ==
      resolve_column({:column, {"A", "products", "price"}},
        [
          {"A", "users"},
          {"A", "purchases"},
          {"A", "products"}],
        context)

    assert {:column, {"A", "products", "name"}} ==
      resolve_column({:column, {nil, "products", "name"}},
        [
          {"A", "products"},
          {"A", "purchases"},
          {"A", "users"},
        ],
        context)


    try do
      resolve_column({:column, {nil, nil, "prix"}},
        [
          {"A", "users"},
          {"A", "purchases"},
          {"A", "products"}],
        context)
    catch
      {:not_found, "prix"} -> :ok
      other -> flunk(inspect other)
    end

    try do
      resolve_column({:column, {nil, nil, "id"}},
        [
          {"A", "users"},
          {"A", "purchases"},
          {"A", "products"}],
        context)
    catch
      {:ambiguous_column_name, "id"} -> :ok
      other -> flunk(other)
    end
  end

  test "Partially defined data" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, result} = ExoSQL.query("
      SELECT products.name, users.name
        FROM products, purchases, users
        WHERE (products.id = product_id) and (user_id = users.id)
        ", context)
    Logger.debug(ExoSQL.format_result result)
  end

  test "Inner join" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = ExoSQL.parse("
      SELECT purchases.id, products.name, users.name
        FROM purchases
       INNER JOIN products
          ON purchases.product_id = products.id
       INNER JOIN users
          ON users.id = purchases.user_id
    ", context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result: #{inspect result, pretty: true}")

    assert result == %ExoSQL.Result{
      columns: [
        {"A", "purchases", "id"}, {"A", "products", "name"},
        {"A", "users", "name"}],
      rows: [
        ["1", "sugus", "David"], ["2", "lollipop", "David"],
        ["3", "donut", "David"], ["4", "lollipop", "Javier"],
        ["5", "water", "Javier"], ["6", "sugus", "Patricio"]
      ]}
  end

  test "Advanced inner join, ask by qual" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
      "B" => {ExoSQL.HTTP, []}
    }
    {:ok, query} = ExoSQL.parse("
      SELECT urls.url, request.status_code
        FROM urls
       INNER JOIN request
          ON urls.url = request.url
    ", context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result: #{inspect result, pretty: true}")

    assert result == %ExoSQL.Result{
      columns: [{"A", "urls", "url"}, {"B", "request", "status_code"}],
      rows: [
        ["https://serverboards.io/e404", 404],
        ["http://www.facebook.com", 302],
        ["https://serverboards.io", 200],
        ["http://www.serverboards.io", 301],
      ]}
  end

  test "Order by" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    }
    {:ok, query} = ExoSQL.parse("
      SELECT url, name
        FROM urls
    ORDER BY url
      ", context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")


    assert result == %ExoSQL.Result{
      columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
      rows: [
        ["http://www.facebook.com", "Facebook"],
        ["http://www.serverboards.io", "Serverboards"],
        ["https://serverboards.io", "Serverboards"],
        ["https://serverboards.io/e404", "Serverboards"],
    ]}

    {:ok, result} = ExoSQL.query("
        SELECT url, name
          FROM urls
      ORDER BY url ASC
        ", context)
    assert result == %ExoSQL.Result{
      columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
      rows: [
        ["http://www.facebook.com", "Facebook"],
        ["http://www.serverboards.io", "Serverboards"],
        ["https://serverboards.io", "Serverboards"],
        ["https://serverboards.io/e404", "Serverboards"],
    ]}

    {:ok, result} = ExoSQL.query("
        SELECT url, name
          FROM urls
      ORDER BY url DESC
      ", context)
    assert result == %ExoSQL.Result{
        columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
        rows: [
          ["https://serverboards.io/e404", "Serverboards"],
          ["https://serverboards.io", "Serverboards"],
          ["http://www.serverboards.io", "Serverboards"],
          ["http://www.facebook.com", "Facebook"],
    ]}
  end

  test "Sort by result order" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    }
    # There is atrick here as if ask for url, name, url is at the 1st column of
    # the origin table, but we want the number 2 of the result table
    {:ok, query} = ExoSQL.parse("
        SELECT name, url
          FROM urls
      ORDER BY 2
      ", context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result == %ExoSQL.Result{
        columns: [{"A", "urls", "name"}, {"A", "urls", "url"}],
        rows: [
          ["Facebook", "http://www.facebook.com"],
          ["Serverboards", "http://www.serverboards.io"],
          ["Serverboards", "https://serverboards.io"],
          ["Serverboards", "https://serverboards.io/e404"],
        ]}
  end

  test "Reflection on current context" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    }
    {:ok, query} = ExoSQL.parse("SELECT * FROM self.tables", context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) > 0
  end

  test "Basic datetime support" do
    context = %{}
    {:ok, result} = ExoSQL.query("SELECT NOW()", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    {:ok, result} = ExoSQL.query("SELECT NOW() > TO_DATETIME(\"2018-01-30\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    {:ok, result} = ExoSQL.query("SELECT NOW() > TO_DATETIME(\"2050-01-30 12:35:21Z\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[false]]

    {:ok, result} = ExoSQL.query("SELECT to_datetime(0) < NOW()", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    {:ok, result} = ExoSQL.query("SELECT to_datetime(1517402656) == to_datetime(\"2018-01-31 12:44:16Z\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    {:ok, result} = ExoSQL.query("SELECT 1517402656 == to_timestamp(to_datetime(\"2018-01-31 12:44:16Z\"))", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    {:ok, result} = ExoSQL.query("SELECT strftime('2018-02-05T09:51:45.489Z', '%H:%M')", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["09:51"]]
  end

  test "Query node proc" do
    context = %{ "A" => {ExoSQL.Node, []}}
    {:ok, result} = ExoSQL.query("SELECT * FROM proc", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) > 0
  end

  test "Query with vars" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
      "__vars__" => %{
        "start" => "2017-01-01",
        "end" => "2017-12-31",
      }
    }

    {:ok, query} = ExoSQL.parse(
      """
      SELECT *
        FROM purchases
       WHERE (date >= $start) AND (date <= $end)
      """,
      context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) > 0
  end


  test "Aggregation on column only mentioned at aggregation" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    }

    {:ok, query} = ExoSQL.parse(
      """
      SELECT SUM(price)
        FROM products
        WHERE stock > 1
      """,
      context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) > 0
  end

  test "Substring" do
    context = %{}
    {:ok, result} = ExoSQL.query(
      """
      SELECT SUBSTR("test-mystring", 0, 4)
      """,
      context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["test"]]

    {:ok, result} = ExoSQL.query(
      """
      SELECT SUBSTR("test-mystring", 5, 400)
      """,
      context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["mystring"]]

    {:ok, result} = ExoSQL.query(
      """
      SELECT SUBSTR("test-mystring", 0, -9)
      """,
      context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["test"]]
  end
end
