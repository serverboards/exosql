require Logger

defmodule ExoSQLTest do
  use ExUnit.Case
  doctest ExoSQL
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

  test "Another database" do
    context = %{
      "A" => {ExoSQL.Node, []}
    }
    {:ok, result} = ExoSQL.query("SELECT A.passwd.uid, A.passwd.user, A.passwd.home FROM A.passwd WHERE A.passwd.uid >= 1001", context)
    Logger.debug("\n#{ExoSQL.format_result(result)}")
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

  @tag skip: "Not ready"
  test "Inner join" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = ExoSQL.parse("
      SELECT purchases.id, products.name, users.name
        FROM purchases
       INNER JOIN product
          ON purchases.product_id = products.id
       INNER JOIN users
          ON users.id = purchases.user_id
    ", context)
    Logger.debug("Query: #{inspect query}")
  end
end
