require Logger

defmodule ExoSQLTest do
  use ExUnit.Case
  doctest ExoSQL

  test "Cartesian product at CrossJoinTables" do
    rows = [
      [[1,11],[2,22],[3,33]],
      [[4,44],[5,55],[6,66]],
      [[7,77],[8,88],[9,99]],
    ]
    cjt = %ExoSQL.CrossJoinTables{
      headers: [[:a, :aa],[:b, :bb],[:c, :cc]],
      rows: rows
    }

    Enum.map(cjt, &(Logger.info("Row: #{inspect &1}")))
  end

  test "Simple parse SQL" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = ExoSQL.parse("SELECT A.products.name, A.products.price FROM A.products")
    {:ok, result} = ExoSQL.execute(query, context)
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
    {:ok, query} = ExoSQL.parse("SELECT A.products.name, A.users.name FROM A.products, A.purchases, A.users WHERE (A.products.id = A.purchases.product_id) and (A.purchases.user_id = A.users.id)")
    Logger.debug("Query: #{inspect query}")
    {:ok, result} = ExoSQL.execute(query, context)
    Logger.debug(ExoSQL.format_result result)
  end

  test "Do some expression at select" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    {:ok, result} = ExoSQL.query("SELECT A.products.name, (A.products.price || ' €'), ROUND( A.products.price * 0.21, 2 ) FROM A.products",  context)

    Logger.debug(ExoSQL.format_result result)
  end

  test "Aggregates" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, result} = ExoSQL.query("""
      SELECT A.products.name, COUNT(*), AVG(A.products.price * 1.21)
        FROM A.products, A.purchases
       WHERE A.products.id = A.purchases.product_id
       GROUP BY A.products.name
    """, context)

    Logger.debug(ExoSQL.format_result result)
  end

  test "Aggregates no group" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    {:ok, result} = ExoSQL.query("""
      SELECT COUNT(*), AVG(A.products.price)
        FROM A.products
    """, context)

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
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    assert {:ok, {:table, {"A", "products"}}} ==
      ExoSQL.resolve_table({:table, {nil, "products"}}, context)

    assert {:ok, {:table, {"A", "products"}}} ==
      ExoSQL.resolve_table({:table, {"A", "products"}}, context)


    assert {:error, :not_found} ==
      ExoSQL.resolve_table({:table, {nil, "prioducts"}}, context)

    assert {:error, :ambiguous_table_name} ==
      ExoSQL.resolve_table({:table, {nil, "products"}}, %{
        "A" => {ExoSQL.Csv, path: "test/data/csv"},
        "B" => {ExoSQL.Csv, path: "test/data/csv"},
        })



    assert {:ok, {:column, {"A", "products", "price"}}} ==
      ExoSQL.resolve_column({:column, {nil, nil, "price"}},
        [
          {:table, {"A", "users"}},
          {:table, {"A", "purchases"}},
          {:table, {"A", "products"}}],
        context)

    assert {:ok, {:column, {"A", "products", "price"}}} ==
      ExoSQL.resolve_column({:column, {nil, "products", "price"}},
        [
          {:table, {"A", "users"}},
          {:table, {"A", "purchases"}},
          {:table, {"A", "products"}}],
        context)
    assert {:ok, {:column, {"A", "products", "price"}}} ==
      ExoSQL.resolve_column({:column, {"A", "products", "price"}},
        [
          {:table, {"A", "users"}},
          {:table, {"A", "purchases"}},
          {:table, {"A", "products"}}],
        context)

    assert {:error, :not_found} ==
      ExoSQL.resolve_column({:column, {nil, nil, "prix"}},
        [
          {:table, {"A", "users"}},
          {:table, {"A", "purchases"}},
          {:table, {"A", "products"}}],
        context)
    assert {:error, :ambiguous_column_name} ==
      ExoSQL.resolve_column({:column, {nil, nil, "id"}},
        [
          {:table, {"A", "users"}},
          {:table, {"A", "purchases"}},
          {:table, {"A", "products"}}],
        context)
  end
end
