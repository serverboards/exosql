require Logger

defmodule ExoSQLTest do
  use ExUnit.Case
  doctest ExoSQL
  doctest ExoSQL.Expr
  @moduletag :capture_log

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
      {:not_found, {"prix", :in, _}} -> :ok
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
end
