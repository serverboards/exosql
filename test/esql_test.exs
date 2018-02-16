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
end
