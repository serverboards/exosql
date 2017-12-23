require Logger

defmodule EsqlTest do
  use ExUnit.Case
  doctest Esql

  test "Simple parse SQL" do
    context = %{
      "A" => {Esql.Csv, path: "test/data/csv/"}
    }
    {:ok, query} = Esql.parse("SELECT A.products.name, A.products.price FROM A.products")
    {:ok, result} = Esql.execute(query, context)
    Logger.debug(inspect result, pretty: true)
  end
end
