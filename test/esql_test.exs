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

  test "Another database" do
    context = %{
      "A" => {Esql.Node, []}
    }
    {:ok, result} = Esql.query("SELECT A.passwd.uid, A.passwd.user, A.passwd.home FROM A.passwd WHERE A.passwd.uid >= 1001", context)
    Logger.debug("\n#{Esql.format_result(result)}")
  end
end
