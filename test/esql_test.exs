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
    {:ok, query} = ExoSQL.parse("SELECT A.products.name, A.users.name FROM A.products, A.purchases, A.users WHERE (A.products.id = A.purchases.product_id) and (A.purchases.user_id = A.user.id)")
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
end
