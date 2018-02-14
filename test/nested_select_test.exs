require Logger

defmodule NestedSelectTest do
  use ExUnit.Case
  doctest ExoSQL
  doctest ExoSQL.Expr
  @moduletag :capture_log

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  }


  test "Nested SELECT" do
    {:ok, query} = ExoSQL.parse(
      """
      SELECT * FROM (
        SELECT user_id, SUM(ammount)
          FROM purchases
          GROUP BY user_id
        ) ORDER BY 2
      """,
      @context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, @context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, @context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) == 3
  end
  test "Nested SELECT 2" do
    {:ok, query} = ExoSQL.parse(
      """
      SELECT name, col_2 FROM (
        SELECT user_id, SUM(ammount)
          FROM purchases
          GROUP BY user_id
        ), (SELECT id, name FROM users)
        WHERE user_id = id
        ORDER BY 2
      """,
      @context)
    Logger.debug("Query: #{inspect query, pretty: true}")
    {:ok, plan} = ExoSQL.plan(query, @context)
    Logger.debug("Plan: #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.execute(plan, @context)
    Logger.debug("Result: #{inspect result, pretty: true}")
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) == 3
    assert result == %ExoSQL.Result{
      columns: [{"A", "users", "name"}, {:tmp, :tmp, "col_2"}],
      rows: [["Patricio", 10], ["David", 40], ["Javier", 110]]
    }
  end
end