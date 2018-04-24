require Logger

defmodule NestedSelectTest do
  use ExUnit.Case
  doctest ExoSQL
  doctest ExoSQL.Expr
  @moduletag :capture_log

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  }

  def analyze_query!(query, context \\ @context) do
    QueryTest.analyze_query!(query, context)
  end



  test "Nested SELECT" do
    {:ok, query} = ExoSQL.parse(
      """
      SELECT * FROM (
        SELECT user_id, SUM(amount)
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
        SELECT user_id, SUM(amount)
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

  test "SELECT FROM with alias" do
    res = analyze_query!("SELECT * FROM (SELECT id, name FROM products LIMIT 3)")
    assert Enum.count(res.rows) == 3

    res = analyze_query!("SELECT * FROM (SELECT id, name FROM products LIMIT 3) AS prods")
    assert res.columns == [{:tmp, "prods", "id"}, {:tmp, "prods", "name"}]
    assert Enum.count(res.rows) == 3

    res = analyze_query!("SELECT * FROM (SELECT id AS pid, name AS product_name FROM products LIMIT 3) AS prods ORDER BY prods.pid")
    assert Enum.count(res.rows) == 3
    assert res.columns == [{:tmp, "prods", "pid"}, {:tmp, "prods", "product_name"}]
  end

  test "Complex select with alias" do
    res = analyze_query!("
      SELECT name, amount*price AS total, to_number(stock) AS stock
        FROM (
          SELECT product_id AS pid, SUM(amount) AS amount
            FROM purchases
        GROUP BY product_id
        ORDER BY 2 DESC
        LIMIT 2
        ) AS purchss
      LEFT JOIN products
        ON products.id = pid
      ORDER BY 3 DESC")
    assert res.columns == [{"A", "products", "name"}, {:tmp, :tmp, "total"}, {:tmp, :tmp, "stock"}]
    assert Enum.count(res.rows) == 2
  end

end
