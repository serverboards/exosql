require Logger

defmodule QueryTest do
  use ExUnit.Case
  @moduletag :capture_log

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  }

  def analyze_query!(query, context \\ @context) do
    Logger.debug("Query is:\n\n#{query}")
    {:ok, parsed} = ExoSQL.parse(query, context)
    Logger.debug("Parsed is #{inspect parsed, pretty: true}")
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
    Logger.debug("Plan is #{inspect plan, pretty: true}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect result, pretty: true)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    result
  end


  test "Simple parse SQL" do
    analyze_query!("SELECT A.products.name, A.products.price FROM A.products")
  end

  test "No from" do
    context = %{}

    result = analyze_query!("SELECT 1 + '1'", context)
    assert result.rows == [[2]]

    result = analyze_query!("SELECT 'test'", context)
    assert result.rows == [["test"]]

    result = analyze_query!("SELECT upper(\"test\")", context)
    assert result.rows == [["TEST"]]
  end

  test "Simple WHERE" do
    context = %{
      "A" => {ExoSQL.Node, []}
    }
    analyze_query!("SELECT A.passwd.uid, A.passwd.user, A.passwd.home FROM A.passwd WHERE A.passwd.uid >= 1001", context)
  end

  test "Operator precedence at WHERE" do
    res = analyze_query!("SELECT * FROM purchases WHERE date >= '2017-01-01'  AND  date <= '2017-12-31'")

    assert Enum.count(res.rows) == 4

    res = analyze_query!("SELECT 1 + 1 == 1 + 1")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT 1 * 1 < 1 + 1")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT 1 * 1 + 1 * 1")
    assert res.rows == [[2]]

    res = analyze_query!("SELECT 1 + 1 + 1")
    assert res.rows == [[3]]

    res = analyze_query!("SELECT 1 + 1 * 1 + 1")
    assert res.rows == [[3]]

    res = analyze_query!("SELECT 1 + 2 / 2 + 1")
    assert res.rows == [[3]]

    res = analyze_query!("SELECT (1 + 2) / (2 + 1)")
    assert res.rows == [[1]]

    res = analyze_query!("SELECT (1 + 2) / (2 + 1)")
    assert res.rows == [[1]]

    res = analyze_query!("SELECT 1 + 2 AND 2 + 2")
    assert res.rows == [[4]]

    res = analyze_query!("SELECT 1 + 2 OR 2 + 2")
    assert res.rows == [[3]]

    res = analyze_query!("SELECT NOT false AND true")
    assert res.rows == [[true]]
  end

  test "Select * from" do
    result = analyze_query!("SELECT * FROM A.users")

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
    ExoSQL.explain("""
      SELECT A.products.name, A.users.name
        FROM A.products, A.purchases, A.users
       WHERE (A.products.id = A.purchases.product_id)
         AND (A.purchases.user_id = A.users.id)
      """, @context)
    analyze_query!("""
      SELECT A.products.name, A.users.name
        FROM A.products, A.purchases, A.users
       WHERE (A.products.id = A.purchases.product_id)
         AND (A.purchases.user_id = A.users.id)
      """)
  end

  test "Do some expression at select" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    result = analyze_query!("SELECT A.products.name, (A.products.price || ' €'), ROUND( A.products.price * 0.21, 2 ) FROM A.products",  context)
    Logger.debug(ExoSQL.format_result result)
  end

  test "Very simple aggregate" do
    result = analyze_query!("""
      SELECT user_id, COUNT(A.purchases.user_id)
        FROM A.purchases
       GROUP BY user_id
    """)

    assert result.rows == [
      ["1",3],
      ["2",2],
      ["3",1]
    ]
  end

  test "Aggregates" do
    analyze_query!("""
      SELECT A.products.name, COUNT(*), AVG(A.products.price * 1.21)
        FROM A.products, A.purchases
       WHERE A.products.id = A.purchases.product_id
       GROUP BY A.products.name
    """)
  end

  test "Aggregates no group" do
    result = analyze_query!("""
      SELECT COUNT(*), AVG(A.products.price)
        FROM A.products
    """)
    assert result == %ExoSQL.Result{columns: [{:tmp, :tmp, "col_1"}, {:tmp, :tmp, "col_2"}], rows: [[4, 16.0]]}
  end


  test "Sort by result order" do
    # There is atrick here as if ask for url, name, url is at the 1st column of
    # the origin table, but we want the number 2 of the result table
    result = analyze_query!("
        SELECT name, url
          FROM urls
      ORDER BY 2
      ")
    assert result == %ExoSQL.Result{
        columns: [{"A", "urls", "name"}, {"A", "urls", "url"}],
        rows: [
          ["Facebook", "http://www.facebook.com"],
          ["Serverboards", "http://www.serverboards.io"],
          ["Serverboards", "https://serverboards.io"],
          ["Serverboards", "https://serverboards.io/e404"],
        ]}
  end

  test "Partially defined data" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }
    result = analyze_query!("
      SELECT products.name, users.name
        FROM products, purchases, users
        WHERE (products.id = product_id) and (user_id = users.id)
        ", context)
    Logger.debug(ExoSQL.format_result result)
  end

  test "Inner join" do
    result = analyze_query!("
      SELECT purchases.id, products.name, users.name
        FROM purchases
       INNER JOIN products
          ON purchases.product_id = products.id
       INNER JOIN users
          ON users.id = purchases.user_id
    ")

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
    result = analyze_query!("
      SELECT urls.url, request.status_code
        FROM urls
       INNER JOIN request
          ON urls.url = request.url
    ", context)

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
    result = analyze_query!("
      SELECT url, name
        FROM urls
    ORDER BY url
      ")

    assert result == %ExoSQL.Result{
      columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
      rows: [
        ["http://www.facebook.com", "Facebook"],
        ["http://www.serverboards.io", "Serverboards"],
        ["https://serverboards.io", "Serverboards"],
        ["https://serverboards.io/e404", "Serverboards"],
    ]}

    result = analyze_query!("
        SELECT url, name
          FROM urls
      ORDER BY url ASC
        ")
    assert result == %ExoSQL.Result{
      columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
      rows: [
        ["http://www.facebook.com", "Facebook"],
        ["http://www.serverboards.io", "Serverboards"],
        ["https://serverboards.io", "Serverboards"],
        ["https://serverboards.io/e404", "Serverboards"],
    ]}

    result = analyze_query!("
        SELECT url, name
          FROM urls
      ORDER BY url DESC
      ")
    assert result == %ExoSQL.Result{
        columns: [{"A", "urls", "url"}, {"A", "urls", "name"}],
        rows: [
          ["https://serverboards.io/e404", "Serverboards"],
          ["https://serverboards.io", "Serverboards"],
          ["http://www.serverboards.io", "Serverboards"],
          ["http://www.facebook.com", "Facebook"],
    ]}
  end

  test "Reflection on current context" do
    result = analyze_query!("SELECT * FROM self.tables")

    assert Enum.count(result.rows) > 0
  end

  test "Basic datetime support" do
    context = %{}
    result = analyze_query!("SELECT NOW()", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    result = analyze_query!("SELECT NOW() > TO_DATETIME(\"2018-01-30\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result = analyze_query!("SELECT NOW() > TO_DATETIME(\"2050-01-30 12:35:21Z\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[false]]

    result = analyze_query!("SELECT to_datetime(0) < NOW()", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result = analyze_query!("SELECT to_datetime(1517402656) == to_datetime(\"2018-01-31 12:44:16Z\")", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result = analyze_query!("SELECT 1517402656 == to_timestamp(to_datetime(\"2018-01-31 12:44:16Z\"))", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result = analyze_query!("SELECT strftime('2018-02-05T09:51:45.489Z', '%H:%M')", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["09:51"]]
  end

  test "Query node proc" do
    context = %{ "A" => {ExoSQL.Node, []}}
    result = analyze_query!("SELECT * FROM proc", context)
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

    result = analyze_query!(
      """
      SELECT *
        FROM purchases
       WHERE (date >= $start) AND (date <= $end)
      """, context)

    assert Enum.count(result.rows) > 0
  end


  test "Aggregation on column only mentioned at aggregation" do
    result = analyze_query!(
      """
      SELECT SUM(price)
        FROM products
        WHERE stock > 1
      """)

    assert Enum.count(result.rows) > 0
  end


  test "Substring" do
    context = %{}
    result = analyze_query!(
      """
      SELECT SUBSTR("test-mystring", 0, 4)
      """,
      context)
    assert result.rows == [["test"]]

    result = analyze_query!(
      """
      SELECT SUBSTR("test-mystring", 5, 400)
      """,
      context)
    assert result.rows == [["mystring"]]

    result = analyze_query!(
      """
      SELECT SUBSTR("test-mystring", 0, -9)
      """,
      context)
    assert result.rows == [["test"]]

    result = analyze_query!(
      """
      SELECT SUBSTR("test-mystring", 2, 2)
      """,
      context)
    assert result.rows == [["st"]]

    result = analyze_query!(
      """
      SELECT SUBSTR("test-mystring", 2, -2)
      """,
      context)
    assert result.rows == [["st-mystri"]]
  end


  test "Query with if" do
    res = analyze_query!("""
      SELECT name, IF(ammount>20, "Gold", "Silver")
        FROM users
       INNER JOIN purchases ON user_id = users.id
      """)
    assert Enum.count(res.rows) >= 1
  end

  test "Query with format() and not top level aggregation" do
    res = analyze_query!("""
      SELECT name, format("%.2f €", SUM(ammount*price)) FROM purchases
      INNER JOIN products ON products.id = product_id
      GROUP BY name
    """)

    assert res == %ExoSQL.Result{
      columns: [{"A", "products", "name"}, {:tmp, :tmp, "col_2"}],
      rows: [
        ["donut", "300.00 €"], ["lollipop", "1320.00 €"],
        ["sugus", "60.00 €"], ["water", "200.00 €"]]}

    # No group by
    res = analyze_query!("""
      SELECT format("%d units sold", SUM(ammount)) FROM purchases
    """)

    assert Enum.count(res.rows) == 1
  end

  test "Width bucket to create histograms. Sales by month." do
    res = analyze_query!("""
    SELECT col_1, sum(ammount) FROM
      (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), ammount
       FROM purchases)
      GROUP BY col_1
    """)

    assert Enum.count(res.rows) == 5
  end

  test "Select from generate_series" do
    res = analyze_query!("SELECT * FROM generate_series(12)")

    assert Enum.count(res.rows) == 12

    res = analyze_query!("SELECT generate_series FROM generate_series(1,12,2)")

    assert Enum.count(res.rows) == 6

    res = analyze_query!("SELECT month FROM generate_series(1,12,2) AS month")

    assert res.columns == [{:tmp, "month", "month"}]
    assert Enum.count(res.rows) == 6


    res = analyze_query!("""
      SELECT week
        FROM generate_series(
                strftime($start, '%W'),
                strftime($end, '%W'),
                1
            ) AS week
      """, %{ "__vars__" => %{
          "start" => "2017-01-01",
          "end" => "2017-12-31",
        }})

    assert Enum.count(res.rows) == 53
  end

  test "Fail get non existant column" do
    try do
      analyze_query!("SELECT nope FROM (SELECT 1)")
      flunk "Should fail bad query"
    rescue
      MatchError ->
        nil
    end
    try do
      analyze_query!("SELECT nope FROM generate_series(1,12,2)")
      flunk "Should fail bad query, generate_series has one column generate_series"
    rescue
      MatchError ->
        nil
    end
  end

  test "Width bucket to create histograms with alias." do
    res = analyze_query!("""
    SELECT months FROM
      generate_series(12) AS months
    """)

    assert Enum.count(res.rows) == 12
  end

  test "Width bucket to create histograms. INNER JOIN. 5 months." do
    res = analyze_query!("""
    SELECT month, sum(ammount) FROM
      generate_series(12) AS month
      JOIN
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), ammount
         FROM purchases)
      ON
        month = col_1
      GROUP BY month
    """)

    assert Enum.count(res.rows) == 5 # innner join
  end

  test "Width bucket to create histograms. LEFT JOIN Return all months." do
    res = analyze_query!("""
    SELECT month, sum(ammount) FROM
      generate_series(12) AS month
      LEFT OUTER JOIN
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), ammount
         FROM purchases)
      ON
        month = col_1
      GROUP BY month
    """)

    assert Enum.count(res.rows) == 12 # outer join
  end

  test "Width bucket to create histograms. RIGHT JOIN. Return all months." do
    res = analyze_query!("""
    SELECT month, sum(ammount) FROM
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), ammount
        FROM purchases)
      RIGHT OUTER JOIN
        generate_series(12) AS month
      ON
        month = col_1
      GROUP BY month
    """)

    assert Enum.count(res.rows) == 12 # outer join
  end

  test "Simple column alias as" do
    res = analyze_query!("""
      SELECT name AS first_name
        FROM users AS us
      """)

    assert res.columns == [{:tmp, "us", "first_name"}]
  end

  test "Function table with alias" do
    res = analyze_query!("""
      SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, ammount
        FROM purchases
      """)
    assert res.columns == [{:tmp, :tmp, "month"}, {"A", "purchases", "ammount"}]
  end

  test "Width bucket, table alias and column alias" do
    res = analyze_query!("""
    SELECT month.month, sum(ammount) FROM
      (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, ammount
        FROM purchases) AS hist
      RIGHT OUTER JOIN
        generate_series(12) AS month
      ON
        month.month = hist.month
      GROUP BY month.month
    """)

    assert Enum.count(res.rows) == 12 # outer join
  end

  test "Ambiguous name in query, not smart enough for group removes columns. (FIXME)" do
    try do
      analyze_query!("""
      SELECT month, sum(ammount) FROM
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, ammount
          FROM purchases) AS hist
        RIGHT OUTER JOIN
          generate_series(12) AS month
        ON
          month.month = hist.month
        GROUP BY month.month
      """)
      flunk "Should fail because of ambigous column. Actually should not if someday the parser is smarter about to use only group columns on select"
    rescue
      MatchError -> :ok
    end
  end

  test "Some ops" do
    res = analyze_query!("SELECT 1 + 1")
    assert res.rows == [[2]]

    res = analyze_query!("SELECT 2 * 3")
    assert res.rows == [[6]]

    res = analyze_query!("SELECT 10 / 2")
    assert res.rows == [[5]]

    res = analyze_query!("SELECT 2 * 100 / 10")
    assert res.rows === [[20.0]]

    res = analyze_query!("SELECT round(2 * 100 / 10)")
    assert res.rows === [[20]]
  end


  test "Empty search sum is 0" do
    res = analyze_query!("SELECT SUM(price) FROM products WHERE id = 9999")
    assert res.rows == [[0]]
  end


  test "Distinct" do
    res = analyze_query!("SELECT DISTINCT product_id FROM purchases")
    assert Enum.count(res.rows) == 4

    res = analyze_query!("SELECT DISTINCT ON (user_id) * FROM purchases")
    assert Enum.count(res.rows) == 3
    assert Enum.count(res.columns) == 5
  end

  test "LIMIT" do
    res = analyze_query!("SELECT * FROM generate_series(20) LIMIT 10")
    assert Enum.count(res.rows) == 10
    assert (Enum.at res.rows, 0) == [1]
    assert (Enum.at res.rows, 9) == [10]

    res = analyze_query!("SELECT * FROM generate_series(20) OFFSET 10 LIMIT 10")
    assert Enum.count(res.rows) == 10
    assert (Enum.at res.rows, 0) == [11]
    assert (Enum.at res.rows, 9) == [20]
  end

  test "Simple nested SELECT" do
    analyze_query!("SELECT id, (SELECT now()), ammount FROM purchases")
  end

  test "Complex nested SELECT" do
    res = analyze_query!("SELECT id, (SELECT name FROM products WHERE id = product_id), ammount, product_id FROM purchases")
    analyze_query!("SELECT id, (SELECT name FROM products WHERE id = purchases.product_id), ammount, product_id FROM purchases")
    # flunk 1
  end
end
