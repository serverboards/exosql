require Logger

defmodule QueryTest.WillFail do
  def schema(_db) do
    {:ok, ["willfail"]}
  end

  def schema(_db, "willfail"), do: {:ok, %{columns: ["fail"]}}

  def execute(_db, "willfail", quals, _columns) do
    fail =
      Enum.find_value(quals, [], fn
        {"fail", "=", fail} -> fail
        _other -> "failure"
      end)

    raise fail
  end
end

defmodule QueryTest do
  use ExUnit.Case
  @moduletag :capture_log
  @moduletag timeout: 5_000

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    "B" => {QueryTest.WillFail, []}
  }

  def analyze_query!(query, context \\ @context) do
    Logger.debug("Query is:\n\n#{query}")
    {:ok, parsed} = ExoSQL.parse(query, context)
    Logger.debug("Parsed is #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
    Logger.debug("Plan is #{inspect(plan, pretty: true)}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    Logger.debug(inspect(result, pretty: true))
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    result
  end

  test "Simple parse SQL" do
    analyze_query!("SELECT A.products.name, A.products.price FROM A.products")
  end

  test "Comments" do
    analyze_query!("
      -- comment one
      SELECT -- comment two
      A.products.name,
      -- comment three
      A.products.price FROM A.products
      -- comment four
      ")
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

  # This BUG caused infinite recursion and finally timeout.
  test "No column at table" do
    try do
      analyze_query!("
        SELECT SUM(products.quantity*id) FROM products
        ")
    rescue
      _ in MatchError -> :ok
    end
  end

  test "MAX/MIN on dates" do
    res = analyze_query!("SELECT MIN(date), MAX(date) FROM purchases")
    assert res.rows == [["2015-08-10", "2018-01-02"]]

    res =
      analyze_query!(
        "SELECT strftime(MIN(to_datetime(date))), strftime(MAX(to_datetime(date))) FROM purchases"
      )

    assert res.rows == [["2015-08-10T00:00:00Z", "2018-01-02T00:00:00Z"]]
  end

  test "Simple WHERE" do
    context = %{
      "A" => {ExoSQL.Node, []}
    }

    analyze_query!(
      "SELECT A.passwd.uid, A.passwd.user, A.passwd.home FROM A.passwd WHERE A.passwd.uid >= 1001",
      context
    )
  end

  test "Operator precedence at WHERE" do
    res =
      analyze_query!(
        "SELECT * FROM purchases WHERE date >= '2017-01-01'  AND  date <= '2017-12-31'"
      )

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

    res = analyze_query!("SELECT NOT 'test'")
    assert res.rows == [[false]]

    res = analyze_query!("SELECT NOT ''")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT NOT format('')")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT NOT NOT 'test'")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT 1 IS (0 + 1)")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT 1 IS '1'")
    assert res.rows == [[false]]

    res = analyze_query!("SELECT 1 == '1'")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT 115 % 60")
    assert res.rows == [[55]]

    res = analyze_query!("SELECT round(1.2), round(1), round('1.6')")
    assert res.rows == [[1, 1, 2]]

    res = analyze_query!("SELECT ceil(1.2), ceil(1), ceil('1.6')")
    assert res.rows == [[2, 1, 2]]

    res = analyze_query!("SELECT floor(1.2), floor(1), floor('1.6')")
    assert res.rows == [[1, 1, 1]]
  end

  test "Test format BUG +-1" do
    res = analyze_query!("SELECT format('%+d', 1 - 100)")
    assert res.rows == [["-99"]]
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
             ]
           }
  end

  test "Multiples tables" do
    ExoSQL.explain(
      """
      SELECT A.products.name, A.users.name
        FROM A.products, A.purchases, A.users
       WHERE (A.products.id = A.purchases.product_id)
         AND (A.purchases.user_id = A.users.id)
      """,
      @context
    )

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

    result =
      analyze_query!(
        "SELECT A.products.name, (A.products.price || ' €'), ROUND( A.products.price * 0.21, 2 ), NOT (A.products.price > 10) FROM A.products",
        context
      )

    Logger.debug(ExoSQL.format_result(result))
  end

  test "Very simple aggregate" do
    result =
      analyze_query!("""
        SELECT user_id, COUNT(A.purchases.user_id)
          FROM A.purchases
         GROUP BY user_id
      """)

    assert result.rows == [
             ["1", 3],
             ["2", 2],
             ["3", 1]
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
    result =
      analyze_query!("""
        SELECT COUNT(*), AVG(A.products.price)
          FROM A.products
      """)

    assert result == %ExoSQL.Result{
             columns: [{:tmp, :tmp, "col_1"}, {:tmp, :tmp, "col_2"}],
             rows: [[4, 16.0]]
           }
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
               ["Serverboards", "https://serverboards.io/e404"]
             ]
           }
  end

  test "Partially defined data" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    result =
      analyze_query!(
        "
      SELECT products.name, users.name
        FROM products, purchases, users
        WHERE (products.id = product_id) and (user_id = users.id)
        ",
        context
      )

    Logger.debug(ExoSQL.format_result(result))
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
               {"A", "purchases", "id"},
               {"A", "products", "name"},
               {"A", "users", "name"}
             ],
             rows: [
               ["1", "sugus", "David"],
               ["2", "lollipop", "David"],
               ["3", "donut", "David"],
               ["4", "lollipop", "Javier"],
               ["5", "water", "Javier"],
               ["6", "sugus", "Patricio"]
             ]
           }
  end

  test "Advanced inner join, ask by qual" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
      "B" => {ExoSQL.HTTP, []}
    }

    result =
      analyze_query!(
        "
      SELECT urls.url, request.status_code
        FROM urls
       INNER JOIN request
          ON urls.url = request.url
    ",
        context
      )

    assert result == %ExoSQL.Result{
             columns: [{"A", "urls", "url"}, {"B", "request", "status_code"}],
             rows: [
               ["https://serverboards.io/e404", 404],
               ["http://www.facebook.com", 302],
               ["https://serverboards.io", 200],
               ["http://www.serverboards.io", 301]
             ]
           }
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
               ["https://serverboards.io/e404", "Serverboards"]
             ]
           }

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
               ["https://serverboards.io/e404", "Serverboards"]
             ]
           }

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
               ["http://www.facebook.com", "Facebook"]
             ]
           }
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

    result =
      analyze_query!(
        "SELECT to_datetime(1517402656) == to_datetime(\"2018-01-31 12:44:16Z\")",
        context
      )

    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result =
      analyze_query!(
        "SELECT 1517402656 == to_timestamp(to_datetime(\"2018-01-31 12:44:16Z\"))",
        context
      )

    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [[true]]

    result = analyze_query!("SELECT strftime('2018-02-05T09:51:45.489Z', '%H:%M')", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    assert result.rows == [["09:51"]]

    result = analyze_query!("SELECT to_string(now('UTC'))")
    [[dt]] = result.rows
    assert String.ends_with?(dt, "Z")

    result = analyze_query!("SELECT to_string(now())")
    [[_dt]] = result.rows
    # assert not String.ends_with?(dt, "Z")

    result = analyze_query!("SELECT to_string(now('US/Eastern'))")
    [[dt]] = result.rows
    # Might be Summer of Winter time
    assert String.ends_with?(dt, "-04:00") or String.ends_with?(dt, "-05:00")
  end

  test "Datetime operations" do
    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '-1D'))")
    assert result.rows == [["2018-02-04T09:51:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '+1D'))")
    assert result.rows == [["2018-02-06T09:51:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '+P1M'))")
    # Logger.warn("Adding 1 month gives extra days for feb!") # NOT ANYMORE!
    assert result.rows == [["2018-03-05T09:51:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '-1M'))")
    assert result.rows == [["2018-01-05T09:51:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '+P2Y'))")
    # Logger.warn("Adding 2 years add a day!")
    assert result.rows == [["2020-02-05T09:51:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '+2YT1M'))")
    assert result.rows == [["2020-02-05T09:52:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', '+PT45M'))")
    assert result.rows == [["2018-02-05T10:36:45.489Z"]]

    result = analyze_query!("SELECT to_string(to_datetime(0, 'Europe/Madrid'))")
    assert result.rows == [["1970-01-01T01:00:00+01:00"]]

    result = analyze_query!("SELECT to_string(to_datetime('2018-02-05T09:51:45.489Z', 'Japan'))")
    assert result.rows == [["2018-02-05T18:51:45.489+09:00"]]
  end

  test "Query node proc" do
    context = %{"A" => {ExoSQL.Node, []}}
    result = analyze_query!("SELECT * FROM proc", context)
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")

    assert Enum.count(result.rows) > 0
  end

  test "Query with vars" do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
      "__vars__" => %{
        "start" => "2017-01-01",
        "end" => "2017-12-31"
      }
    }

    result =
      analyze_query!(
        """
        SELECT *
          FROM purchases
         WHERE (date >= $start) AND (date <= $end)
        """,
        context
      )

    assert Enum.count(result.rows) > 0
  end

  test "Aggregation on column only mentioned at aggregation" do
    result =
      analyze_query!("""
      SELECT SUM(price)
        FROM products
        WHERE stock > 1
      """)

    assert Enum.count(result.rows) > 0
  end

  test "Substring" do
    context = %{}

    result =
      analyze_query!(
        """
        SELECT SUBSTR("test-mystring", 0, 4)
        """,
        context
      )

    assert result.rows == [["test"]]

    result =
      analyze_query!(
        """
        SELECT SUBSTR("test-mystring", 5, 400)
        """,
        context
      )

    assert result.rows == [["mystring"]]

    result =
      analyze_query!(
        """
        SELECT SUBSTR("test-mystring", 0, -9)
        """,
        context
      )

    assert result.rows == [["test"]]

    result =
      analyze_query!(
        """
        SELECT SUBSTR("test-mystring", 2, 2)
        """,
        context
      )

    assert result.rows == [["st"]]

    result =
      analyze_query!(
        """
        SELECT SUBSTR("test-mystring", 2, -2)
        """,
        context
      )

    assert result.rows == [["st-mystri"]]
  end

  test "Join and split strings" do
    result = analyze_query!("SELECT join(['the', 'answer', 42], ' ')")
    assert result.rows == [["the answer 42"]]

    result = analyze_query!("SELECT join([1,2,3])")
    assert result.rows == [["1,2,3"]]

    result = analyze_query!("SELECT split('the#answer#is#42', '#')")
    assert result.rows == [[["the", "answer", "is", "42"]]]

    result = analyze_query!("SELECT split('the,answer is, 42')")
    assert result.rows == [[["the", "answer", "is", "42"]]]

    result = analyze_query!("SELECT split('the answer is, 42')")
    assert result.rows == [[["the", "answer", "is", "42"]]]
  end

  test "Query with if" do
    res =
      analyze_query!("""
      SELECT name, (IF amount>20 THEN "Gold" ELSE "Silver" END) as metal
        FROM users
       INNER JOIN purchases ON user_id = users.id
      """)

    [{_, _, "name"}, {_, _, "metal"}] = res.columns
    assert Enum.count(res.rows) >= 1
  end

  test "Query with format() and not top level aggregation" do
    res =
      analyze_query!("""
        SELECT name, format("%.2f €", SUM(amount*price)) FROM purchases
        INNER JOIN products ON products.id = product_id
        GROUP BY name
      """)

    assert res == %ExoSQL.Result{
             columns: [{"A", "products", "name"}, {:tmp, :tmp, "col_2"}],
             rows: [
               ["donut", "300.00 €"],
               ["lollipop", "1320.00 €"],
               ["sugus", "60.00 €"],
               ["water", "200.00 €"]
             ]
           }

    # No group by
    res =
      analyze_query!("""
        SELECT format("%d units sold", SUM(amount)) FROM purchases
      """)

    assert Enum.count(res.rows) == 1
  end

  test "Width bucket to create histograms. Sales by month." do
    res =
      analyze_query!("""
      SELECT col_1, sum(amount) FROM
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), amount
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

    res =
      analyze_query!(
        """
        SELECT week
          FROM generate_series(
                  strftime($start, '%W'),
                  strftime($end, '%W'),
                  1
              ) AS week
        """,
        %{
          "__vars__" => %{
            "start" => "2017-01-01",
            "end" => "2017-12-31"
          }
        }
      )

    assert Enum.count(res.rows) == 53

    res =
      analyze_query!(
        "SELECT date FROM generate_series(to_datetime('2018-01-01'), to_datetime('2018-12-31')) AS date"
      )

    assert Enum.count(res.rows) == 365

    res =
      analyze_query!(
        "SELECT date FROM generate_series(to_datetime('2018-01-01'), to_datetime('2018-12-31'), '1M') AS date"
      )

    assert Enum.count(res.rows) == 12

    res =
      analyze_query!(
        "SELECT date FROM generate_series(to_datetime('2018-05-15T22:00:00.000Z'), to_datetime('2018-05-16T21:59:59.000Z'), 'T1H') AS date"
      )

    assert Enum.count(res.rows) == 24

    res =
      analyze_query!(
        "SELECT date FROM generate_series(to_datetime('2018-05-17T22:00:00.000Z'), to_datetime('2018-05-16T21:59:59.000Z'), '-T1H') AS date"
      )

    # There is a sneaky minute at the end range that makes it stop at 21h
    assert Enum.count(res.rows) == 25

    res =
      analyze_query!(
        "SELECT date FROM generate_series('2018-05-17T22:00:00.000Z', '2018-05-16T21:59:59.000Z', '-T1H') AS date"
      )

    # There is a sneaky minute at the end range that makes it stop at 21h
    assert Enum.count(res.rows) == 25

    try do
      res =
        analyze_query!(
          "SELECT date FROM generate_series('2018-05-17T22:00:00.000Z', '2018-05-16T21:59:59.000Z', 'T0H') AS date"
        )

      Logger.debug "Fixed bug no duration, infinite loop"
      res
    catch
      {:error, :invalid_duration} ->
        :ok
    end
  end

  test "Generate series as single call" do
    res = analyze_query!("
      SELECT generate_series(10)
    ")
    assert Enum.count(res.rows) == 10

    res = analyze_query!("
      SELECT 1, generate_series(10)
    ")
    assert Enum.count(res.rows) == 10

    res = analyze_query!("
      SELECT generate_series(10), 1
    ")
    assert Enum.count(res.rows) == 10

    res = analyze_query!("
      SELECT 1, generate_series(10), 1
    ")
    assert Enum.count(res.rows) == 10
    assert res.columns == [{:tmp, :tmp, "col_1"}, {:tmp, :tmp, "generate_series"}, {:tmp, :tmp, "col_3"}]
  end

  test "generate series as call and alias" do
    res = analyze_query!("
      SELECT generate_series(10) as t
    ")
    assert res.columns == [{:tmp, :tmp, "t"}]
    assert Enum.count(res.rows) == 10

    res = analyze_query!("
      SELECT n, generate_series(n) t FROM generate_series(3) n
    ")
    assert Enum.count(res.rows) == (1 + 2 + 3)
  end

  test "Fail get non existant column" do
    try do
      analyze_query!("SELECT nope FROM (SELECT 1)")
      flunk("Should fail bad query")
    rescue
      RuntimeError ->
        nil
    end

    try do
      analyze_query!("SELECT nope FROM generate_series(1,12,2)")
      flunk("Should fail bad query, generate_series has one column generate_series")
    rescue
      RuntimeError ->
        nil
    end
  end

  test "Width bucket to create histograms with alias." do
    res =
      analyze_query!("""
      SELECT months FROM
        generate_series(12) AS months
      """)

    assert res.columns == [{:tmp, "months", "months"}]
    assert Enum.count(res.rows) == 12
  end

  test "Width bucket to create histograms. INNER JOIN. 5 months." do
    res =
      analyze_query!("""
      SELECT month, sum(amount) FROM
        generate_series(12) AS month
        JOIN
          (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), amount
           FROM purchases)
        ON
          month = col_1
        GROUP BY month
      """)

    # innner join
    assert Enum.count(res.rows) == 5
  end

  test "Width bucket to create histograms. LEFT JOIN Return all months." do
    res =
      analyze_query!("""
      SELECT month, sum(amount) FROM
        generate_series(12) AS month
        LEFT OUTER JOIN
          (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), amount
           FROM purchases)
        ON
          month = col_1
        GROUP BY month
      """)

    # outer join
    assert Enum.count(res.rows) == 12
  end

  test "Width bucket to create histograms. RIGHT JOIN. Return all months." do
    res =
      analyze_query!("""
      SELECT month, sum(amount) FROM
          (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12), amount
          FROM purchases)
        RIGHT OUTER JOIN
          generate_series(12) AS month
        ON
          month = col_1
        GROUP BY month
      """)

    # outer join
    assert Enum.count(res.rows) == 12
  end

  test "Simple column alias as" do
    res =
      analyze_query!("""
      SELECT name AS first_name
        FROM users AS us
      """)

    assert res.columns == [{:tmp, "us", "first_name"}]
  end

  test "Simple table alias as" do
    res =
      analyze_query!("""
      SELECT * FROM users AS us
      """)

    assert res.columns == [{:tmp, "us", "id"}, {:tmp, "us", "name"}, {:tmp, "us", "email"}]
  end

  test "Function table with alias" do
    res =
      analyze_query!("""
      SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, amount
        FROM purchases
      """)

    assert res.columns == [{:tmp, :tmp, "month"}, {"A", "purchases", "amount"}]
  end

  test "Width bucket, table alias and column alias" do
    res =
      analyze_query!("""
      SELECT month.month, sum(amount) FROM
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, amount
          FROM purchases) AS hist
        RIGHT OUTER JOIN
          generate_series(12) AS month
        ON
          month.month = hist.month
        GROUP BY month.month
      """)

    # outer join
    assert Enum.count(res.rows) == 12
  end

  test "Ambiguous name in query, not smart enough for group removes columns. (FIXME)" do
    try do
      analyze_query!("""
      SELECT month, sum(amount) FROM
        (SELECT width_bucket(strftime(date, "%m"), 0, 12, 12) AS month, amount
          FROM purchases) AS hist
        RIGHT OUTER JOIN
          generate_series(12) AS month
        ON
          month.month = hist.month
        GROUP BY month.month
      """)

      flunk(
        "Should fail because of ambigous column. Actually should not if someday the parser is smarter about to use only group columns on select"
      )
    rescue
      RuntimeError -> :ok
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
    assert Enum.at(res.rows, 0) == [1]
    assert Enum.at(res.rows, 9) == [10]

    res = analyze_query!("SELECT * FROM generate_series(20) OFFSET 10 LIMIT 10")
    assert Enum.count(res.rows) == 10
    assert Enum.at(res.rows, 0) == [11]
    assert Enum.at(res.rows, 9) == [20]
  end

  test "Simple nested SELECT" do
    analyze_query!("SELECT id, (SELECT now()), amount FROM purchases")
  end

  test "Complex nested SELECT" do
    res =
      analyze_query!(
        "SELECT id, (SELECT name FROM products WHERE products.id = product_id), amount FROM purchases"
      )

    assert Enum.count(res.rows) == 6
    assert Enum.count(res.columns) == 3
    assert hd(res.rows) == ["1", "sugus", "10"]

    analyze_query!(
      "SELECT id, (SELECT name FROM products WHERE products.id = purchases.product_id), amount FROM purchases"
    )

    assert Enum.count(res.rows) == 6
    assert Enum.count(res.columns) == 3
    assert hd(res.rows) == ["1", "sugus", "10"]
  end

  test "nested SELECT corner cases" do
    res =
      analyze_query!(
        "SELECT 1, (SELECT generate_series FROM generate_series(1,10) WHERE generate_series == 0)"
      )

    assert res.rows == [[1, nil]]

    try do
      analyze_query!("SELECT 1, (SELECT generate_series FROM generate_series(1,10))")
      flunk("Did not trhow error")
    catch
      {:error, {:nested_query_too_many_columns, 10}} ->
        :ok

      _other ->
        flunk("Did not throw error")
    end
  end

  test "IN operator and lists" do
    res = analyze_query!("SELECT [1,2,3,4]")

    assert res.rows == [[[1, 2, 3, 4]]]

    res = analyze_query!("SELECT 2 IN [1,2,3,4]")
    assert res.rows == [[true]]

    res = analyze_query!("SELECT NOT (2 IN [1,2,3,4])")
    assert res.rows == [[false]]

    res = analyze_query!("SELECT 111 IN [1,2,3,4]")
    assert res.rows == [[false]]
  end

  test "jp at arrays" do
    res = analyze_query!("SELECT jp([1,2,3,4],'2')")
    assert res.rows == [[3]]

    res = analyze_query!("SELECT jp([1,2,3,4],2)")
    assert res.rows == [[3]]
  end

  test "jp at json" do
    with {:ok, parsed} <-
           ExoSQL.Parser.parse(
             "SELECT jp(json('{\"one\": 1, \"two\": {\"three\": 3}}'), 'two/three')",
             @context
           ),
         {:ok, planned} <- ExoSQL.Planner.plan(parsed) do
      Logger.debug("#{inspect(planned, pretty: true)}")
      assert planned == {:select, %ExoSQL.Result{columns: ["?NONAME"], rows: [[1]]}, [{:lit, 3}]}
    end

    res = analyze_query!("SELECT jp(json('{\"one\": 1, \"two\": {\"three\": 3}}'), 'two/three')")
    assert res.rows == [[3]]
  end

  test "SELECT IN" do
    res = analyze_query!("SELECT * FROM products WHERE id IN [1,2,3]")

    assert Enum.count(res.rows) == 3
  end

  test "NULL" do
    res = analyze_query!("SELECT NULL")

    assert res.rows == [[nil]]

    res =
      analyze_query!(
        "SELECT * FROM generate_series(5) LEFT JOIN urls ON id = generate_series WHERE url is NULL"
      )

    assert Enum.count(res.rows) == 1
  end

  test "LIKE/ILIKE" do
    assert ExoSQL.Expr.like("a", "a") == true
    assert ExoSQL.Expr.like("a", "%a") == true
    assert ExoSQL.Expr.like("a", "a%") == true
    assert ExoSQL.Expr.like("a", "%a%") == true
    assert ExoSQL.Expr.like("test", "test%") == true
    assert ExoSQL.Expr.like("testing", "test%") == true
    assert ExoSQL.Expr.like("testing", "%test%") == true
    assert ExoSQL.Expr.like("testing", "%test") == false
    assert ExoSQL.Expr.like("a", "%b") == false
    assert ExoSQL.Expr.like("aaaaaa", "%b") == false
    assert ExoSQL.Expr.like("aaaaaab", "%b") == true
    assert ExoSQL.Expr.like("aaaaaabaaa", "%b") == false
    assert ExoSQL.Expr.like("aaaaaabaaa", "%b%") == true
    assert ExoSQL.Expr.like("axxa", "a__a") == true

    res = analyze_query!("SELECT * FROM products WHERE name LIKE 'w%'")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE name LIKE 'W%'")
    assert Enum.count(res.rows) == 0
    res = analyze_query!("SELECT * FROM products WHERE name ILIKE 'W%'")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE name LIKE '%u%'")
    assert Enum.count(res.rows) == 2

    res = analyze_query!("SELECT * FROM products WHERE name LIKE 's%s'")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE name LIKE 's___s'")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE name LIKE '_o%'")
    assert Enum.count(res.rows) == 2
  end

  test "REGEX" do
    res = analyze_query!("SELECT * FROM products WHERE regex(name, '^.*$')")
    assert Enum.count(res.rows) == 4

    res = analyze_query!("SELECT * FROM products WHERE regex(name, '^s.*$')")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE regex(name, '^s(.*)$', 1)")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE regex(name, '^s(?<ug>.*)$', 'ug')")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE jp(regex(name, '^s(?<ug>.*)'), 'ug')")
    assert Enum.count(res.rows) == 1

    res = analyze_query!("SELECT * FROM products WHERE regex(name, 'https://.*')")
    assert Enum.count(res.rows) == 0
  end

  test "CASE WHEN" do
    res =
      analyze_query!("""
      SELECT
        name,
        CASE
          WHEN (price >= 20)
           THEN 'expensive'
          WHEN price >= 10
           THEN 'ok'
          ELSE 'cheap'
        END
      FROM products
      ORDER BY name
      """)

    assert res.rows == [
             ["donut", "expensive"],
             ["lollipop", "ok"],
             ["sugus", "cheap"],
             ["water", "expensive"]
           ]

    res =
      analyze_query!("""
      SELECT
        name,
        CASE
          WHEN (price >= 20)
           THEN 'expensive'
        END
      FROM products
      ORDER BY name
      """)

    assert res.rows == [
             ["donut", "expensive"],
             ["lollipop", nil],
             ["sugus", nil],
             ["water", "expensive"]
           ]
  end

  test "IF THEN" do
    res =
      analyze_query!("""
      SELECT
        name,
        IF   price >= 20 THEN 'expensive'
        ELIF price >= 10 THEN 'ok'
        ELSE 'cheap'
        END
      FROM products
      ORDER BY name
      """)

    assert res.rows == [
             ["donut", "expensive"],
             ["lollipop", "ok"],
             ["sugus", "cheap"],
             ["water", "expensive"]
           ]
  end

  test "RANDOM" do
    res = analyze_query!("SELECT random() FROM generate_series(10000)")
    assert Enum.all?(res.rows, fn [n] -> n >= 0.0 && n < 1.0 end)

    res = analyze_query!("SELECT randint(100) FROM generate_series(10000)")
    assert Enum.all?(res.rows, fn [n] -> n >= 0 && n < 100 end)

    res = analyze_query!("SELECT randint(50, 100) FROM generate_series(10000)")
    assert Enum.all?(res.rows, fn [n] -> n >= 50 && n < 100 end)
  end

  test "UNION" do
    res = analyze_query!("SELECT DISTINCT * FROM (SELECT 1)")
    assert res.rows == [[1]]

    res = analyze_query!("SELECT 1 UNION ALL SELECT 1")
    assert res.rows == [[1]]

    res = analyze_query!("SELECT 1 UNION SELECT 2")
    assert res.rows == [[1], [2]]

    res = analyze_query!("SELECT 1 UNION ALL SELECT 2")
    assert res.rows == [[1], [2]]

    res = analyze_query!("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 1")
    assert res.rows == [[1], [2], [3]]

    res =
      analyze_query!(
        "SELECT 'product_' || id, name FROM products UNION ALL SELECT 'user_' || id, name FROM users"
      )

    assert Enum.count(res.rows) == 7

    res =
      analyze_query!(
        "SELECT 'product_' || id, name FROM products UNION ALL SELECT 'user_' || id, name FROM users"
      )

    assert Enum.count(res.rows) == 7

    res = analyze_query!("SELECT MAX(a), MAX(b) FROM (SELECT 1 AS a, 0 AS b UNION SELECT 0, 1)")
    assert Enum.count(res.rows) == 1
    assert res.rows == [[1, 1]]
  end

  test "COUNT variations" do
    # count all
    res =
      analyze_query!(
        "SELECT COUNT(*) FROM generate_series(10) LEFT JOIN purchases ON purchases.id = generate_series"
      )

    assert res.rows == [[10]]

    # count not nulls
    res =
      analyze_query!(
        "SELECT COUNT(id) FROM generate_series(10) LEFT JOIN purchases ON purchases.id = generate_series"
      )

    assert res.rows == [[6]]

    # count not nulls AND unique product_id
    res =
      analyze_query!(
        "SELECT COUNT(DISTINCT product_id) FROM generate_series(10) LEFT JOIN purchases ON purchases.id = generate_series"
      )

    assert res.rows == [[4]]

    res =
      analyze_query!(
        "SELECT count(DISTINCT product_id) FROM generate_series(10) LEFT JOIN purchases ON purchases.id = generate_series"
      )

    assert res.rows == [[4]]
  end

  test "Same table with alias and joins" do
    res = analyze_query!("
      SELECT 'returning customers', COUNT(DISTINCT old.user_id) AS count
        FROM purchases AS new
        INNER JOIN purchases AS old
          ON old.user_id = new.user_id
        WHERE
          new.date >= '2017-01-01' AND new.date <= '2017-12-31' AND
          old.date < '2017-01-01'
      ")
    assert res.rows == [["returning customers", 1]]

    res = analyze_query!("
      SELECT 'new customers', a - b
        FROM (
          SELECT COUNT(DISTINCT user_id) AS a
          FROM purchases
          WHERE date >= '2017-01-01' AND date <= '2017-12-31'
        ), (
          SELECT COUNT(DISTINCT old.user_id) AS b
          FROM purchases AS new
          INNER JOIN purchases AS old
            ON old.user_id = new.user_id
          WHERE
            new.date >= '2017-01-01' AND new.date <= '2017-12-31' AND
            old.date < '2017-01-01'
        )
      ")
    assert res.rows == [["new customers", 2]]

    res = analyze_query!("
      SELECT 'total customers', COUNT(DISTINCT user_id)
        FROM purchases
        WHERE date >= '2017-01-01' AND date <= '2017-12-31'
        ")

    assert res.rows == [["total customers", 3]]
  end

  test "Huge JOIN do it fast" do
    # Uses task to give a max time (5s) for execution.
    pid =
      Task.async(fn ->
        res = analyze_query!("
        SELECT COUNT(*)
         FROM generate_series(10000) AS a
        INNER JOIN generate_series(10000) AS b
         ON a == b
      ")

        assert res.rows == [[10_000]]
      end)

    Task.await(pid)

    # same, use = to ensure works on both
    pid =
      Task.async(fn ->
        res = analyze_query!("
        SELECT COUNT(*)
         FROM generate_series(10000) AS a
        INNER JOIN generate_series(10000) AS b
         ON a = b
      ")

        assert res.rows == [[10_000]]
      end)

    Task.await(pid)
  end

  test "Ranges" do
    res =
      analyze_query!(
        "SELECT COUNT(*) FROM purchases WHERE date IN range('2017-01-01', '2017-12-31')"
      )

    assert res.rows == [[4]]

    # availability of product
    res =
      analyze_query!(
        "SELECT COUNT(*) FROM campaigns WHERE range(datestart,dateend) * range('2017-01-15', '2017-09-15')"
      )

    assert res.rows == [[3]]

    res = analyze_query!("
      SELECT lower(match), upper(match) FROM (
        SELECT (range(datestart,dateend) * range('2017-01-15', '2017-09-15')) AS match FROM campaigns
        )")

    assert res.rows == [
             ["2017-01-15", "2017-03-31"],
             ["2017-04-01", "2017-05-31"],
             ["2017-06-01", "2017-09-15"],
             [nil, nil]
           ]

    res = analyze_query!("SELECT lower(range(1,1000)), upper(range(1,1000))")
    assert res.rows == [[1, 1000]]
  end

  test "Coalesce and NULLIF" do
    res =
      analyze_query!("""
      SELECT oid, users.name, products.name, COALESCE(users.name, products.name, 'no name')
      FROM users
      RIGHT JOIN products
      ON products.id = users.id
      RIGHT JOIN generate_series(6) AS oid
      ON oid = products.id
      ORDER BY oid
      """)

    assert res.rows == [
             [1, "David", "sugus", "David"],
             [2, "Javier", "lollipop", "Javier"],
             [3, "Patricio", "donut", "Patricio"],
             [4, nil, "water", "water"],
             [5, nil, nil, "no name"],
             [6, nil, nil, "no name"]
           ]

    # This is a pattern used to change simple values, normally empty strings and so on
    res = analyze_query!("SELECT COALESCE(NULLIF(name, 'David'), 'Me') FROM users")
    assert res.rows == [["Me"], ["Javier"], ["Patricio"]]
  end

  test "WITH" do
    analyze_query!("
      WITH customers AS (
        SELECT * FROM users
      ),
      buy_per_customer AS (
        SELECT customers.id, customers.name, SUM(purchases.amount * products.price) as amount
          FROM products
        INNER JOIN purchases
           ON purchases.product_id = products.id
        INNER JOIN customers
           ON purchases.user_id = customers.id
        GROUP BY customers.id, customers.name
     )
     SELECT name AS Bourgeoisie
      FROM buy_per_customer
      WHERE amount > 10
    ")
  end

  test "SQL standard table alias" do
    analyze_query!("SELECT * FROM purchases pur")
    analyze_query!("SELECT name n FROM products pro")
  end

  test "SQL improved errors" do
    {type, _data} = ExoSQL.query("SELECT format('%2d', id) FROM purchases", @context)

    assert type == :error
  end

  @tag skip: "Only manual. Comment this line."
  test "Speed test" do
    {time, _data} =
      :timer.tc(fn ->
        ExoSQL.query("SELECT format('%05d', n) FROM generate_series(1000000) n", @context)
      end)

    Logger.debug("#{inspect(time / 1_000_000)}")

    flunk(1)
  end

  test "Random and Randint are not optimized" do
    res = analyze_query!("SELECT RANDOM() FROM generate_series(2)")
    assert Enum.at(res.rows, 0) != Enum.at(res.rows, 1)

    res = analyze_query!("SELECT RANDINT(0, 1000) FROM generate_series(2)")
    assert Enum.at(res.rows, 0) != Enum.at(res.rows, 1)
  end

  test "FROM LATERAL access to previous row data -> CROSS JOIN LATERAL" do
    # Uses data from json field to do a lateral join
    res2 =
      analyze_query!(
        "SELECT id, email, name FROM json CROSS JOIN LATERAL unnest(json, 'email', 'name')"
      )

    assert Enum.count(res2.rows) == 4

    res1 =
      analyze_query!("SELECT id, email, name FROM json, LATERAL unnest(json, 'email', 'name')")

    assert res1 == res2

    res1 =
      analyze_query!(
        "SELECT id, jp(unnest, 'email'), jp(unnest, 'name') FROM json, LATERAL unnest(json)"
      )

    assert res1.rows == res2.rows

    # MUST WORK!
    # res1 =
    #   analyze_query!(
    #     "SELECT id, jp(unnest, 'email'), jp(unnest, 'name') FROM json, unnest(json)"
    #   )
    #
    # assert res1.rows == res2.rows

    res1 =
      analyze_query!(
        "SELECT id, jp(json.json, 'email'), jp(json.json, 'name') FROM json AS orig, LATERAL json(orig.json)"
      )

    assert res1.rows == res2.rows

    res1 =
      analyze_query!(
        "SELECT id, jp(js.js, 'email'), jp(js.js, 'name') FROM json AS orig CROSS JOIN LATERAL json(orig.json) AS js"
      )

    assert res1.rows == res2.rows

    res1 =
      analyze_query!(
        "SELECT id, jp(js.js, 'email'), jp(js.js, 'name') FROM json AS orig, LATERAL json(orig.json) AS js"
      )

    assert res1.rows == res2.rows
  end

  test "CROSS JOIN function is always LATERAL" do
    res = analyze_query!("SELECT email, name FROM json, unnest(json, 'email', 'name')")

    assert Enum.count(res.rows) == 4
    assert Enum.count(res.columns) == 2

    res = analyze_query!("SELECT email, name FROM json, unnest(json, 'email', 'name') as data")

    assert Enum.count(res.rows) == 4
    assert Enum.count(res.columns) == 2

  end

  test "FROM LATERAL query" do
    # Single cross lateral
    analyze_query!("
      SELECT me.id, me.name, me.parentA_id, parentA.name FROM family AS me CROSS JOIN LATERAL (
          SELECT parentA.name FROM family AS parentA WHERE me.parentA_id = parentA.id
        )
    ")

    res = analyze_query!("
      SELECT me.id, me.name, me.parentA_id, parentA.name, me.parentB_id, parentB.name FROM family AS me
        CROSS JOIN LATERAL (
          SELECT parentA.name FROM family AS parentA WHERE me.parentA_id = parentA.id
        )
        CROSS JOIN LATERAL (
          SELECT parentB.name FROM family AS parentB WHERE me.parentB_id = parentB.id
        )
    ")

    assert res.rows == [
             ["1", "Mom", "0", "LUCA", "0", "LUCA"],
             ["2", "Dad", "0", "LUCA", "0", "LUCA"],
             ["3", "Son", "1", "Mom", "2", "Dad"],
             ["4", "Alice", "0", "LUCA", "0", "LUCA"],
             ["5", "Grandson", "3", "Son", "4", "Alice"]
           ]
  end

  test "Mixed LATERALs" do
    # a bit complicated but takes several cross lateral options
    analyze_query!("
      SELECT * FROM products, LATERAL (
        SELECT user_id, users.name FROM purchases CROSS JOIN LATERAL (
          SELECT * FROM users WHERE users.id = purchases.user_id
          ) WHERE purchases.product_id = products.id
        )
    ")
  end

  test "Early termination on WHERE false" do
    try do
      analyze_query!("
      SELECT * FROM willfail WHERE fail = 'bad-url://bad.bad'
      ")
      flunk("Should have failed")
    rescue
      _ -> :ok
    end

    analyze_query!("
      SELECT * FROM willfail WHERE fail = 'bad-url://bad.bad' and to_string(1) != 1
    ")
    analyze_query!("
      SELECT * FROM willfail WHERE fail = 'bad-url://bad.bad' AND to_string(1) != 1
    ")
  end

  test "Project JSON " do
    res = analyze_query!("""
      SELECT unnest(json, 'name', 'empty') FROM json
    """)

    assert Enum.count(res.columns) == 2
    assert Enum.count(res.rows) == 4
  end

  test "Project JSON literal" do
    res = analyze_query!("""
      SELECT unnest('[{"name": "test"}, {"name": "dos"}]', 'name', 'empty')
    """)

    assert Enum.count(res.columns) == 2
    assert Enum.count(res.rows) == 2

    res = analyze_query!("""
      SELECT unnest('[{"name": "test"}, {"name": "dos"}]', 'name')
    """)

    assert res.columns == [{:tmp, :tmp, "name"}]
    assert Enum.count(res.rows) == 2
  end

  test "CTE with with access to prev data" do
    res = analyze_query!("""
      WITH
         a AS (SELECT unnest(json, "name", "email") FROM json),
         b AS (SELECT name || " <" || email || ">" FROM a)
      SELECT
        *
      FROM b
    """)
    assert Enum.count(res.rows) == 4
  end
end
