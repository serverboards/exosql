require Logger

defmodule ExecutorTest do
  use ExUnit.Case
  doctest ExoSQL.Executor, import: true
  @moduletag :capture_log

  @context %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}}

  test "Execute a simple manual plan" do
    plan =
      {:execute, {:table, {"A", "products"}}, [],
       [{"A", "products", "name"}, {"A", "products", "price"}]}

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [{"A", "products", "name"}, {"A", "products", "price"}],
             rows: [
               ["sugus", "3"],
               ["lollipop", "11"],
               ["donut", "30"],
               ["water", "20"]
             ]
           }
  end

  test "Execute a mix complex manual plan" do
    plan = {
      :select,
      {:filter,
       {:execute, {:table, {"A", "products"}}, [],
        [{"A", "products", "name"}, {"A", "products", "price"}]},
       {:op, {">", {:column, {"A", "products", "price"}}, {:lit, "10"}}}},
      [{:column, {"A", "products", "name"}}, {:column, {"A", "products", "price"}}]
    }

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [{"A", "products", "name"}, {"A", "products", "price"}],
             rows: [
               ["lollipop", "11"],
               ["donut", "30"],
               ["water", "20"]
             ]
           }
  end

  test "Execute a cross join" do
    plan = {
      :cross_join,
      {:execute, {:table, {"A", "purchases"}}, [],
       [{"A", "purchases", "user_id"}, {"A", "purchases", "product_id"}]},
      {:execute, {:table, {"A", "products"}}, [],
       [{"A", "products", "id"}, {"A", "products", "name"}]}
    }

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [
               {"A", "purchases", "user_id"},
               {"A", "purchases", "product_id"},
               {"A", "products", "id"},
               {"A", "products", "name"}
             ],
             rows: [
               ["1", "1", "1", "sugus"],
               ["1", "1", "2", "lollipop"],
               ["1", "1", "3", "donut"],
               ["1", "1", "4", "water"],
               ["1", "2", "1", "sugus"],
               ["1", "2", "2", "lollipop"],
               ["1", "2", "3", "donut"],
               ["1", "2", "4", "water"],
               ["1", "3", "1", "sugus"],
               ["1", "3", "2", "lollipop"],
               ["1", "3", "3", "donut"],
               ["1", "3", "4", "water"],
               ["2", "2", "1", "sugus"],
               ["2", "2", "2", "lollipop"],
               ["2", "2", "3", "donut"],
               ["2", "2", "4", "water"],
               ["2", "4", "1", "sugus"],
               ["2", "4", "2", "lollipop"],
               ["2", "4", "3", "donut"],
               ["2", "4", "4", "water"],
               ["3", "1", "1", "sugus"],
               ["3", "1", "2", "lollipop"],
               ["3", "1", "3", "donut"],
               ["3", "1", "4", "water"]
             ]
           }
  end

  test "Execute a complex manual plan" do
    plan =
      {:select,
       {:filter,
        {:cross_join,
         {:execute, {:table, {"A", "users"}}, [], [{"A", "users", "id"}, {"A", "users", "name"}]},
         {:cross_join,
          {:execute, {:table, {"A", "purchases"}}, [],
           [{"A", "purchases", "user_id"}, {"A", "purchases", "product_id"}]},
          {:execute, {:table, {"A", "products"}}, [],
           [{"A", "products", "id"}, {"A", "products", "name"}]}}},
        {:op,
         {"AND",
          {:op, {"=", {:column, {"A", "users", "id"}}, {:column, {"A", "purchases", "user_id"}}}},
          {:op,
           {"=", {:column, {"A", "purchases", "product_id"}}, {:column, {"A", "products", "id"}}}}}}},
       [{:column, {"A", "users", "name"}}, {:column, {"A", "products", "name"}}]}

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [{"A", "users", "name"}, {"A", "products", "name"}],
             rows: [
               ["David", "sugus"],
               ["David", "lollipop"],
               ["David", "donut"],
               ["Javier", "lollipop"],
               ["Javier", "water"],
               ["Patricio", "sugus"]
             ]
           }
  end

  test "Execute manual simple aggregation" do
    # SELECT COUNT(*) FROM products
    # converted to
    # SELECT COUNT(A.products.*) FROM products GROUP BY true ## all to one set, returns the table true, {"A","products","*"}
    plan =
      {:select,
       {:group_by,
        {:execute, {:table, {"A", "products"}}, [],
         [{"A", "products", "id"}, {"A", "products", "name"}]}, [{:lit, true}]},
       [{:fn, {"count", [{:column, 1}, {:pass, {:lit, "*"}}]}}]}

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)
    assert result == %ExoSQL.Result{columns: [{:tmp, :tmp, "col_1"}], rows: [[4]]}
  end

  test "Execute complex aggregation" do
    plan =
      {:select,
       {:group_by,
        {:execute, {:table, {"A", "purchases"}}, [], [{"A", "purchases", "product_id"}]},
        [{:column, {"A", "purchases", "product_id"}}]},
       [
         {:column, {"A", "purchases", "product_id"}},
         {:fn, {"count", [{:column, 1}, {:pass, {:lit, "*"}}]}}
       ]}

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [{"A", "purchases", "product_id"}, {:tmp, :tmp, "col_2"}],
             rows: [["1", 2], ["2", 2], ["3", 1], ["4", 1]]
           }
  end

  test "Execute complex aggregation 2" do
    # SELECT users.name, SUM(price*amount) FROM users, purchases, products
    # WHERE users.id = purchases.user_id AND products.id = purchases.product_id
    # GROUP BY product.id
    plan =
      {:select,
       {:group_by,
        {:filter,
         {:cross_join,
          {:execute, {:table, {"A", "users"}}, [],
           [{"A", "users", "id"}, {"A", "users", "name"}]},
          {:cross_join,
           {:execute, {:table, {"A", "purchases"}}, [],
            [
              {"A", "purchases", "user_id"},
              {"A", "purchases", "product_id"},
              {"A", "purchases", "amount"}
            ]},
           {:execute, {:table, {"A", "products"}}, [],
            [{"A", "products", "id"}, {"A", "products", "name"}, {"A", "products", "price"}]}}},
         {:op,
          {"AND",
           {:op,
            {"=", {:column, {"A", "users", "id"}}, {:column, {"A", "purchases", "user_id"}}}},
           {:op,
            {"=", {:column, {"A", "purchases", "product_id"}}, {:column, {"A", "products", "id"}}}}}}},
        [{:column, {"A", "users", "name"}}]},
       [
         {:column, {"A", "users", "name"}},
         {:fn,
          {"sum",
           [
             {:column, 1},
             {:pass,
              {:op,
               {"*", {:column, {"A", "products", "price"}},
                {:column, {"A", "purchases", "amount"}}}}}
           ]}}
       ]}

    {:ok, result, _context} = ExoSQL.Executor.execute(plan, @context)

    assert result == %ExoSQL.Result{
             columns: [{"A", "users", "name"}, {:tmp, :tmp, "col_2"}],
             rows: [["David", 550], ["Javier", 1300], ["Patricio", 30]]
           }

    Logger.info(ExoSQL.format_result(result))
  end

  test "extra quals on = and ==" do
    q =
      ExoSQL.Executor.get_extra_quals(
        %{columns: [{:tmp, :tmp, "A"}], rows: []},
        {:op, {"=", {:column, {:tmp, :tmp, "A"}}, {:column, {:tmp, "B", "B"}}}},
        @context
      )

    assert q != []

    q =
      ExoSQL.Executor.get_extra_quals(
        %{columns: [{:tmp, :tmp, "A"}], rows: []},
        {:op, {"==", {:column, {:tmp, :tmp, "A"}}, {:column, {:tmp, "B", "B"}}}},
        @context
      )

    assert q != []
  end
end
