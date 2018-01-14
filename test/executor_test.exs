# require Logger

defmodule PlannerTest do
  use ExUnit.Case
  doctest ExoSQL.Executor, import: true
  @context %{ "A" => {ExoSQL.Csv, path: "test/data/csv/"} }

  test "Execute a simple manual plan"  do
    plan = {:execute, {{"A","products"}, [], [{"A","products","name"}, {"A","products","price"}]}}

    {:ok, result} = ExoSQL.Executor.execute(plan, @context)
    assert result == %ExoSQL.Result{
      columns: [{"A","products", "name"}, {"A","products","price"}],
      rows: [
        ["sugus", "3"],
        ["lollipop", "11"],
        ["donut", "30"],
        ["water", "20"]
      ]
    }
  end


  test "Execute a mix complex manual plan"  do
    plan = {
      :select,
        {{:filter,{
          {:execute, {{"A","products"}, [], [{"A","products","name"}, {"A","products","price"}]}},
          {:op, {">", {:column, {"A","products","price"}}, {:lit, "10"} } }
        } },
        [{:column, {"A","products","name"}}, {:column, {"A","products","price"}}]
      } }

    {:ok, result} = ExoSQL.Executor.execute(plan, @context)
    assert result == %ExoSQL.Result{
      columns: [{"A","products", "name"}, {"A","products","price"}],
      rows: [
        ["lollipop", "11"],
        ["donut", "30"],
        ["water", "20"]
      ]
    }
  end

  test "Execute a cross join" do
    plan = {
      :cross_join, {
        {:execute, {{"A","purchases"}, [], [{"A","purchases","user_id"},{"A","purchases", "product_id"}]}},
        {:execute, {{"A","products"}, [], [{"A","products","id"},{"A","products","name"}]}
      } } }
    {:ok, result} = ExoSQL.Executor.execute(plan, @context)
    assert result == %ExoSQL.Result{
     columns: [{"A", "purchases", "user_id"},
              {"A", "purchases", "product_id"}, {"A", "products", "id"},
              {"A", "products", "name"}],
     rows: [["1", "1", "1", "sugus"], ["1", "1", "2", "lollipop"],
            ["1", "1", "3", "donut"], ["1", "1", "4", "water"],
            ["1", "2", "1", "sugus"], ["1", "2", "2", "lollipop"],
            ["1", "2", "3", "donut"], ["1", "2", "4", "water"],
            ["1", "3", "1", "sugus"], ["1", "3", "2", "lollipop"],
            ["1", "3", "3", "donut"], ["1", "3", "4", "water"],
            ["2", "2", "1", "sugus"], ["2", "2", "2", "lollipop"],
            ["2", "2", "3", "donut"], ["2", "2", "4", "water"],
            ["2", "4", "1", "sugus"], ["2", "4", "2", "lollipop"],
            ["2", "4", "3", "donut"], ["2", "4", "4", "water"],
            ["3", "1", "1", "sugus"], ["3", "1", "2", "lollipop"],
            ["3", "1", "3", "donut"], ["3", "1", "4", "water"]]
        }
  end

  test "Execute a complex manual plan"  do
    plan = {:select,
      { { :filter,
        { {:cross_join, {
        {:execute, {{"A","users"}, [], [{"A", "users", "id"}, {"A", "users", "name"}]} },
          {:cross_join, {
            {:execute, {{"A","purchases"}, [], [{"A","purchases","user_id"},{"A","purchases", "product_id"}]} },
            {:execute, {{"A","products"}, [], [{"A","products","id"},{"A","products","name"}]} }
          } },
      } }, {:op, {"and",
          {:op, {"=", {:column, {"A","users","id"}}, {:column, {"A","purchases","user_id"}}}},
          {:op, {"=", {:column, {"A","purchases","product_id"}}, {:column, {"A","products","id"}}}},
        } } }
    }, [{:column, {"A", "users", "name"}}, {:column, {"A","products", "name"}}]}}

    {:ok, result} = ExoSQL.Executor.execute(plan, @context)
    assert result == %ExoSQL.Result{columns: [{"A", "users", "name"},
              {"A", "products", "name"}],
             rows: [["David", "sugus"], ["David", "lollipop"],
              ["David", "donut"], ["Javier", "lollipop"], ["Javier", "water"],
              ["Patricio", "sugus"]]}
  end

end
