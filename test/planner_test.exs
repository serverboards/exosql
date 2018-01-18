require Logger

defmodule PlannerTest do
  use ExUnit.Case
  doctest ExoSQL.Planner, import: true

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"}
  }

  test "Plan something"  do
    q = "SELECT name, stock FROM products WHERE (price > 0) and (stock >= 1)"

    {:ok, parsed} = ExoSQL.Parser.parse(q, @context)
    {:ok, plan} = ExoSQL.Planner.plan(parsed)

    assert plan ==
      {:select,
        {:filter,
          {:execute, {"A", "products"}, [], []},
          {:op, {"and",
            {:op, {">", {:column, {"A", "products", "price"}}, {:lit, "0"}}},
            {:op, {">=", {:column, {"A", "products", "stock"}}, {:lit, "1"}}
          }}}
        },
        [
          column: {"A", "products", "name"},
          column: {"A", "products", "stock"}
        ]
      }

    ExoSQL.explain(q, @context)
  end

  test "Plan over several tables" do

  end
end
