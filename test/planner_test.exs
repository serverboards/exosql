# require Logger

defmodule PlannerTest do
  use ExUnit.Case
  doctest ExoSQL.Planner, import: true

  test "Plan something"  do
    context = %{
      "A" => {ExoSQL.Csv, path: "test/data/csv/"}
    }

    ExoSQL.explain("SELECT name, price FROM products", context)
  end
end
