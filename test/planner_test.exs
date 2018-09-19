require Logger

defmodule PlannerTest do
  use ExUnit.Case
  doctest ExoSQL.Planner, import: true
  @moduletag :capture_log

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"}
  }

  test "Plan something" do
    q = "SELECT name, stock FROM products WHERE (price > 0) and (stock >= 1)"

    {:ok, parsed} = ExoSQL.Parser.parse(q, @context)
    {:ok, plan} = ExoSQL.Planner.plan(parsed)

    assert plan ==
      {:select,
        {:filter,
          {:execute, {"A", "products"}, [["price", ">", 0], ["stock", ">=", 1]], [
            {"A", "products", "price"},
            {"A", "products", "stock"},
            {"A", "products", "name"},
          ]},
          {:op, {"AND",
            {:op, {">", {:column, {"A", "products", "price"}}, {:lit, 0}}},
            {:op, {">=", {:column, {"A", "products", "stock"}}, {:lit, 1}}
          }}}
        },
        [
          column: {"A", "products", "name"},
          column: {"A", "products", "stock"}
        ]
      }

    ExoSQL.explain(q, @context)
  end

  test "Ask for quals" do
    {:ok, parsed} =
      ExoSQL.parse(
        "SELECT name, stock FROM products WHERE (stock > 0) AND (price <= 100)",
        @context
      )

    Logger.debug("Parsed: #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.plan(parsed, @context)
    Logger.debug("Planned: #{inspect(plan, pretty: true)}")

    {:execute, _from, quals, _columns} = plan |> elem(1) |> elem(1)
    Logger.debug("quals: #{inspect(quals)}, should be stock > 0, price <= 100")

    assert quals == [["stock", ">", 0], ["price", "<=", 100]]

    # Maybe OR
    {:ok, parsed} =
      ExoSQL.parse(
        "SELECT name, stock>0 FROM products WHERE (stock > 0) OR (price <= 100)",
        @context
      )

    Logger.debug("Parsed: #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.plan(parsed, @context)
    Logger.debug("Planned: #{inspect(plan, pretty: true)}")

    {:execute, _from, quals, _columns} = plan |> elem(1) |> elem(1)
    Logger.debug("quals: #{inspect(quals)}, should be []")

    assert quals == []

    #
    {:ok, parsed} =
      ExoSQL.parse(
        "SELECT name, stock FROM products WHERE (stock > $test) AND (price <= 100)",
        @context
      )

    Logger.debug("Parsed: #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.plan(parsed, @context)
    Logger.debug("Planned: #{inspect(plan, pretty: true)}")

    {:execute, _from, quals, _columns} = plan |> elem(1) |> elem(1)
    Logger.debug("quals: #{inspect(quals)}, should be stock > $test, price <= 100")

    assert quals == [["stock", ">", {:var, "test"}], ["price", "<=", 100]]
  end

  test "quals when ALIAS" do
    {:ok, parsed} = ExoSQL.parse("SELECT * FROM products AS p WHERE id > 0", @context)
    Logger.debug("Parsed: #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.plan(parsed, @context)
    Logger.debug("Planned: #{inspect(plan, pretty: true)}")

    {:select, {:filter, {:alias, {:execute, _table, quals, _columns}, _}, _}, _} = plan

    Logger.debug("quals: #{inspect(quals)}")
    assert quals == [["id", ">", 0]]
  end
end
