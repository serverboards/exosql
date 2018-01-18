require Logger

defmodule ExoSQL.Planner do
  @doc ~S"""
  Given a query, returns a tree of actions (AST) to perform to resolve the query.

  Each action is `{:plan, {step_function, step_data}}` to call.

  step_data may contain more tagged :plan to call recursively as required.

  They will be performed in reverse order and replaced where it is required.

  For example, it may return for a very simple:

    iex> {:ok, query} = ExoSQL.Parser.parse("SELECT name, price FROM products", %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    iex> plan(query)
    {:ok, {:select, {
          {:execute, {{"A", "products"}, [], []}}, [
            column: {"A", "products", "name"},
            column: {"A", "products", "price"}]}
    }}

  Or a more complex:

    iex> query = "SELECT users.name, products.name FROM users, purchases, products WHERE (users.id = purchases.user_id) AND (purchases.product_id = products.id)"
    iex> {:ok, query} = ExoSQL.Parser.parse(query, %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    iex> plan(query)
    {:ok, {:select, {
      {:filter, {
        {:cross_join, {
          {:execute, {{"A", "products"}, [], []}},
          {:cross_join, {
            {:execute, {{"A", "purchases"}, [], []}},
            {:execute, {{"A", "users"}, [], []}}
          }}
        }},
        {:op, {"AND",
              {:op, {"=",
                {:column, {"A", "users", "id"}},
                {:column, {"A", "purchases", "user_id"}}
              }},
              {:op, {"=",
                {:column, {"A", "purchases", "product_id"}},
                {:column, {"A", "products", "id"}}
              }}
          }}
        }},
        [column: {"A", "users", "name"},
         column: {"A", "products", "name"}]
    } } }

  Which means that it will extract A.users, cross join with A.purchases, then cross
  join that with A.produtcs, apply a filter of the expession, and finally
  return only the users name and product name.

  TODO: explore different plans acording to some weights and return the optimal one.
  """
  def plan(query) do
    Logger.debug("Prepare plan for query: #{inspect query}")

    from = for {db, table} <- query.from do
      columns = ExoSQL.Parser.get_vars(db, table, query.select)
      quals = []
      {:execute, {{db, table}, quals, columns}}
    end

    from_plan = Enum.reduce((tl from), (hd from), fn fr, acc ->
      {:cross_join, {fr, acc} }
    end)

    where_plan = if query.where do
      {:filter, {from_plan, query.where}}
    else
      from_plan
    end

    select_plan = {:select, {where_plan, query.select}}

    plan = select_plan

    {:ok, plan}
  end
end
