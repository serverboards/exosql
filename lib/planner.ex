require Logger

defmodule ExoSQL.Planner do
  @doc ~S"""
  Given a query, returns a tree of actions (AST) to perform to resolve the query.

  Each action is `{:plan, {step_function, step_data}}` to call.

  step_data may contain more tagged :plan to call recursively as required.

  They will be performed in reverse order and replaced where it is required.

  For example, it may return for a very simple:

    iex> plan("SELECT name, price FROM products", %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    {:execute, {{"A","products"}, [], ["name", "price"]}}

  Or a more complex:

    iex> plan("SELECT users.name, products.name FROM users, purchases, products WHERE users.id = purchases.user_id AND purchases_product_id = product.id", %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    {:select, {
      {:filter,
        {:cross_join, {
        {:execute, {{"A","users"}, [], ["id","name"]}},
          {:cross_join, {
            {:execute, {{"A","purchases"}, [], ["user_id","product_id"]}},
            {:execute, {{"A","products"}, [], ["id","name"]}}
          }},
      }}, {:op, {"AND",
          {:op, {"=", {:column, {"A","users","id"}}, {:column, {"A","purchases","user_id"}}}},
          {:op, {"=", {:column, {"A","purchases","product_id"}}, {:column, {"A","products","id"}}}},
        }}
    }, ["A.users.name", "A.products.name"]}}

  Which means that it will extract A.users, cross join with A.purchases, then cross
  join that with A.produtcs, apply a filter of the expession, and finally
  return only the users name and product name.

  TODO: explore different plans acording to some weights and return the optimal one.
  """
  def plan(query, context) do
    Logger.debug("Prepare plan for query: #{inspect query}")

        for {db, table} <- query.from do
          columns = ExoSQL.Parser.get_vars(db, table, query.select)
          quals = []
          {db, table, quals, columns}
        end
  end
end
