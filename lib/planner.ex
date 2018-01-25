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
    {:ok,
      {:select,
        {:execute, {"A", "products"}, [], [{"A", "products", "name"}, {"A", "products", "price"}]}, [
          column: {"A", "products", "name"},
          column: {"A", "products", "price"}]
        }
    }

  Or a more complex:

    iex> query = "SELECT users.name, products.name FROM users, purchases, products WHERE (users.id = purchases.user_id) AND (purchases.product_id = products.id)"
    iex> {:ok, query} = ExoSQL.Parser.parse(query, %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    iex> plan(query)
    {:ok,
      {:select,
        {:filter,
          {:cross_join,
            {:execute, {"A", "products"}, [], [{"A", "products", "id"}, {"A", "products", "name"}]},
            {:cross_join,
              {:execute, {"A", "purchases"}, [], [
                  {"A", "purchases", "user_id"},
                  {"A", "purchases", "product_id"}
                ]},
              {:execute, {"A", "users"}, [], [{"A", "users", "id"}, {"A", "users", "name"}]}
            }
          },
          {:op, {"AND",
           {:op, {"=",
             {:column, {"A", "users", "id"}},
             {:column, {"A", "purchases", "user_id"}}}
            },
            {:op, {"=",
             {:column, {"A", "purchases", "product_id"}},
             {:column, {"A", "products", "id"}}}
         }}},
        },
        [column: {"A", "users", "name"},
         column: {"A", "products", "name"}]
      } }

  Which means that it will extract A.users, cross join with A.purchases, then cross
  join that with A.produtcs, apply a filter of the expession, and finally
  return only the users name and product name.

  TODO: explore different plans acording to some weights and return the optimal one.
  """
  def plan(query) do
    from = for {db, table} <- query.from do
      all_expressions = [query.where, query.select, query.groupby]

      Logger.debug(inspect all_expressions)

      columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
      quals = []
      {:execute, {db, table}, quals, columns}
    end

    from_plan = Enum.reduce((tl from), (hd from), fn fr, acc ->
      {:cross_join, fr, acc}
    end)

    where_plan = if query.where do
      {:filter, from_plan, query.where}
    else
      from_plan
    end

    group_plan = if query.groupby do
      {:group_by, where_plan, query.groupby}
    else
      where_plan
    end

    select_plan = {:select, group_plan, query.select}

    plan = select_plan

    {:ok, plan}
  end

  # Gets all the vars referenced in an expression that refer to a given table
  def get_table_columns_at_expr(db, table, l) when is_list(l) do
    # Logger.debug("Get columns at expr #{inspect table} #{inspect l}")
    res = Enum.flat_map(l, &get_table_columns_at_expr(db, table, &1))
    # Logger.debug("res #{inspect res}")
    res
  end
  def get_table_columns_at_expr(db, table, {:op, {_op, op1, op2}}) do
    get_table_columns_at_expr(db, table, op1) ++ get_table_columns_at_expr(db, table, op2)
  end
  def get_table_columns_at_expr(db, table, {:column, {db, table, var} = res}), do: [res]
  def get_table_columns_at_expr(db, table, {:fn, {f, params}}), do: Enum.flat_map(params, &get_table_columns_at_expr(db, table, &1))
  def get_table_columns_at_expr(db, table, _other), do: []
end
