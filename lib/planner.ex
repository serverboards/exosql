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
    all_expressions = [
      query.where,
      query.select,
      query.groupby,
      Enum.map(query.join, fn {:inner_join, {_from, expr}} -> expr end),
    ]
    # Logger.debug("All expressions: #{inspect all_expressions}")
    from = for {db, table} <- query.from do
      columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
      quals = []
      {:execute, {db, table}, quals, columns}
    end

    from_plan = Enum.reduce((tl from), (hd from), fn fr, acc ->
      {:cross_join, fr, acc}
    end)

    join_plan = Enum.reduce(query.join, from_plan, fn
      {:inner_join, {from, expr}}, acc ->
        # Logger.debug(inspect from)
        {db, table} = from
        columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
        from = {:execute, from, [], columns}
        {:inner_join, acc, from, expr}
    end)

    where_plan = if query.where do
      {:filter, join_plan, query.where}
    else
      join_plan
    end

    group_plan = if query.groupby do
      {:group_by, where_plan, query.groupby}
    else
      where_plan
    end


    select_plan = cond do
      # if grouping, special care on aggregate builtins
      query.groupby ->
        select = Enum.map(query.select, &fix_aggregates_select(&1, Enum.count(query.groupby)))
        {:select, group_plan, select}
      # groups full table, do a table to row conversion, and then the ops
      has_aggregates(query.select) ->
        table_in_a_row = {:table_to_row, group_plan}
        select = Enum.map(query.select, &fix_aggregates_select(&1, 0))
        {:select, table_in_a_row, select}
      true ->
        {:select, group_plan, query.select}
    end


    plan = select_plan

    {:ok, plan}
  end

  def plan(plan, _context), do: plan(plan)

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


  # If an aggregate function is found, rewrite it to be a real aggregate
  # The way to do it is set as first argument the column with the aggregated table
  # and the rest inside `{:pass, op}`, so its the real function that evaluates it
  # over the first argument
  def fix_aggregates_select({:op, {op, op1, op2}}, aggregate_column) do
    op1 = fix_aggregates_select(op1, aggregate_column)
    op2 = fix_aggregates_select(op2, aggregate_column)

    {:op, {op, op1, op2}}
  end
  def fix_aggregates_select({:fn, {f, args}}, aggregate_column) do
    if ExoSQL.Builtins.is_aggregate(f) do
      args = for a <- args, do: {:pass, a}
      {:fn, {f, [ {:column, aggregate_column} | args]}}
    else
      {:fn, {f, args}}
    end
  end
  def fix_aggregates_select(other, _), do: other

  def has_aggregates({:op, {op, op1, op2}}) do
    has_aggregates(op1) or has_aggregates(op2)
  end
  def has_aggregates({:fn, {f, args}}) do
    ExoSQL.Builtins.is_aggregate(f)
  end
  def has_aggregates(l) when is_list(l), do: Enum.any?(l, &has_aggregates/1)
  def has_aggregates(_other), do: false
end