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
    where = ExoSQL.Expr.simplify(query.where, %{})
    select = ExoSQL.Expr.simplify(query.select, %{})

    all_expressions = [
      where,
      select,
      query.groupby,
      Enum.map(query.orderby, fn {_type, expr} -> expr end),
      Enum.map(query.join, fn {_join, {_from, expr}} -> expr end),
    ]
    # Logger.debug("All expressions: #{inspect all_expressions}")
    # Logger.debug("From #{inspect query.from, pretty: true}")
    from = Enum.map(query.from, &plan_execute(&1, where, all_expressions))

    from_plan = if from == [] do
      %ExoSQL.Result{columns: ["?NONAME"], rows: [[1]]} # just one element
    else
      Enum.reduce((tl from), (hd from), fn fr, acc ->
        {:cross_join, fr, acc}
      end)
    end

    join_plan = Enum.reduce(query.join, from_plan, fn
      {join_type, {toplan, expr}}, acc ->
        from = plan_execute(toplan, expr, all_expressions)
        {join_type, acc, from, expr}
    end)

    where_plan = if where do
      {:filter, join_plan, where}
    else
      join_plan
    end

    group_plan = if query.groupby do
      {:group_by, where_plan, query.groupby}
    else
      where_plan
    end

    order_plan = Enum.reduce(query.orderby, group_plan, fn
      {_type, {:lit, _n}}, acc ->
        acc
      {type, expr}, acc ->
        {:order_by, type, expr, acc}
    end)

    select = Enum.map(select, fn
      {:select, query} ->
        {:ok, plan} = plan(query)
        {:select, plan}
      other -> other
    end)

    select_plan = cond do
      # if grouping, special care on aggregate builtins
      query.groupby ->
        select = Enum.map(select, &fix_aggregates_select(&1, Enum.count(query.groupby)))
        {:select, order_plan, select}
      # groups full table, do a table to row conversion, and then the ops
      has_aggregates(select) ->
        table_in_a_row = {:table_to_row, order_plan}
        select = Enum.map(select, &fix_aggregates_select(&1, 0))
        {:select, table_in_a_row, select}
      true ->
        {:select, order_plan, select}
    end

    distinct_plan = case query.distinct do
      nil -> select_plan
      other ->
        {:distinct, other, select_plan}
    end

    order_plan = Enum.reduce(query.orderby, distinct_plan, fn
      {type, {:lit, n}}, acc ->
        {:order_by, type, {:column, n-1}, acc}
      {_type, _expr}, acc ->
        acc
    end)

    limit_plan = case query.offset do
      nil -> order_plan
      number ->
        {:offset, number, order_plan}
    end

    limit_plan = case query.limit do
      nil -> limit_plan
      number ->
        {:limit, number, limit_plan}
    end

    union_plan = case query.union do
      nil -> limit_plan
      {:distinct, other} ->
        {:ok, other_plan} = plan(other)
        {
          :distinct,
          :all_columns,
          {:union, limit_plan, other_plan}
        }
      {:all, other} ->
        {:ok, other_plan} = plan(other)
        {:union, limit_plan, other_plan}
    end


    with_plan = Enum.reduce(query.with, union_plan, fn {name, query}, prev_plan ->
      {:ok, plan} = plan(query)
      {:with, {name, plan}, prev_plan}
    end)

    plan = with_plan

    {:ok, plan}
  end

  def plan(plan, _context), do: plan(plan)

  defp plan_execute({:alias, {{:fn, {function, params}}, alias_}}, _where, _all_expressions) do
    ex = {:fn, {function, params}}
    {:alias, ex, alias_}
  end
  defp plan_execute({:alias, {{db, table}, alias_}}, where, all_expressions) do
    columns = Enum.uniq(get_table_columns_at_expr(:tmp, alias_, all_expressions))
    columns = Enum.map(columns, fn {:tmp, ^alias_, column} -> {db, table, column} end)
    quals = get_quals(:tmp, alias_, where)
    ex = {:execute, {db, table}, quals, columns}
    {:alias, ex, alias_}
  end
  defp plan_execute({:alias, {%ExoSQL.Query{} = q, alias_}}, _where, _all_expressions) do
    {:ok, ex} = plan(q)
    {:alias, ex, alias_}
  end
  defp plan_execute({db, table}, where, all_expressions) do
    columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
    quals = get_quals(db, table, where)
    {:execute, {db, table}, quals, columns}
  end
  defp plan_execute(%ExoSQL.Query{} = q, _where, _all_expressions) do
    {:ok, q} = plan(q)
    q
  end

  # Gets all the vars referenced in an expression that refer to a given table
  defp get_table_columns_at_expr(db, table, l) when is_list(l) do
    # Logger.debug("Get columns at expr #{inspect table} #{inspect l}")
    res = Enum.flat_map(l, &get_table_columns_at_expr(db, table, &1))
    # Logger.debug("res #{inspect res}")
    res
  end
  defp get_table_columns_at_expr(db, table, {:op, {_op, op1, op2}}) do
    get_table_columns_at_expr(db, table, op1) ++ get_table_columns_at_expr(db, table, op2)
  end
  defp get_table_columns_at_expr(db, table, {:column, {db, table, _var} = res}), do: [res]
  defp get_table_columns_at_expr(db, table, {:fn, {_f, params}}) do
    get_table_columns_at_expr(db, table, params)
  end
  defp get_table_columns_at_expr(db, table, {:alias, {expr, _alias}}) do
    get_table_columns_at_expr(db, table, expr)
  end
  defp get_table_columns_at_expr(_db, _table, {:select, query}) do
    {:ok, plan} = plan(query)
    res = get_parent_columns(plan)
    Logger.debug("Get parents from #{inspect plan, pretty: true}: #{inspect res}")
    res
  end
  defp get_table_columns_at_expr(db, table, {:case, list}) do
    Enum.flat_map(list, fn {e,v} ->
      Enum.flat_map([e,v], &get_table_columns_at_expr(db, table, &1))
    end)
  end
  defp get_table_columns_at_expr(db, table, {:distinct, expr}) do
    get_table_columns_at_expr(db, table, expr)
  end
  defp get_table_columns_at_expr(_db, _table, _other), do: []


  defp get_parent_columns({:parent_column, column}) do
    [column]
  end
  defp get_parent_columns(list) when is_list(list) do
    Enum.flat_map(list, &get_parent_columns(&1))
  end
  defp get_parent_columns({:filter, from, expr}) do
    get_parent_columns(from) ++ get_parent_columns(expr)
  end
  defp get_parent_columns({:select, from, columns}) do
    get_parent_columns(from) ++ get_parent_columns(columns)
  end
  defp get_parent_columns({:op, {_op, a, b}}) do
    get_parent_columns(a) ++ get_parent_columns(b)
  end
  defp get_parent_columns(_other) do
    []
  end

  # If an aggregate function is found, rewrite it to be a real aggregate
  # The way to do it is set as first argument the column with the aggregated table
  # and the rest inside `{:pass, op}`, so its the real function that evaluates it
  # over the first argument
  defp fix_aggregates_select({:op, {op, op1, op2}}, aggregate_column) do
    op1 = fix_aggregates_select(op1, aggregate_column)
    op2 = fix_aggregates_select(op2, aggregate_column)

    {:op, {op, op1, op2}}
  end
  defp fix_aggregates_select({:fn, {f, args}}, aggregate_column) do
    if ExoSQL.Builtins.is_aggregate(f) do
      args = for a <- args, do: {:pass, a}
      {:fn, {f, [ {:column, aggregate_column} | args]}}
    else
      args = for a <- args, do: fix_aggregates_select(a, aggregate_column)
      {:fn, {f, args}}
    end
  end
  defp fix_aggregates_select({:alias, {expr, alias_}}, aggregate_column) do
    {:alias, {fix_aggregates_select(expr, aggregate_column), alias_}}
  end
  defp fix_aggregates_select(other, _) do
    other
  end

  defp has_aggregates({:op, {_op, op1, op2}}) do
    has_aggregates(op1) or has_aggregates(op2)
  end
  defp has_aggregates({:alias, {expr, _alias}}) do
    has_aggregates(expr)
  end
  defp has_aggregates({:fn, {f, args}}) do
    if not ExoSQL.Builtins.is_aggregate(f) do
      Enum.reduce(args, false, fn arg, acc ->
        acc or has_aggregates(arg)
      end)
    else
      true
    end
  end
  defp has_aggregates(l) when is_list(l), do: Enum.any?(l, &has_aggregates/1)
  defp has_aggregates(_other), do: false


  defp get_quals(db, table, expressions) when is_list(expressions) do
    Enum.flat_map(expressions, &(get_quals(db, table, &1)))
  end
  defp get_quals(db, table, {:op, {op, {:column, {db, table, column}}, {:lit, value}}}) do
    [[column, op, value]]
  end
  defp get_quals(db, table, {:op, {op, {:lit, value}}, {:column, {db, table, column}}}) do
    [[column, op, value]]
  end
  defp get_quals(db, table, {:op, {op, {:column, {db, table, column}}, {:var, variable}}}) do
    [[column, op, {:var, variable}]]
  end
  defp get_quals(db, table, {:op, {op, {:var, variable}}, {:column, {db, table, column}}}) do
    [[column, op, {:var, variable}]]
  end
  defp get_quals(db, table, {:op, {"IN", {:column, {db, table, column}}, {:lit, list}}}) when is_list(list) do
    [[column, "IN", list]]
  end
  defp get_quals(db, table, {:op, {"AND", op1, op2}}) do
    Enum.flat_map([op1, op2], &(get_quals(db, table, &1)))
  end
  defp get_quals(_db, _table, _expr), do: []
end
