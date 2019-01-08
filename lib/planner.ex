require Logger

defmodule ExoSQL.Planner do
  @doc ~S"""
  Given a query, returns a tree of actions (AST) to perform to resolve the query.

  Each action is `{:plan, {step_function, step_data}}` to call.

  step_data may contain more tagged :plan to call recursively as required.

  They will be performed in reverse order and replaced where it is required.

  For example, it may return for a very simple:

    iex> {:ok, query} = ExoSQL.Parser.parse("SELECT name, price FROM products", %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    iex> plan(query, %{})
    {:ok,
      {:select,
        {:execute, {:table, {"A", "products"}}, [], [{"A", "products", "name"}, {"A", "products", "price"}]}, [
          column: {"A", "products", "name"},
          column: {"A", "products", "price"}]
        }
    }

  Or a more complex:

    iex> query = "SELECT users.name, products.name FROM users, purchases, products WHERE (users.id = purchases.user_id) AND (purchases.product_id = products.id)"
    iex> {:ok, query} = ExoSQL.Parser.parse(query, %{"A" => {ExoSQL.Csv, path: "test/data/csv/"}})
    iex> plan(query, %{})
    {:ok,
      {:select,
        {:filter,
          {:cross_join,
            {:cross_join,
              {:execute, {:table, {"A", "users"}}, [], [
                {"A", "users", "id"},
                {"A", "users", "name"}
              ]},
              {:execute, {:table, {"A", "purchases"}}, [], [
                {"A", "purchases", "user_id"},
                {"A", "purchases", "product_id"}
              ]}},
            {:execute, {:table, {"A", "products"}}, [], [
              {"A", "products", "id"},
              {"A", "products", "name"}
            ]}
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
  def plan(query, context) do
    where = ExoSQL.Expr.simplify(query.where, %{})
    select = ExoSQL.Expr.simplify(query.select, %{})

    all_expressions = [
      where,
      select,
      query.groupby,
      query.from,
      query.join,
      Enum.map(query.orderby, fn {_type, expr} -> expr end),
      Enum.map(query.join, fn
        {:cross_join_lateral, q} -> q
        {_join, {_from, expr}} -> expr
        _other -> []
      end)
    ]

    # Logger.debug("a All expressions: #{inspect query.from} | #{inspect all_expressions}")
    from = plan_execute(query.from, where, all_expressions, context)
    # Logger.debug("a Plan #{inspect from, pretty: true}")

    from_plan =
      if from == nil do
        # just one element
        %ExoSQL.Result{columns: ["?NONAME"], rows: [[1]]}
      else
        from
      end

    # Logger.debug("From plan #{inspect from} -> #{inspect from_plan}")

    join_plan =
      Enum.reduce(query.join, from_plan, fn
        {:cross_join, toplan}, acc ->
          # Logger.debug("b All expressions: #{inspect toplan} | #{inspect all_expressions}")
          from = plan_execute(toplan, all_expressions, context)
          # Logger.debug("b Plan #{inspect from, pretty: true}")
          {:cross_join, acc, from}

        {:cross_join_lateral, toplan}, acc ->
          from = plan_execute(toplan, all_expressions, context)
          {:cross_join_lateral, acc, from}

        {join_type, {toplan, expr}}, acc ->
          from = plan_execute(toplan, expr, all_expressions, context)
          {join_type, acc, from, expr}
      end)

    where_plan =
      if where do
        {:filter, join_plan, where}
      else
        join_plan
      end

    group_plan =
      if query.groupby do
        {:group_by, where_plan, query.groupby}
      else
        where_plan
      end

    # Order can be applied pre select or post select. This is the pre select.
    order_plan =
      query.orderby
      |> Enum.reverse()
      |> Enum.reduce(group_plan, fn
        {_type, {:lit, _n}}, acc ->
          acc

        {type, expr}, acc ->
          {:order_by, type, expr, acc}
      end)

    select =
      Enum.map(select, fn
        {:select, query} ->
          {:ok, plan} = plan(query, context)
          {:select, plan}

        other ->
          other
      end)

    select_plan =
      cond do
        # if grouping, special care on aggregate builtins
        query.groupby ->
          selectg = Enum.map(select, &fix_aggregates_select(&1, Enum.count(query.groupby)))
          {:select, order_plan, selectg}

        # groups full table, do a table to row conversion, and then the ops
        has_aggregates(select) ->
          table_in_a_row = {:table_to_row, order_plan}
          selecta = Enum.map(select, &fix_aggregates_select(&1, 0))
          {:select, table_in_a_row, selecta}

        true ->
          {:select, order_plan, select}
      end

    select_plan =
      if Enum.any?(select, fn
           {:fn, {name, _args}} -> ExoSQL.Builtins.is_projectable(name)
           {:lit, %{columns: _, rows: _}} -> true
           {:alias, {{:lit, %{columns: _, rows: _}}, _}} -> true
           {:alias, {{:fn, {name, _args}}, _}} -> ExoSQL.Builtins.is_projectable(name)
           _other -> false
         end) do
        {:project, select_plan}
      else
        select_plan
      end

    distinct_plan =
      case query.distinct do
        nil ->
          select_plan

        other ->
          {:distinct, other, select_plan}
      end

    crosstab_plan =
      if query.crosstab do
        {:crosstab, query.crosstab, distinct_plan}
      else
        distinct_plan
      end

    # Order can be applied pre select or post select. This is the post select.
    order_plan =
      query.orderby
      |> Enum.reverse()
      |> Enum.reduce(crosstab_plan, fn
        {type, {:lit, n}}, acc ->
          {:order_by, type, {:column, n - 1}, acc}

        {_type, _expr}, acc ->
          acc
      end)

    limit_plan =
      case query.offset do
        nil ->
          order_plan

        number ->
          {:offset, number, order_plan}
      end

    limit_plan =
      case query.limit do
        nil ->
          limit_plan

        number ->
          {:limit, number, limit_plan}
      end

    union_plan =
      case query.union do
        nil ->
          limit_plan

        {:distinct, other} ->
          {:ok, other_plan} = plan(other, context)

          {
            :distinct,
            :all_columns,
            {:union, limit_plan, other_plan}
          }

        {:all, other} ->
          {:ok, other_plan} = plan(other, context)
          {:union, limit_plan, other_plan}
      end

    # On first with it will generate the :with plan, and for further just use
    # it.

    {with_plan, _} =
      query.with
      |> Enum.reverse()
      |> Enum.reduce({union_plan, %{}}, fn
        {name, cols}, {prev_plan, withs} when is_list(cols) ->
          # Logger.debug("Prepare plan cols: #{inspect(name)} #{inspect(cols)}")
          {prev_plan, withs}

        {name, query}, {prev_plan, withs} ->
          case withs[name] do
            nil ->
              # Logger.debug("Prepare plan query: #{inspect(name)} #{inspect(withs)}")
              {:ok, plan} = plan(query, context)
              next_plan = {:with, {name, plan}, prev_plan}
              {next_plan, Map.put(withs, name, [])}

            # no need to plan it
            _columns ->
              {prev_plan, withs}
          end
      end)

    plan = with_plan

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Plan: #{inspect(plan, pretty: true)}")
    end

    {:ok, plan}
  end

  defp plan_execute(
         {:alias, {{:fn, {function, params}}, alias_}},
         _where,
         _all_expressions,
         _context
       ) do
    ex = {:fn, {function, params}}
    {:alias, ex, alias_}
  end

  defp plan_execute({:alias, {{:table, {db, table}}, alias_}}, where, all_expressions, _context) do
    columns = Enum.uniq(get_table_columns_at_expr(:tmp, alias_, all_expressions))
    columns = Enum.map(columns, fn {:tmp, ^alias_, column} -> {db, table, column} end)
    quals = get_quals(:tmp, alias_, where)
    ex = {:execute, {:table, {db, table}}, quals, columns}
    {:alias, ex, alias_}
  end

  defp plan_execute({:alias, {%ExoSQL.Query{} = q, alias_}}, _where, _all_expressions, context) do
    {:ok, ex} = plan(q, context)
    {:alias, ex, alias_}
  end

  defp plan_execute({:table, {db, table}}, where, all_expressions, _context) do
    columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
    quals = get_quals(db, table, where)
    {:execute, {:table, {db, table}}, quals, columns}
  end

  defp plan_execute(nil, _where, _all_expressions, _context) do
    nil
  end

  defp plan_execute(%ExoSQL.Query{} = q, _where, _all_expressions, context) do
    {:ok, q} = plan(q, context)
    q
  end

  defp plan_execute({:fn, f}, _where, _all_expressions, _context) do
    {:fn, f}
  end

  # this are with no _where
  defp plan_execute({:alias, {{:fn, {function, params}}, alias_}}, _all_expressions, _context) do
    ex = {:fn, {function, params}}
    {:alias, ex, alias_}
  end

  defp plan_execute({:fn, _} = func, _all_expressions, _context), do: func

  defp plan_execute({:table, {db, table}}, all_expressions, _context) do
    columns = Enum.uniq(get_table_columns_at_expr(db, table, all_expressions))
    {:execute, {:table, {db, table}}, [], columns}
  end

  defp plan_execute(%ExoSQL.Query{} = q, _all_expressions, context) do
    {:ok, q} = plan(q, context)
    q
  end

  ~S"""
  Gets all the vars referenced in an expression that refer to a given table

  Given a database and table, and an expression, return all columns from that
  {db, table} that are required by those expressions.

  This is used to know which columns to extract from the table.
  """

  defp get_table_columns_at_expr(_db, _table, []) do
    []
  end

  defp get_table_columns_at_expr(db, table, l) when is_list(l) do
    res = Enum.flat_map(l, &get_table_columns_at_expr(db, table, &1))

    # Logger.debug("Get columns at table #{inspect {db, table}} at expr #{inspect l}: #{inspect res}")
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

  defp get_table_columns_at_expr(db, table, {:select, query}) do
    res = get_table_columns_at_expr(db, table, [query.select, query.where, query.join])

    # Logger.debug("Get parents #{inspect {db, table}} from #{inspect query, pretty: true}: #{inspect res}")
    res
  end

  defp get_table_columns_at_expr(db, table, {:case, list}) do
    Enum.flat_map(list, fn
      {e, v} ->
        Enum.flat_map([e, v], &get_table_columns_at_expr(db, table, &1))

      {v} ->
        get_table_columns_at_expr(db, table, v)
    end)
  end

  defp get_table_columns_at_expr(db, table, {:distinct, expr}) do
    get_table_columns_at_expr(db, table, expr)
  end

  defp get_table_columns_at_expr(db, table, {:cross_join_lateral, expr}) do
    get_table_columns_at_expr(db, table, expr)
  end

  defp get_table_columns_at_expr(db, table, {:lateral, expr}) do
    get_table_columns_at_expr(db, table, expr)
  end

  defp get_table_columns_at_expr(_db, _table, _other) do
    []
  end

  ~S"""
  If an aggregate function is found, rewrite it to be a real aggregate

  The way to do it is set as first argument the column with the aggregated table
  and the rest inside `{:pass, op}`, so its the real function that evaluates it
  over the first argument
  """

  defp fix_aggregates_select({:op, {op, op1, op2}}, aggregate_column) do
    op1 = fix_aggregates_select(op1, aggregate_column)
    op2 = fix_aggregates_select(op2, aggregate_column)

    {:op, {op, op1, op2}}
  end

  defp fix_aggregates_select({:fn, {f, args}}, aggregate_column) do
    if ExoSQL.Builtins.is_aggregate(f) do
      args = for a <- args, do: {:pass, a}
      {:fn, {f, [{:column, aggregate_column} | args]}}
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
    Enum.flat_map(expressions, &get_quals(db, table, &1))
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

  defp get_quals(db, table, {:op, {"IN", {:column, {db, table, column}}, {:lit, list}}})
       when is_list(list) do
    [[column, "IN", list]]
  end

  defp get_quals(db, table, {:op, {"AND", op1, op2}}) do
    Enum.flat_map([op1, op2], &get_quals(db, table, &1))
  end

  defp get_quals(_db, _table, _expr), do: []
end
