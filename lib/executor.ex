require Logger

defmodule ExoSQL.Executor do
  @doc ~S"""
  Executes the AST for the query.

  Always returns a ExoSQL.Result and work over them.
  """
  def execute({:select, from, columns}, context) do
    {:ok, %{ columns: rcolumns, rows: rows}} = execute(from, context)
    # Logger.debug("Get #{inspect columns} from #{inspect rcolumns}. Context: #{inspect context}")

    context = Map.put(context, :columns, rcolumns)
    exprs = Enum.map(columns, &ExoSQL.Expr.simplify(&1, context))
    # Logger.debug("From #{inspect rcolumns}\n get #{inspect exprs, pretty: true} /\n #{inspect columns, pretty: true}")

    rows = Enum.map(rows, fn row ->
      Enum.map(exprs, &ExoSQL.Expr.run_expr(&1, Map.put(context, :row, row)))
    end)

    columns = resolve_column_names(columns, rcolumns)

    {:ok, %ExoSQL.Result{ rows: rows, columns: columns}}
  end

  def execute({:distinct, what, from}, context) do
    {:ok, %{ columns: columns, rows: rows}} = execute(from, context)

    rows = case what do
      :all_columns ->
        Enum.map(rows, fn row ->
          {row, row}
        end)
      what ->
        context = Map.put(context, :columns, columns)
        expr = ExoSQL.Expr.simplify(what, context)
        Enum.map(rows, fn row ->
          {ExoSQL.Expr.run_expr(expr, Map.put(context, :row, row)), row}
        end)
    end

    rows = Enum.sort(rows)
      |> Enum.uniq_by(fn {key,_} -> key end)
      |> Enum.map(fn {_, row} -> row end)

    # Logger.debug("Get distinct from #{inspect rows}")

    {:ok, %{
      columns: columns,
      rows: rows
    }}
  end

  def execute({:execute, {"self", "tables"}, _quals, _columns}, context) do
    rows = Enum.flat_map(context, fn {db, _conf} ->
      {:ok, tables} = ExoSQL.schema(db, context)
      Enum.flat_map(tables, fn table ->
        {:ok, %{ columns: columns}} = ExoSQL.schema(db, table, context)
        Enum.map(columns, fn column ->
          [db, table, column]
        end)
      end)
    end)
    # Logger.debug("Rows: #{inspect rows}")

    {:ok, %{
      columns: [{"self", "tables", "db"},{"self", "tables", "table"},{"self", "tables", "column"}],
      rows: rows
    }}
  end
  def execute({:execute, {:fn, {function, params}}, _quals, []}, context) do
    params = params
      |> Enum.map(&ExoSQL.Expr.simplify(&1, context))

    res = ExoSQL.Expr.run_expr({:fn, {function, params}}, context)

    res = %ExoSQL.Result{
      columns: [{:tmp, function, function}],
      rows: res.rows
    }

    {:ok, res}
  end
  # alias select needs to rename quals and columns to the final, and back to aliased
  def execute({:execute, {:alias, {table, alias_}}, quals, columns}, context) do
    {:ok, res} = execute({:execute, table, quals, columns}, context)
    columns = Enum.map(res.columns, fn {_db, _table, column} ->
      {:tmp, alias_, column}
    end)
    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: res.rows
    }}
  end

  def execute({:execute, {:with, table}, _quals, columns}, context) do
    data = context[:with][table]
    column_reselect(data, columns, :with, table, context)
  end

  def execute({:execute, {db, table}, quals, columns}, context) do
    # Logger.debug("#{inspect {db, table, columns, context}}")
    {dbmod, ctx} = context[db]

    quals = quals_with_vars(quals, Map.get(context, "__vars__", %{}))

    scolumns = Enum.map(columns, fn {^db, ^table, column} ->
      column
    end)

    executor_res = apply(dbmod, :execute, [ctx, table, quals, scolumns])
    case executor_res do
      {:ok, data} ->
        column_reselect(data, columns, db, table, context)
      {:error, other} ->
        {:error, {:extractor, {db, table}, other}}
    end
  end

  def execute({:filter, from, expr}, context) do
    {:ok, %{ columns: columns, rows: rows }} = execute(from, context)

    context = Map.put(context, :columns, columns)
    expr = ExoSQL.Expr.simplify(expr, context)
    rows = Enum.filter(rows, fn row ->
      context = Map.put(context, :columns, columns)
      ExoSQL.Expr.run_expr(expr, Map.put(context, :row, row))
    end)
    {:ok, %ExoSQL.Result{ columns: columns, rows: rows}}
  end

  def execute({:cross_join, table1, table2}, context) do
    {:ok, res1} = execute(table1, context)
    {:ok, res2} = execute(table2, context)

    rows = Enum.flat_map(res1.rows, fn r1 ->
      Enum.map(res2.rows, fn r2 ->
        r1 ++ r2
      end)
    end)

    {:ok, %ExoSQL.Result{
      columns: res1.columns ++ res2.columns,
      rows: rows
    }}
  end

  # An inner join does a first loop to get the quals for a single query on the
  # second table with all the join ids (a {:in, "id", [1,2,3,4]} or similar)
  # and then does the full join and filter
  def execute({:inner_join, table1, table2, expr}, context) do
    execute_join(table1, table2, expr, context, :empty)
  end

  # An left outer join is as the inner join, but instead of
  # collapsing the non matching rows, generates a null one for
  # the right side
  def execute({:left_join, table1, table2, expr}, context) do
    execute_join(table1, table2, expr, context, :left)
  end
  def execute({:right_join, table1, table2, expr}, context) do
    execute_join(table2, table1, expr, context, :left)
  end

  def execute({:group_by, from, groups}, context) do
    {:ok, data} = execute(from, context)

    context = Map.put(context, :columns, data.columns)
    sgroups = Enum.map(groups, &ExoSQL.Expr.simplify(&1, context))
    rows = Enum.reduce(data.rows, %{}, fn row, acc ->
      context = Map.put(context, :columns, data.columns)

      set = Enum.map(sgroups, &ExoSQL.Expr.run_expr( &1, Map.put(context, :row, row)))
      # Logger.debug("Which set for #{inspect row} by #{inspect sgroups}/#{inspect groups} (#{inspect data.columns}): #{inspect set}")
      Map.put( acc, set, [row] ++ Map.get(acc, set, []))
    end) |> Enum.map(fn {group,row} ->
      table = %ExoSQL.Result{
        columns: data.columns,
        rows: row
      }
      group ++ [table]
    end)

    columns = resolve_column_names(groups, data.columns) ++ ["group_by"]
    # Logger.debug("Grouped rows: #{inspect columns}\n #{inspect rows, pretty: true}")

    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: rows
    } }
  end

  def execute({:order_by, type, expr, from}, context) do
    {:ok, data} = execute(from, context)

    context = Map.put(context, :columns, data.columns)
    expr = case expr do
      {:column, _} ->
        ExoSQL.Expr.simplify(expr, context)
      {:lit, n} ->
        {:column, n}
    end

    rows = if type == :asc do
      Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, Map.put(context, :row, &1)))
    else
      Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, Map.put(context, :row, &1)), &>=/2)
    end

    {:ok, %ExoSQL.Result{
      columns: data.columns,
      rows: rows,
    }}
  end

  def execute({:table_to_row, from}, context) do
    {:ok, data} = execute(from, context)
    {:ok, %ExoSQL.Result{
      columns: ["group_by"],
      rows: [[data]]
    }}
  end

  # alias of fn renames the table and the column inside
  def execute({:alias, {:fn, {function, params}}, alias_}, context) do
    params = params
      |> Enum.map(&ExoSQL.Expr.simplify(&1, context))

    res = ExoSQL.Expr.run_expr({:fn, {function, params}}, context)
    columns = Enum.map(res.columns, fn _column ->
      {:tmp, alias_, alias_}
    end)
    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: res.rows
    }}
  end
  def execute({:alias, from, alias_}, context) do
    {:ok, data} = execute(from, context)
    columns = Enum.map(data.columns, fn {_db, _table, column} ->
      {:tmp, alias_, column}
    end)
    # Logger.debug("Set alias for #{inspect alias_} #{inspect data} -> #{inspect columns}")
    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: data.rows
    }}
  end

  def execute({:offset, offset, from}, context) do
    {:ok, data} = execute(from, context)
    rows = Enum.drop(data.rows, offset)
    {:ok, %ExoSQL.Result{
      columns: data.columns,
      rows: rows
    }}
  end

  def execute({:limit, limit, from}, context) do
    {:ok, data} = execute(from, context)
    rows = Enum.take(data.rows, limit)
    {:ok, %ExoSQL.Result{
      columns: data.columns,
      rows: rows
    }}
  end

  def execute({:union, froma, fromb}, context) do
    {:ok, dataa} = execute(froma, context)
    {:ok, datab} = execute(fromb, context)

    if Enum.count(dataa.columns) != Enum.count(datab.columns) do
      {:error, {:union_column_count_mismatch}}
    else
      {:ok, %ExoSQL.Result{
        columns: dataa.columns,
        rows: dataa.rows ++ datab.rows
      }}
    end
  end

  def execute({:with, {name, plan}, next}, context) do
    {:ok, data} = execute(plan, context)

    data = %{ data |
      columns: Enum.map(data.columns, fn {_, _, name} -> name end)
    }

    newwith = Map.put(Map.get(context, :with, %{}), name, data)
    context = Map.put(context, :with, newwith)

    execute(next, context)
  end


  def execute(%ExoSQL.Result{} = res, _context), do: {:ok, res}
  def execute(%{ rows: rows, columns: columns}, _context), do: {:ok, %ExoSQL.Result{ rows: rows, columns: columns }}

  def execute_join(table1, table2, expr, context, no_match_strategy) do
    {:ok, res1} = execute(table1, context)

    # calculate extraquals with the {:in, "id", [...]} form, or none and
    # do a full query
    # if it follows the canonical form, then all OK
    table2 = case table2 do
      {:execute, from2, quals2, columns2} ->
        extraquals = get_extra_quals(res1, expr, context)

        {:execute, from2, quals2 ++ extraquals, columns2}
      other -> # sorry no qual optimization yet TODO
        other
    end


    # Now we get the final table2. As always if the quals are ignored it is just
    # less efficient.
    {:ok, res2} = execute(table2, context)

    # Logger.debug("Left join of\n\n#{inspect res1, pretty: true}\n\n#{inspect res2, pretty: true}\n\n#{inspect expr}")


    # Use hashmap or loop strategy depending on size of second table.
    # The size limit is a very arbitrary number that balances the
    # cost of creating an M the map and looking into it N times (N*log M+M*logM),
    # vs looping and checking N*M. N is out of control, so we focus on M.
    if Enum.count(res2.rows) > 50 do
      execute_join_hashmap(res1, res2, expr, context, no_match_strategy)
    else
      execute_join_loop(res1, res2, expr, context, no_match_strategy)
    end
  end

  def execute_join_loop(res1, res2, expr, context, no_match_strategy) do
    columns = res1.columns ++ res2.columns
    context = Map.put(context, :columns, columns)
    rexpr = ExoSQL.Expr.simplify(expr, context)
    empty_row2 = Enum.map(res2.columns, fn _ -> nil end)
    # Logger.debug("Columns #{inspect columns}")
    rows = Enum.reduce( res1.rows, [], fn row1, acc ->
      nrows = Enum.map( res2.rows, fn row2 ->
        row = row1 ++ row2
        if ExoSQL.Expr.run_expr(rexpr, Map.put(context, :row, row)) do
          row
        else
          nil
        end
      end) |> Enum.filter(&(&1 != nil))

      nrows = if nrows == [] and no_match_strategy == :left do
        [row1 ++ empty_row2]
      else
        nrows
      end
      # Logger.debug("Test row #{inspect nrow} #{inspect rexpr}")
      nrows ++ acc
    end)

    # Logger.debug("Result #{inspect no_match_strategy} #{inspect rows, pretty: true}")

    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: rows
    }}
  end

  def execute_join_hashmap(res1, res2, expr, context, no_match_strategy) do
    case hashmap_decompose_expr(res1.columns, expr) do
      {expra, exprb} ->
        expra = ExoSQL.Expr.simplify(expra, Map.put(context, :columns, res1.columns))
        exprb = ExoSQL.Expr.simplify(exprb, Map.put(context, :columns, res2.columns))

        mapb = Enum.reduce(res2.rows, %{}, fn row, acc ->
          {_, map} = Map.get_and_update( acc, ExoSQL.Expr.run_expr(exprb, Map.put(context, :row, row)),  fn
            nil -> {nil, [row]}
            list -> {nil, [row | list]}
          end)
          map
        end)
        empty_row2 = Enum.map(res2.columns, fn _ -> nil end)

        rows = Enum.reduce(res1.rows, [], fn row1, acc ->
          vala = ExoSQL.Expr.run_expr(expra, Map.put(context, :row, row1))
          rows2 = Map.get(mapb, vala, [])
          nrows = if rows2 == [] and no_match_strategy == :left do
            [row1 ++ empty_row2]
          else
            Enum.map(rows2, fn row2 ->
              row1 ++ row2
            end)
          end

          nrows ++ acc
        end)

        columns = res1.columns ++ res2.columns
        {:ok, %ExoSQL.Result{
          columns: columns,
          rows: rows
        }}
      _ ->
        Logger.debug("Cant decompose expr, use loop strategy")
        execute_join_loop(res1, res2, expr, context, no_match_strategy)
    end
  end

  def hashmap_decompose_expr(columns, {:op, {"=", {:column, a}, {:column, b}}}) do
    at_a = Enum.any?(columns, &(&1 == a))
    if at_a do
      {{:column, a}, {:column, b}}
    else
      {{:column, b}, {:column, a}}
    end
  end
  def hashmap_decompose_expr(columns, {:op, {"==", {:column, a}, {:column, b}}}) do
    at_a = Enum.any?(columns, &(&1 == a))
    if at_a do
      {{:column, a}, {:column, b}}
    else
      {{:column, b}, {:column, a}}
    end
  end
  def hashmap_decompose_expr(_, _), do: nil

  def quals_with_vars(quals, vars) do
    Enum.map(quals, fn
      [op1,op,{:var, variable}] ->
        [op1, op, vars[variable]]
      other -> other
    end)
  end

  # common data that given a result, reorders the columns as required
  def column_reselect(data, columns, db, table, context) do
    case data do
      %{ columns: ^columns, rows: rows} ->
        {:ok, %ExoSQL.Result{
          columns: Enum.map(columns, fn c -> {db, table, c} end),
          rows: rows
        }}
      %{ columns: rcolumns, rows: rows} ->
        result = %ExoSQL.Result{
          columns: Enum.map(rcolumns, fn c -> {db, table, c} end),
          rows: rows
        }
        columns = Enum.map(columns, &({:column, &1}))
        execute({:select, result, columns}, context)
      other -> other
    end
  end

  def get_extra_quals(res1, expr, context) do
    case expr do
      {:op, {"=", {:column, a}, {:column, b}}} ->
        get_extra_quals_from_eq(res1, a, b, context)
      {:op, {"==", {:column, a}, {:column, b}}} ->
        get_extra_quals_from_eq(res1, a, b, context)
      _expr ->
        []
    end
  end
  def get_extra_quals_from_eq(res1, a, b, context) do
    res1_contains_a = Enum.find(res1.columns, fn
      ^a -> true
      _ -> false
    end)

    {idf, idt} = if res1_contains_a do
      {a, b}
    else
      {b, a}
    end

    ids = ExoSQL.Expr.simplify({:column, idf}, Map.put(context, :columns, res1.columns))
    # Logger.debug("From ltable get #{inspect idf} #{inspect ids}")
    inids = Enum.reduce(res1.rows, [], fn row, acc ->
      [ExoSQL.Expr.run_expr(ids, Map.put(context, :row, row)) | acc]
    end) |> Enum.uniq
    # Logger.debug("inids #{inspect inids}")
    {_db, _table, columnname} = idt
    [{columnname, "IN", inids}]
  end

  defp resolve_column_names(columns, pcolumns), do: resolve_column_names(columns, pcolumns, 1)

  defp resolve_column_names([{:column, col} | rest], pcolumns, count) when is_tuple(col) do
    [col | resolve_column_names(rest, pcolumns, count + 1)]
  end
  defp resolve_column_names([{:column, col} | rest], pcolumns, count) when is_number(col) do
    col = Enum.at(pcolumns, col)
    [col | resolve_column_names(rest, pcolumns, count + 1)]
  end
  defp resolve_column_names([{:alias, {oldcol, name}} | rest], pcolumns, count) do
    [{_db, table, _column}] = resolve_column_names([oldcol], pcolumns, 1) # to keep the table name
    [{:tmp, table, name} | resolve_column_names(rest, pcolumns, count + 1)]
  end
  defp resolve_column_names([_other | rest], pcolumns, count) do
    [{:tmp, :tmp, "col_#{count}"} | resolve_column_names(rest, pcolumns, count + 1)]
  end
  defp resolve_column_names([], _pcolumns, _count), do: []
end
