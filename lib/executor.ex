require Logger

defmodule ExoSQL.Executor do
  @doc ~S"""
  Executes the AST for the query.

  Always returns a {:ok, ExoSQL.Result, newcontext}.
  """
  def execute({:select, from, columns}, context) do
    {:ok, %{columns: rcolumns, rows: rows}, context} = execute(from, context)
    ocontext = context
    # Logger.debug("Get #{inspect columns} from #{inspect rcolumns}. Context: #{inspect context}")
    # Logger.debug("Rows: #{inspect {rcolumns, rows}, pretty: true}")

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:select, columns})}")
    end

    {rows, columns} =
      case Enum.count(rows) do
        0 ->
          columns = resolve_column_names(columns, rcolumns)
          {rows, columns}

        _ ->
          context = Map.put(context, :columns, rcolumns)
          exprs = Enum.map(columns, &ExoSQL.Expr.simplify(&1, context))

          # Logger.debug("From #{inspect rcolumns}\n get #{inspect exprs, pretty: true} /\n #{inspect columns, pretty: true}")
          rows =
            Enum.map(rows, fn row ->
              Enum.map(exprs, &ExoSQL.Expr.run_expr(&1, Map.put(context, :row, row)))
            end)

          columns = resolve_column_names(columns, rcolumns)
          {rows, columns}
      end

    {:ok, %ExoSQL.Result{rows: rows, columns: columns}, ocontext}
  end

  def execute({:distinct, what, from}, context) do
    {:ok, %{columns: columns, rows: rows}, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:distinct, what})}")
    end

    rows =
      case what do
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

    rows =
      Enum.sort(rows)
      |> Enum.uniq_by(fn {key, _} -> key end)
      |> Enum.map(fn {_, row} -> row end)

    # Logger.debug("Get distinct from #{inspect rows}")

    {:ok,
     %{
       columns: columns,
       rows: rows
     }, context}
  end

  def execute({:execute, {:table, {"self", "tables"}}, _quals, _columns}, context) do
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:table, "self", "tables"})}")
    end

    rows =
      Enum.flat_map(context, fn {db, _conf} ->
        {:ok, tables} = ExoSQL.schema(db, context)

        Enum.flat_map(tables, fn table ->
          {:ok, %{columns: columns}} = ExoSQL.schema(db, table, context)

          Enum.map(columns, fn column ->
            [db, table, column]
          end)
        end)
      end)

    # Logger.debug("Rows: #{inspect rows}")

    {:ok,
     %{
       columns: [
         {"self", "tables", "db"},
         {"self", "tables", "table"},
         {"self", "tables", "column"}
       ],
       rows: rows
     }, context}
  end

  def execute({:fn, {function, params}}, context) do
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:fn, {function, params}})}")
    end

    params =
      params
      |> Enum.map(&ExoSQL.Expr.simplify(&1, context))

    res = ExoSQL.Expr.run_expr({:fn, {function, params}}, context)

    res = %ExoSQL.Result{
      columns: [{:tmp, function, function}],
      rows: res.rows
    }

    {:ok, res, context}
  end

  # alias select needs to rename quals and columns to the final, and back to aliased
  def execute({:execute, {:alias, {table, alias_}}, quals, columns}, context) do
    {:ok, res, context} = execute({:execute, table, quals, columns}, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:alias, alias_})}")
    end

    columns =
      Enum.map(res.columns, fn {_db, _table, column} ->
        {:tmp, alias_, column}
      end)

    {:ok,
     %ExoSQL.Result{
       columns: columns,
       rows: res.rows
     }, context}
  end

  def execute({:execute, {:table, {:with, table}}, _quals, columns}, context) do
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:table, :with, table})}")
    end

    data = context[:with][table]
    column_reselect(data, columns, context)
  end

  def execute({:execute, {:table, {db, table}}, quals, columns}, context) do
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({{:table, {db, table}}, quals, columns})}")
    end

    # Logger.debug("Execute table #{inspect {db, table}}")
    {dbmod, ctx} = context[db]

    quals = quals_with_vars(quals, Map.get(context, "__vars__", %{}))

    scolumns =
      Enum.map(columns, fn {^db, ^table, column} ->
        column
      end)

    executor_res = apply(dbmod, :execute, [ctx, table, quals, scolumns])

    case executor_res do
      {:ok, data} ->
        data = %ExoSQL.Result{
          columns: Enum.map(data.columns, &{db, table, &1}),
          rows: data.rows
        }

        column_reselect(data, columns, context)

      {:error, other} ->
        {:error, {:extractor, {db, table}, other}}
    end
  end

  def execute({:project, from}, context) do
    {:ok, from, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:project})}")
    end

    if Enum.count(from.rows) == 0 do
      # warning, the projection fails and the number of columns is undefined.
      {:ok, from}
    else
      # Logger.debug("Orig #{inspect from}")
      columns = project_columns(from.columns, hd(from.rows))
      rows = Enum.flat_map(from.rows, &project_row(&1))
      # Logger.debug("Final #{inspect {columns, rows}}")
      {:ok,
       %ExoSQL.Result{
         columns: columns,
         rows: rows
       }, context}
    end
  end

  def execute({:filter, from, expr}, context) do
    # Can not use the same simplification as an inner query may require a
    # change from a column to a literal (See "Complex nested SELECT" example)
    # So first one simplification to check if the expr is false, and then the
    # real simplification.
    expr_ = ExoSQL.Expr.simplify(expr, context)

    case expr_ do
      {:lit, false} ->
        if ExoSQL.debug_mode(context) do
          Logger.debug("ExoSQL Executor #{inspect({:filter, false})}")
        end

        {:ok, %ExoSQL.Result{columns: [], rows: []}, context}

      _ ->
        {:ok, %{columns: columns, rows: rows}, context} = execute(from, context)

        if ExoSQL.debug_mode(context) do
          Logger.debug("ExoSQL Executor #{inspect({:filter, expr})}")
        end

        context = Map.put(context, :columns, columns)
        expr = ExoSQL.Expr.simplify(expr, context)

        rows =
          Enum.filter(rows, fn row ->
            context = Map.put(context, :columns, columns)
            ExoSQL.Expr.run_expr(expr, Map.put(context, :row, row))
          end)

        {:ok, %ExoSQL.Result{columns: columns, rows: rows}, context}
    end
  end

  def execute({:cross_join, table1, table2}, context) do
    {:ok, res1, context} = execute(table1, context)
    {:ok, res2, context} = execute(table2, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:cross_join})}")
    end

    rows =
      Enum.flat_map(res1.rows, fn r1 ->
        Enum.map(res2.rows, fn r2 ->
          r1 ++ r2
        end)
      end)

    {:ok,
     %ExoSQL.Result{
       columns: res1.columns ++ res2.columns,
       rows: rows
     }, context}
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

  def execute({:cross_join_lateral, from, expr}, context) do
    {:ok, data, context} = execute(from, context)
    context_orig = context

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:cross_join_lateral})}")
    end

    # Logger.debug("Cross join lateral #{inspect from, pretty: true}")
    # Logger.debug("Cross join lateral #{inspect expr, pretty: true}")
    ncolumns =
      case expr do
        {:fn, {"unnest", [_from]}} ->
          [{:tmp, "unnest", "unnest"}]

        {:fn, {"unnest", [_from | columns]}} ->
          Enum.map(columns, fn {:lit, col} -> {:tmp, "unnest", col} end)

        {:fn, {func, _args}} ->
          [{:tmp, func, func}]

        {:alias, {:fn, {"unnest", [_from]}}, alias_} ->
          [{:tmp, alias_, alias_}]

        {:alias, {:fn, {"unnest", [_from | columns]}}, alias_} ->
          Enum.map(columns, fn {:lit, col} -> {:tmp, alias_, col} end)

        {:alias, {:fn, {_func, _args}}, alias_} ->
          [{:tmp, alias_, alias_}]

        {:select, _expr, cols} ->
          resolve_column_names(cols, Map.get(context, :columns, []))

        _ ->
          data.columns
      end

    #  It accumulative to allow nested cross
    parent_columns = Map.get(context, :parent_columns, []) ++ data.columns
    context = Map.put(context, :parent_columns, parent_columns)

    nrows =
      Enum.flat_map(data.rows, fn row ->
        parent_row = Map.get(context, :parent_row, []) ++ row
        context = Map.put(context, :parent_row, parent_row)
        sexpr = ExoSQL.Expr.simplify(expr, context)

        res =
          case sexpr do
            {:select, _, _} = select ->
              {:ok, data, _context} = execute(select, context)
              data.rows

            sexpr ->
              ExoSQL.Expr.run_expr(sexpr, context)
          end

        # Logger.debug("Expression is #{inspect expr, pretty: true}:\n---\n#{inspect res, pretty: true}")

        case res do
          res when is_list(res) ->
            res
            |> Enum.map(fn
              nrow when is_list(nrow) ->
                nrow ++ row

              other ->
                [other] ++ row
            end)

          %{columns: _rcolumns, rows: rrows} ->
            rrows |> Enum.map(&(&1 ++ row))

          other ->
            [other] ++ row
        end
      end)

    result = %ExoSQL.Result{
      columns: ncolumns ++ data.columns,
      rows: nrows
    }

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:cross_join_lateral_end})}")
    end

    # Logger.debug("Got result #{inspect result, pretty: true}")
    {:ok, result, context_orig}
  end

  def execute({:group_by, from, groups}, context) do
    {:ok, data, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:group_by, groups})}")
    end

    context = Map.put(context, :columns, data.columns)
    sgroups = Enum.map(groups, &ExoSQL.Expr.simplify(&1, context))

    rows =
      Enum.reduce(data.rows, %{}, fn row, acc ->
        set = Enum.map(sgroups, &ExoSQL.Expr.run_expr(&1, Map.put(context, :row, row)))

        # Logger.debug("Which set for #{inspect row} by #{inspect sgroups}/#{inspect groups} (#{inspect data.columns}): #{inspect set}")
        Map.put(acc, set, [row | Map.get(acc, set, [])])
      end)
      |> Enum.map(fn {group, row} ->
        table = %ExoSQL.Result{
          columns: data.columns,
          rows: row
        }

        group ++ [table]
      end)

    columns = resolve_column_names(groups, data.columns) ++ ["group_by"]
    # Logger.debug("Grouped rows: #{inspect columns}\n #{inspect rows, pretty: true}")

    {:ok,
     %ExoSQL.Result{
       columns: columns,
       rows: rows
     }, context}
  end

  def execute({:order_by, type, expr, from}, context) do
    {:ok, data, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:order_by, type, expr})}")
    end

    context2 = Map.put(context, :columns, data.columns)

    expr =
      case expr do
        {:lit, n} ->
          {:column, n}

        _other ->
          ExoSQL.Expr.simplify(expr, context2)
      end

    rows =
      if type == :asc do
        Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, Map.put(context, :row, &1)))
      else
        Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, Map.put(context, :row, &1)), &>=/2)
      end

    {:ok,
     %ExoSQL.Result{
       columns: data.columns,
       rows: rows
     }, context}
  end

  def execute({:table_to_row, from}, context) do
    {:ok, data, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:table_to_row})}")
    end

    {:ok,
     %ExoSQL.Result{
       columns: ["group_by"],
       rows: [[data]]
     }, context}
  end

  # alias of fn renames the table and the column inside
  def execute({:alias, {:fn, {function, params}}, alias_}, context) do
    params =
      params
      |> Enum.map(&ExoSQL.Expr.simplify(&1, context))

    res = ExoSQL.Expr.run_expr({:fn, {function, params}}, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:alias, {:fn, {function, params}}, alias_})}")
    end

    columns =
      Enum.map(res.columns, fn _column ->
        {:tmp, alias_, alias_}
      end)

    {:ok,
     %ExoSQL.Result{
       columns: columns,
       rows: res.rows
     }, context}
  end

  def execute({:alias, from, alias_}, context) do
    {:ok, data, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:alias, alias_})}")
    end

    columns =
      Enum.map(data.columns, fn {_db, _table, column} ->
        {:tmp, alias_, column}
      end)

    # Logger.debug("Set alias for #{inspect alias_} #{inspect data} -> #{inspect columns}")
    {:ok,
     %ExoSQL.Result{
       columns: columns,
       rows: data.rows
     }, context}
  end

  def execute({:offset, offset, from}, context) do
    {:ok, data, context} = execute(from, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:offset, offset})}")
    end

    rows = Enum.drop(data.rows, offset)

    {:ok,
     %ExoSQL.Result{
       columns: data.columns,
       rows: rows
     }, context}
  end

  def execute({:limit, limit, from}, context) do
    {:ok, data, context} = execute(from, context)
    rows = Enum.take(data.rows, limit)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:limit, limit})}")
    end

    {:ok,
     %ExoSQL.Result{
       columns: data.columns,
       rows: rows
     }, context}
  end

  def execute({:union, froma, fromb}, context) do
    taska = Task.async(fn -> execute(froma, context) end)
    taskb = Task.async(fn -> execute(fromb, context) end)
    {:ok, dataa, _context} = Task.await(taska)
    {:ok, datab, context} = Task.await(taskb)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:union})}")
    end

    if Enum.count(dataa.columns) != Enum.count(datab.columns) do
      {:error, {:union_column_count_mismatch}}
    else
      {:ok,
       %ExoSQL.Result{
         columns: dataa.columns,
         rows: dataa.rows ++ datab.rows
       }, context}
    end
  end

  def execute({:with, {name, plan}, next}, context) do
    {:ok, data, context} = execute(plan, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:with, name})}")
    end

    data = %{data | columns: Enum.map(data.columns, fn {_, _, col} -> {:with, name, col} end)}

    newwith = Map.put(Map.get(context, :with, %{}), name, data)
    context = Map.put(context, :with, newwith)

    execute(next, context)
  end

  def execute(%ExoSQL.Result{} = res, context), do: {:ok, res, context}

  def execute({:crosstab, ctcolumns, query}, context) do
    {:ok, data, context} = execute(query, context)

    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:crosstab, ctcolumns, query})}")
    end

    first_column = hd(data.columns)

    columns =
      case ctcolumns do
        :all_columns ->
          Enum.reduce(data.rows, MapSet.new(), fn [_, name, _], acc ->
            MapSet.put(acc, name)
          end)
          |> MapSet.to_list()
          |> Enum.sort()

        other ->
          other
      end

    columns_ri = Enum.with_index(columns) |> Map.new()

    empty_row = Enum.map(columns, fn _ -> nil end)

    rows =
      Enum.reduce(data.rows, %{}, fn [row, column, value], acc ->
        current = Map.get(acc, row, empty_row)

        ncurrent =
          case Map.get(columns_ri, column, nil) do
            nil ->
              current

            coln ->
              List.replace_at(current, coln, value)
          end

        Map.put(acc, row, ncurrent)
      end)

    columns = [first_column | columns |> Enum.map(&{:tmp, :tmp, &1})]
    rows = rows |> Enum.map(fn {k, v} -> [k | v] end)

    {:ok,
     %{
       columns: columns,
       rows: rows
     }, context}
  end

  def project_row([head]) do
    cond do
      is_list(head) ->
        [head]

      is_map(head) ->
        %{columns: _columns, rows: rows} = head
        rows

      true ->
        [head]
    end
  end

  def project_row([head | rest]) do
    rest_rows = project_row(rest)

    cond do
      is_list(head) ->
        Enum.flat_map(head, fn h -> Enum.map(rest_rows, fn r -> h ++ [r] end) end)

      is_map(head) ->
        %{columns: _columns, rows: rows} = head
        Enum.flat_map(rows, fn h -> Enum.map(rest_rows, fn r -> h ++ [r] end) end)

      true ->
        for r <- rest_rows do
          [head | r]
        end
    end
  end

  def project_columns([chead], [rhead]) do
    # Logger.debug("Project columns #{inspect {chead, rhead}}")
    cond do
      is_list(rhead) ->
        [chead]

      is_map(rhead) ->
        %{columns: columns, rows: _rows} = rhead

        # special case, I inherit the column name from alias or 1. If more columns at inner table, sorry
        column_change? = columns in [[{:tmp, :tmp, "generate_series"}], [{:tmp, :tmp, "unnest"}]]

        # Logger.debug("Change column names #{inspect chead} #{inspect columns} #{inspect column_change?}")
        if column_change? do
          [chead]
        else
          columns
        end

      true ->
        [chead]
    end
  end

  def project_columns([chead | crest], [rhead | rrest]) do
    rest_columns = project_columns(crest, rrest)

    me_column =
      cond do
        is_list(chead) ->
          [chead]

        is_map(rhead) ->
          %{columns: columns, rows: _rows} = rhead
          columns

        true ->
          [chead]
      end

    me_column ++ rest_columns
  end

  def execute_join(table1, table2, expr, context, no_match_strategy) do
    {:ok, res1, context} = execute(table1, context)

    # calculate extraquals with the {:in, "id", [...]} form, or none and
    # do a full query
    # if it follows the canonical form, then all OK
    table2 =
      case table2 do
        {:execute, from2, quals2, columns2} ->
          extraquals = get_extra_quals(res1, expr, context)

          {:execute, from2, quals2 ++ extraquals, columns2}

        # sorry no qual optimization yet TODO
        other ->
          other
      end

    # Now we get the final table2. As always if the quals are ignored it is just
    # less efficient.
    {:ok, res2, context} = execute(table2, context)

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
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:join_loop, expr, no_match_strategy})}")
    end

    columns = res1.columns ++ res2.columns
    context = Map.put(context, :columns, columns)
    rexpr = ExoSQL.Expr.simplify(expr, context)
    empty_row2 = Enum.map(res2.columns, fn _ -> nil end)
    left_match = no_match_strategy == :left

    # Logger.debug("Columns #{inspect columns}")
    rows =
      Enum.reduce(res1.rows, [], fn row1, acc ->
        nrows =
          Enum.map(res2.rows, fn row2 ->
            row = row1 ++ row2

            if ExoSQL.Expr.run_expr(rexpr, Map.put(context, :row, row)) do
              row
            else
              nil
            end
          end)
          |> Enum.filter(&(&1 != nil))

        nrows =
          if nrows == [] and left_match do
            [row1 ++ empty_row2]
          else
            nrows
          end

        # Logger.debug("Test row #{inspect nrow} #{inspect rexpr}")
        nrows ++ acc
      end)

    # Logger.debug("Result #{inspect no_match_strategy} #{inspect rows, pretty: true}")

    {:ok,
     %ExoSQL.Result{
       columns: columns,
       rows: rows
     }, context}
  end

  def execute_join_hashmap(res1, res2, expr, context, no_match_strategy) do
    if ExoSQL.debug_mode(context) do
      Logger.debug("ExoSQL Executor #{inspect({:join_hashmap, expr, no_match_strategy})}")
    end

    left_match = no_match_strategy == :left

    case hashmap_decompose_expr(res1.columns, expr) do
      {expra, exprb} ->
        expra = ExoSQL.Expr.simplify(expra, Map.put(context, :columns, res1.columns))
        exprb = ExoSQL.Expr.simplify(exprb, Map.put(context, :columns, res2.columns))

        mapb =
          Enum.reduce(res2.rows, %{}, fn row, acc ->
            {_, map} =
              Map.get_and_update(acc, ExoSQL.Expr.run_expr(exprb, Map.put(context, :row, row)), fn
                nil -> {nil, [row]}
                list -> {nil, [row | list]}
              end)

            map
          end)

        empty_row2 = Enum.map(res2.columns, fn _ -> nil end)

        rows =
          Enum.reduce(res1.rows, [], fn row1, acc ->
            vala = ExoSQL.Expr.run_expr(expra, Map.put(context, :row, row1))
            rows2 = Map.get(mapb, vala, [])

            nrows =
              if rows2 == [] and left_match do
                [row1 ++ empty_row2]
              else
                Enum.map(rows2, fn row2 ->
                  row1 ++ row2
                end)
              end

            nrows ++ acc
          end)

        columns = res1.columns ++ res2.columns

        {:ok,
         %ExoSQL.Result{
           columns: columns,
           rows: rows
         }, context}

      _ ->
        Logger.debug("Cant decompose expr, use loop strategy (#{inspect(expr)})")
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
      [op1, op, {:var, variable}] ->
        [op1, op, vars[variable]]

      other ->
        other
    end)
  end

  # common data that given a result, reorders the columns as required
  def column_reselect(data, columns, context) do
    case data do
      %{columns: ^columns, rows: _rows} ->
        if ExoSQL.debug_mode(context) do
          Logger.debug("ExoSQL Executor #{inspect({:column_reselect, :equal})}")
        end

        {:ok, data, context}

      %{columns: rcolumns, rows: rows} ->
        if ExoSQL.debug_mode(context) do
          Logger.debug("ExoSQL Executor #{inspect({:column_reselect, rcolumns, columns})}")
        end

        new_order = Enum.map(columns, fn col -> Enum.find_index(rcolumns, &(&1 == col)) end)

        rows =
          Enum.map(rows, fn row ->
            Enum.map(new_order, &Enum.at(row, &1))
          end)

        {:ok,
         %ExoSQL.Result{
           columns: columns,
           rows: rows
         }, context}
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
    res1_contains_a =
      Enum.find(res1.columns, fn
        ^a -> true
        _ -> false
      end)

    {idf, idt} =
      if res1_contains_a do
        {a, b}
      else
        {b, a}
      end

    ids = ExoSQL.Expr.simplify({:column, idf}, Map.put(context, :columns, res1.columns))
    # Logger.debug("From ltable get #{inspect idf} #{inspect ids}")
    inids =
      Enum.reduce(res1.rows, [], fn row, acc ->
        [ExoSQL.Expr.run_expr(ids, Map.put(context, :row, row)) | acc]
      end)
      |> Enum.uniq()

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
    # to keep the table name
    [{_db, table, _column}] = resolve_column_names([oldcol], pcolumns, 1)
    [{:tmp, table, name} | resolve_column_names(rest, pcolumns, count + 1)]
  end

  defp resolve_column_names([_other | rest], pcolumns, count) do
    [{:tmp, :tmp, "col_#{count}"} | resolve_column_names(rest, pcolumns, count + 1)]
  end

  defp resolve_column_names([], _pcolumns, _count), do: []
end
