require Logger

defmodule ExoSQL.Executor do
  # The where filtering has passed, run the expressions for the select, and returns this row
  defp execute_select_where(select, [], [], cur) do
    [for s <- select do
      ExoSQL.Expr.run_expr(s, cur)
    end]
  end


  # I have a full row at cur, as a map with the header name as key, perform the where filtering, only one expr always
  defp execute_select_where(select, [expr], [], cur) do
    # Logger.debug("Check row #{inspect cur} | #{inspect expr}")
    if ExoSQL.Expr.run_expr(expr, cur) do
      execute_select_where(select, [], [], cur)
    else
      []
    end
  end

  # for each table, get each of the rows, and use as cur, then do the rest of tables
  defp execute_select_where(select, where, [head | rest ], cur) do
    %{ headers: headers, rows: rows} = head
    Enum.flat_map(rows, fn row ->
      myrows = Enum.zip(headers, row)
      execute_select_where(select, where, rest, cur ++ myrows)
    end)
  end

  def do_group_by(query, cjt, rows) do
    groupby = for expr <- query.groupby, do: ExoSQL.Parser.convert_column_names(expr, cjt.headers)
    groups = Enum.reduce(rows, %{}, fn row, acc ->
      key = Enum.map(groupby, &(ExoSQL.Expr.run_expr( &1, row )))
      # Logger.debug("Group by #{inspect key}")
      Map.put(acc, key, [row] ++ Map.get(acc, key, []))
    end)
    # Logger.debug("Grouped: #{inspect groups}")

    # now I have other headers, and other select behaviour
    headers = for {:column, col} <- query.groupby, do: col
    # Logger.debug("Prepare headers for #{inspect headers}")
    #headers = for expr <- query.groupby, do: convert_column_names(expr, headers)
    # Logger.debug("New headers are: #{inspect headers}")
    select = for expr <- query.select do
      nn = ExoSQL.Parser.convert_column_names_nofn(expr, headers)
      # Logger.debug("#{inspect {expr, headers, nn}}")
      nn
    end
    # Logger.debug("New select is: #{inspect select} // #{inspect headers}")

    rows = Enum.map(groups, fn {row, data} ->
      for expr <- select do
        case expr do
          {:fn, {fun, ['*']}} ->
            apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [nil, data])
          {:fn, {fun, [expr]}} ->
            expr = ExoSQL.Parser.convert_column_names(expr, cjt.headers)
            # Logger.debug("Do #{inspect fun} ( #{inspect expr} )  // #{inspect data}")
            apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [expr, data])
          expr ->
            ExoSQL.Expr.run_expr(expr, row)
        end
      end
    end)
    rows
  end

  def execute(query, context) when is_map(context) do
    Logger.debug("Execute query #{inspect query, pretty: true}")
    plan = for {db, table} <- query.from do
      columns = ExoSQL.Parser.get_vars(db, table, query.select)
      quals = []
      {db, table, quals, columns}
    end

    Logger.debug("My plan is #{inspect plan, pretty: true}")

    data = for {db, table, quals, columns} <- plan do
      # Logger.debug("Plan: #{inspect db} ( #{inspect {table, quals, columns}})")
      {dbmod, context} = context[db]
      {:ok, data} = apply(dbmod, :execute, [context, table, quals, columns])

      %{ headers: headers, rows: rows} = data

      headers = for h <- headers, do: {db, table, h}

      %{headers: headers, rows: rows}
    end

    cjt = CrossJoinTables.new(data) # this is an enumerable
    # Logger.debug("rows #{inspect rows, pretty: true}")
    # Logger.debug("Total count: #{Enum.count(rows)}")
    # Logger.debug("Data: #{inspect data}")
    rows = cjt
    rows = if query.where do
      expr = ExoSQL.Parser.convert_column_names(query.where, rows.headers)
      # Logger.debug("expr #{inspect expr}")
      rows = Enum.filter(rows, fn row ->
        # Logger.debug(row)
        ExoSQL.Expr.run_expr(expr, row)
      end)
    else
      rows
    end
    # group by
    rows = if query.groupby do
      do_group_by(query, cjt, rows)
    else
      # aggregates full result
      is_aggretate_no_group = Enum.all?(query.select, fn
        {:fn, {f, _params}} -> ExoSQL.Builtins.is_aggregate(String.downcase(f))
        other -> false
      end)
      if is_aggretate_no_group do
        res = for expr <- query.select do
          {:fn, {fun, [expr]}} = ExoSQL.Parser.convert_column_names(expr, cjt.headers)
          apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [expr, rows])
        end
        [res]
      else
        # just plain old select
        select = for expr <- query.select, do: ExoSQL.Parser.convert_column_names(expr, cjt.headers)
        # Logger.debug(inspect select)
        rows = Enum.map(rows, fn row ->
          for expr <- select do
            ExoSQL.Expr.run_expr(expr, row)
          end
        end)
      end
    end

    # rows = execute_select_where(query.select, query.where, data, [])
    #   |> Enum.filter(&(&1))

    {:ok, %{ headers: query.select, rows: rows }}
  end
end
