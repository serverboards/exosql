require Logger

defmodule ExoSQL.Executor do
  @doc ~S"""
  Executes the AST for the query.

  Always returns a ExoSQL.Result and work over them.
  """
  def execute({:select, from, columns}, context) do
    {:ok, %{ columns: rcolumns, rows: rows}} = execute(from, context)
    # Logger.debug("Get #{inspect columns} from #{inspect rcolumns}. Context: #{inspect context}")

    exprs = Enum.map(columns, &simplify_expr_columns(&1, rcolumns, context["__vars__"]))
    # Logger.debug("From #{inspect {rcolumns, rows}} get #{inspect exprs} / #{inspect columns}")

    rows = Enum.map(rows, fn row ->
      Enum.map(exprs, &ExoSQL.Expr.run_expr(&1, row) )
    end)

    columns = resolve_column_names(columns)

    {:ok, %ExoSQL.Result{ rows: rows, columns: columns}}
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
  def execute({:execute, {db, table}, quals, columns}, context) do
    {dbmod, ctx} = context[db]

    vars = Map.get(context, "__vars__", %{})
    quals = Enum.map(quals, fn
      [op1,op,{:var, variable}] ->
        [op1, op, vars[variable]]
      other -> other
    end)

    scolumns = Enum.map(columns, fn {^db, ^table, column} ->
      column
    end)

    case apply(dbmod, :execute, [ctx, table, quals, scolumns]) do
      {:ok, %{ columns: ^columns, rows: rows}} ->
        {:ok, %ExoSQL.Result{
          columns: Enum.map(columns, fn c -> {db, table, c} end),
          rows: rows
        }}
      {:ok, %{ columns: rcolumns, rows: rows}} ->
        result = %ExoSQL.Result{
          columns: Enum.map(rcolumns, fn c -> {db, table, c} end),
          rows: rows
        }
        columns = Enum.map(columns, &({:column, &1}))
        execute({:select, result, columns}, context)
      other -> other
    end
  end

  def execute({:filter, from, expr}, context) do
    {:ok, %{ columns: columns, rows: rows }} = execute(from, context)


    expr = simplify_expr_columns(expr, columns, context["__vars__"])
    rows = Enum.filter(rows, fn row ->
      ExoSQL.Expr.run_expr(expr, row)
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
    {:ok, res1} = execute(table1, context)

    # calculate extraquals with the {:in, "id", [...]} form, or none and
    # do a full query
    # if it follows the canonical form, then all OK
    {:execute, {db2, tablename2}, quals2, columns2} = table2
    extraquals = case expr do
      {:op, {"=", {:column, a}, {:column, b}}} ->
        idf = case a do
          {^db2, ^tablename2, _columnname} -> b
          _other -> a
        end
        ids = simplify_expr_columns({:column, idf}, res1.columns, context["__vars__"])
        # Logger.debug("From ltable get #{inspect idf} #{inspect ids}")
        inids = Enum.reduce(res1.rows, [], fn row, acc ->
          [ExoSQL.Expr.run_expr(ids, row) | acc]
        end) |> Enum.uniq
        # Logger.debug("inids #{inspect inids}")
        {_db, _table, columnname} = idf
        [{:in, columnname, inids}]
      _expr ->
        []
    end
    table2 = {:execute, {db2, tablename2}, quals2 ++ extraquals, columns2}

    # Now we get the final table2. As always if the quals are ignored it is just
    # less efficient.
    {:ok, res2} = execute(table2, context)

    # Logger.debug("Inner join of\n\n#{inspect table1, pretty: true}\n\n#{inspect table2, pretty: true}\n\n#{inspect expr}")

    columns = res1.columns ++ res2.columns
    # Logger.debug("Columns #{inspect columns}")
    rexpr = simplify_expr_columns(expr, columns, context["__vars__"])
    rows = Enum.reduce( res1.rows, [], fn row1, acc ->
      nrows = Enum.map( res2.rows, fn row2 ->
        row = row1 ++ row2
        if ExoSQL.Expr.run_expr(rexpr, row) do
          row
        else
          nil
        end
      end) |> Enum.filter(&(&1 != nil))
      # Logger.debug("Test row #{inspect nrow} #{inspect rexpr}")
      nrows ++ acc
    end)

    # Logger.debug("Result #{inspect rows, pretty: true}")

    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: rows
    }}
  end

  def execute({:group_by, from, groups}, context) do
    {:ok, data} = execute(from, context)

    sgroups = Enum.map(groups, &simplify_expr_columns(&1, data.columns, context["__vars__"]))
    rows = Enum.reduce(data.rows, %{}, fn row, acc ->
      set = Enum.map(sgroups, &ExoSQL.Expr.run_expr( &1, row ))
      # Logger.debug("Which set for #{inspect row} by #{inspect sgroups}/#{inspect groups} (#{inspect data.columns}): #{inspect set}")
      Map.put( acc, set, [row] ++ Map.get(acc, set, []))
    end) |> Enum.map(fn {group,row} ->
      table = %ExoSQL.Result{
        columns: data.columns,
        rows: row
      }
      group ++ [table]
    end)

    columns = resolve_column_names(groups) ++ [{"group_by"}]
    # Logger.debug("Grouped rows: #{inspect columns}\n #{inspect rows, pretty: true}")

    {:ok, %ExoSQL.Result{
      columns: columns,
      rows: rows
    } }
  end

  def execute({:order_by, type, expr, from}, context) do
    {:ok, data} = execute(from, context)

    expr = case expr do
      {:column, _} ->
        simplify_expr_columns(expr, data.columns, context["__vars__"])
      {:lit, n} ->
        {:column, n}
    end

    rows = if type == :asc do
      Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, &1))
    else
      Enum.sort_by(data.rows, &ExoSQL.Expr.run_expr(expr, &1), &>=/2)
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

  def execute(%ExoSQL.Result{} = res, _context), do: {:ok, res}
  def execute(%{ rows: rows, columns: columns}, _context), do: {:ok, %ExoSQL.Result{ rows: rows, columns: columns }}


  @doc """
  Simplify the column ids to positions on the list of columns, to ease operations.

  This operation is required to change expressions from column names to column
  positions, so that `ExoSQL.Expr` can perform its operations on rows.
  """
  def simplify_expr_columns({:column, cn}, _names, _vars) when is_number(cn) do
    {:column, cn}
  end
  def simplify_expr_columns({:column, cn}, names, _vars) do
    i = Enum.find_index(names, &(&1 == cn))
    if i == nil do
      throw {:error, {:not_found, cn, :in, names}}
    end
    {:column, i}
  end
  def simplify_expr_columns({:var, cn}, _names, vars) do
    {:lit, vars[cn]}
  end
  def simplify_expr_columns({:op, {op, op1, op2}}, names, vars) do
    op1 = simplify_expr_columns(op1, names, vars)
    op2 = simplify_expr_columns(op2, names, vars)
    {:op, {op, op1, op2}}
  end
  def simplify_expr_columns({:fn, {f, params}}, names, vars) do
    params = if ExoSQL.Builtins.is_no_resolve(f) do
      params
    else
      Enum.map(params, &simplify_expr_columns(&1, names, vars))
    end
    {:fn, {f, params}}
  end
  def simplify_expr_columns(other, _names, _vars), do: other

  # def simplify_expr_columns_nofn({:column, cn}, names) do
  #   i = Enum.find_index(names, &(&1 == cn))
  #   {:column, i}
  # end
  # def simplify_expr_columns_nofn({:op, {op, op1, op2}}, names) do
  #   op1 = simplify_expr_columns(op1, names)
  #   op2 = simplify_expr_columns(op2, names)
  #   {:op, {op, op1, op2}}
  # end
  # def simplify_expr_columns_nofn(other, _names), do: other

  def resolve_column_names(columns) do
    Enum.map(columns, fn
      {:column, col} -> col
      _other -> "?NONAME"
    end)
  end
end
