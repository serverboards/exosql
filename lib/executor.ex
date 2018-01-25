require Logger

defmodule ExoSQL.Executor do
  @doc ~S"""
  Executes the AST for the query.

  Always returns a ExoSQL.Result and work over them.
  """
  def execute({:select, from, columns}, context) do
    {:ok, %{ columns: rcolumns, rows: rows}} = execute(from, context)
    # Logger.debug("Get #{inspect columns} from #{inspect rcolumns}")

    exprs = Enum.map(columns, &simplify_expr_columns(&1, rcolumns))
    # Logger.debug("From #{inspect {rcolumns, rows}} get #{inspect exprs} / #{inspect columns}")

    rows = Enum.map(rows, fn row ->
      Enum.map(exprs, &ExoSQL.Expr.run_expr(&1, row) )
    end)

    columns = resolve_column_names(columns)

    {:ok, %ExoSQL.Result{ rows: rows, columns: columns}}
  end

  def execute({:execute, {db, table}, quals, columns}, context) do
    {dbmod, context} = context[db]
    case apply(dbmod, :execute, [context, table, quals, columns]) do
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


    expr = simplify_expr_columns(expr, columns)
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

  def execute({:inner_join, table1, table2, expr}, context) do
    {:ok, table1} = execute(table1, context)
    {:ok, table2} = execute(table2, context)

    # Logger.debug("Inner join of\n\n#{inspect table1, pretty: true}\n\n#{inspect table2, pretty: true}\n\n#{inspect expr}")

    columns = table1.columns ++ table2.columns
    # Logger.debug("Columns #{inspect columns}")
    rexpr = simplify_expr_columns(expr, columns)
    rows = Enum.reduce( table1.rows, [], fn row1, acc ->
      nrows = Enum.map( table2.rows, fn row2 ->
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

    sgroups = Enum.map(groups, &simplify_expr_columns(&1, data.columns))
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

  def execute({:table_to_row, from}, context) do
    {:ok, data} = execute(from, context)
    {:ok, %ExoSQL.Result{
      columns: ["group_by"],
      rows: [[data]]
    }}
  end

  def execute(%ExoSQL.Result{} = res, _context), do: {:ok, res}
  def execute(%{ rows: rows, columns: columns}, _context), do: {:ok, %ExoSQL.Result{ rows: rows, columns: columns }}


  ## Simplify the column ids to positions on the list of columns, to ease operations.
  def simplify_expr_columns({:column, cn}, names) when is_number(cn) do
    {:column, cn}
  end
  def simplify_expr_columns({:column, cn}, names) do
    i = Enum.find_index(names, &(&1 == cn))
    if i == nil do
      throw {:error, {:not_found, cn, :in, names}}
    end
    {:column, i}
  end
  def simplify_expr_columns({:op, {op, op1, op2}}, names) do
    op1 = simplify_expr_columns(op1, names)
    op2 = simplify_expr_columns(op2, names)
    {:op, {op, op1, op2}}
  end
  def simplify_expr_columns({:fn, {f, params}}, names) do
    params = Enum.map(params, &simplify_expr_columns(&1, names))
    {:fn, {f, params}}
  end
  def simplify_expr_columns(other, _names), do: other

  def simplify_expr_columns_nofn({:column, cn}, names) do
    i = Enum.find_index(names, &(&1 == cn))
    {:column, i}
  end
  def simplify_expr_columns_nofn({:op, {op, op1, op2}}, names) do
    op1 = simplify_expr_columns(op1, names)
    op2 = simplify_expr_columns(op2, names)
    {:op, {op, op1, op2}}
  end
  def simplify_expr_columns_nofn(other, _names), do: other

  def resolve_column_names(columns) do
    Enum.map(columns, fn
      {:column, col} -> col
      other -> "?NONAME"
    end)
  end
end
