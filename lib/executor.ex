require Logger

defmodule ExoSQL.Executor do
  @doc ~S"""
  Executes the AST for the query.

  Always returns a ExoSQL.Result and work over them.
  """
  def execute({:select, {from, columns}}, context) do
    {:ok, %{ columns: rcolumns, rows: rows}} = execute(from, context)

    exprs = Enum.map(columns, &simplify_expr_columns(&1, rcolumns))
    rows = Enum.map(rows, fn row ->
      Enum.map(exprs, &ExoSQL.Expr.run_expr(&1, row) )
    end)

    columns = Enum.map(columns, fn
      {:column, col} -> col
      other -> "?NONAME"
    end)

    {:ok, %ExoSQL.Result{ rows: rows, columns: columns}}
  end

  def execute({:execute, {{db, table}, quals, columns}}, context) do
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
        execute({:select, {result, columns}}, context)
      other -> other
    end
  end

  def execute({:filter, {from, expr}}, context) do
    {:ok, %{ columns: columns, rows: rows }} = execute(from, context)


    expr = simplify_expr_columns(expr, columns)
    rows = Enum.filter(rows, fn row ->
      ExoSQL.Expr.run_expr(expr, row)
    end)
    {:ok, %ExoSQL.Result{ columns: columns, rows: rows}}
  end

  def execute({:cross_join, {table1, table2}}, context) do
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

  def execute(%ExoSQL.Result{} = res, _context), do: {:ok, res}
  def execute(%{ rows: rows, columns: columns}, _context), do: {:ok, %ExoSQL.Result{ rows: rows, columns: columns }}


  def simplify_expr_columns({:column, cn}, names) do
    i = Enum.find_index(names, &(&1 == cn))
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
end
