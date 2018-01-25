require Logger

defmodule ExoSQL.Builtins do
  import ExoSQL.Utils, only: [to_number: 1, to_float: 1]

  def round(n, r) do
    {:ok, n} = to_float(n)
    {:ok, r} = to_number(r)

    Float.round(n, r)
  end

  def concat(a, b) do
    a = to_string(a)
    b = to_string(b)

    a <> b
  end


  def is_aggregate("count"), do: true
  def is_aggregate("avg"), do: true
  def is_aggregate("sum"), do: true
  def is_aggregate(_other), do: false

  def count(data) do
    # Logger.debug("Count #{inspect data}")
    Enum.count(data.rows)
  end

  def avg(data, expr) do
    # Logger.debug("Avg of #{inspect data} by #{inspect expr}")
    sum(data, expr) / count(data)
  end

  def sum(data, expr) do
    # Logger.debug("Sum of #{inspect data} by #{inspect expr}")
    expr = ExoSQL.Executor.simplify_expr_columns(expr, data.columns)
    # Logger.debug("Simplified expression #{inspect expr}")
    Enum.reduce(data.rows, 0, fn row, acc ->
      n = ExoSQL.Expr.run_expr(expr, row)
      {:ok, n} = ExoSQL.Utils.to_number(n)
      acc + n
    end)
  end
end
