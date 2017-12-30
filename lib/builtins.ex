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

  def count(_anything, data) do
    Enum.count(data)
  end

  def avg(expr, data) do
    sum(expr, data) / count(nil, data)
  end

  def sum(expr, data) do
    Enum.reduce(data, 0, fn row, acc ->
      n = ExoSQL.Expr.run_expr(expr, row)
      {:ok, n} = ExoSQL.Utils.to_number(n)
      acc + n
    end)
  end
end
