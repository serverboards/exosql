require Logger

defmodule ExoSQL.Expr do
  @moduledoc """
  Expression executor.

  Requires a simplified expression from `simplify_expr_columns` that converts
  columns names to column positions, and then use as:

    ```
    iex> row = [1,2,3,4,5]
    iex> expr = {:op, {"*", {:column, 1}, {:column, 2}}}
    iex> ExoSQL.Expr.run_expr(expr, row)
    6
    ```
  """

  import ExoSQL.Utils, only: [to_number: 1]

  def run_expr({:op, {"and", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    r1 && r2
  end
  def run_expr({:op, {"AND", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    r1 && r2
  end
  def run_expr({:op, {"or", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    r1 || r2
  end
  def run_expr({:op, {"OR", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    r1 || r2
  end

  def run_expr({:op, {"=", op1, op2}}, cur), do: run_expr(op1, cur) == run_expr(op2, cur)
  def run_expr({:op, {"==", op1, op2}}, cur), do: run_expr(op1, cur) == run_expr(op2, cur)
  def run_expr({:op, {"!=", op1, op2}}, cur), do: run_expr(op1, cur) != run_expr(op2, cur)

  def run_expr({:op, {">", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 > n2
  end
  def run_expr({:op, {"<", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 < n2
  end

  def run_expr({:op, {">=", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 >= n2
  end
  def run_expr({:op, {"<=", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 <= n2
  end

  def run_expr({:op, {"*", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 * n2
  end

  def run_expr({:op, {"+", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 + n2
  end

  def run_expr({:op, {"-", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 - n2
  end

  def run_expr({:op, {"||", op1, op2}}, cur) do
    s1 = to_string(run_expr(op1, cur))
    s2 = to_string(run_expr(op2, cur))

    s1<>s2
  end

  def run_expr({:fn, {fun, exprs}}, cur) when is_atom(fun) do
    params = for e <- exprs, do: run_expr(e, cur)
    apply(ExoSQL.Builtins, fun, params)
  end

  def run_expr({:fn, {fun, exprs}}, cur) do
    params = for e <- exprs, do: run_expr(e, cur)
    apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), params)
  end
  def run_expr({:pass, val}, _cur), do: val

  def run_expr({:lit, val}, _cur), do: val

  def run_expr({:column, n}, cur) when is_number(n) do
    Enum.at(cur, n)
  end
end
