require Logger

defmodule ExoSQL.Expr do
  defp get_item([{k, v} | rest], k), do: v
  defp get_item([{nk, _} | rest], k), do: get_item(rest, k)
  defp get_item([], _k), do: nil

  defp to_number(n) when is_number(n), do: n
  defp to_number(n) when is_binary(n) do # Weak typing
    {n, rem} = if String.contains?(n, ".") do
      Float.parse(n)
    else
      Integer.parse(n)
    end
    if rem == "" do
      {:ok, n}
    else
      {:error, :bad_number}
    end
  end

  def run_expr({:and, op1, op2}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    Logger.debug("and1: #{inspect op1} => #{inspect r1}")
    Logger.debug("and2: #{inspect op2} => #{inspect r2}")
    r1 && r2
  end

  def run_expr({:op, {"=", op1, op2}}, cur), do: run_expr(op1, cur) == run_expr(op2, cur)
  def run_expr({:op, {"==", op1, op2}}, cur), do: run_expr(op1, cur) == run_expr(op2, cur)

  def run_expr({:op, {">", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 > n2
  end

  def run_expr({:op, {">=", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 >= n2
  end

  def run_expr({:op, {"*", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 * n2
  end

  def run_expr({:lit, val}, _cur) when is_binary(val), do: val

  def run_expr({:column, {db, _, _} = k}, cur) when is_binary(db) do
    v = get_item(cur, k)
    v
  end
end
