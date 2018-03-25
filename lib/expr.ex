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

  def run_expr({:op, {"=", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)

    {r1, r2} = match_types(r1, r2)

    r1 == r2
  end
  def run_expr({:op, {"IS", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)

    r1 === r2
  end
  def run_expr({:op, {">", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    {r1, r2} = match_types(r1, r2)

    case {r1, r2} do
      {%DateTime{}, %DateTime{}} ->
        DateTime.compare(r1, r2) == :gt
      _ ->
        with {:ok, n1} <- to_number(r1),
             {:ok, n2} <- to_number(r2) do
           n1 > n2
        else
          {:error, _} ->
            r1 > r2
        end
    end
  end
  def run_expr({:op, {">=", op1, op2}}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)

    {r1, r2} = match_types(r1, r2)

    case {r1, r2} do
      {%DateTime{}, %DateTime{}} ->
        DateTime.compare(r1, r2) == :eq
      {a, b} ->
        with {:ok, n1} <- to_number(r1),
             {:ok, n2} <- to_number(r2) do
          n1 >= n2
        else
          {:error, _} ->
            a >= b
        end
    end
  end

  def run_expr({:op, {"==", op1, op2}}, cur), do: run_expr({:op, {"=", op1, op2}}, cur)
  def run_expr({:op, {"!=", op1, op2}}, cur), do: not run_expr({:op, {"=", op1, op2}}, cur)
  def run_expr({:op, {"<", op1, op2}}, cur), do: not run_expr({:op, {">=", op1, op2}}, cur)
  def run_expr({:op, {"<=", op1, op2}}, cur), do: not run_expr({:op, {">", op1, op2}}, cur)

  def run_expr({:op, {"*", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 * n2
  end

  def run_expr({:op, {"/", op1, op2}}, cur) do
    {:ok, n1} = to_number(run_expr(op1, cur))
    {:ok, n2} = to_number(run_expr(op2, cur))

    n1 / n2
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

  def run_expr({:op, {:not, op}}, cur), do: run_expr({:not, op}, cur)

  def run_expr({:not, op}, cur) do
    n = run_expr(op, cur)

    cond do
      n == "" -> true
      n -> false
      true -> true
    end
  end

  def run_expr({:op, {"IN", op1, op2}}, cur) do
    op1 = run_expr(op1, cur)
    op2 = run_expr(op2, cur)

    Enum.any?(op2, fn el2 ->
      {op1, el2} = match_types(op1, el2)
      op1 == el2
    end)
  end

  def run_expr({:op, {"LIKE", op1, op2}}, cur) do
    op1 = run_expr(op1, cur)
    op2 = run_expr(op2, cur)

    like(op1, op2)
  end

  def run_expr({:op, {"ILIKE", op1, op2}}, cur) do
    op1 = run_expr(op1, cur)
    op2 = run_expr(op2, cur)

    like(String.downcase(op1), String.downcase(op2))
  end


  def run_expr({:case, list}, cur) do
    Enum.find_value(list, fn {condition, expr} ->
      case run_expr(condition, cur) do
        "" -> nil
        val ->
          if val do
            run_expr(expr, cur)
          else
            nil
          end
      end
    end)
  end

  def run_expr({:fn, {fun, exprs}}, cur) do
    params = for e <- exprs, do: run_expr(e, cur)
    ExoSQL.Builtins.call_function(fun, params)
  end
  def run_expr({:pass, val}, _cur), do: val

  def run_expr({:lit, val}, _cur), do: val

  def run_expr({:column, n}, cur) when is_number(n) do
    Enum.at(cur, n)
  end

  def run_expr({:list, data}, cur) when is_list(data) do
    Enum.map(data, &(run_expr(&1, cur)))
  end


  def like(str, str), do: true
  def like(str, ""), do: false
  def like(str, "%"), do: true
  def like(str, "%" <> more) do
    # Logger.debug("Like #{inspect {str, "%", more}}")
    length = String.length(str)
    Enum.any?(0..length, fn n ->
      like(String.slice(str, n, length), more)
    end)
  end
  def like(<<_::size(8)>> <> str, "_" <> more), do: like(str, more)

  def like(<<chr::size(8)>> <> str, <<chr::size(8)>> <> more), do: like(str, more)
  def like(str, expr) do
    # Logger.debug("Like #{inspect {str, expr}} -> false")
    false
  end
  @doc """
  Try to return matching types.

  * If any is datetime, return datetimes
  * If any is number, return numbers
  * Otherwise, as is
  """
  def match_types(a, b) do
    case {a, b} do
      {t1, t2} when is_number(t1) and is_number(t2) ->
        {a, b}
      {%DateTime{}, _} ->
        {a, ExoSQL.Builtins.to_datetime(b)}
      {_, %DateTime{}} ->
        {ExoSQL.Builtins.to_datetime(a), b}
      {t1, _} when is_number(t1) ->
        {:ok, t2} = to_number(b)
        {t1, t2}
      {_, t2} when is_number(t2) ->
        {:ok, t1} = to_number(a)
        {t1, t2}
      _other ->
        {a, b}
    end
  end


  @doc ~S"""
  Try to simplify expressions.

  Will return always a valid expression.

  If any subexpression is of any of these types, the expression will be the
  maximum complxity.

  This makes for example to simplify:

  {:list, [lit: 1, lit: 2]} -> {:lit, [1,2]}
  """
  def simplify({:lit, n}), do: {:lit, n}
  def simplify({:op, {op, op1, op2}}) do
    op1 = simplify(op1)
    op2 = simplify(op2)
    case {op1, op2} do
      {{:lit, op1}, {:lit, op2}} ->
        {:lit, run_expr({:op, {op, {:lit, op1}, {:lit, op2}}}, [])}
      _other ->
        {:op, {op, op1, op2}}
    end
  end
  def simplify({:list, list}) do
    list = Enum.map(list, &simplify(&1))
    all_literals = Enum.all?(list, fn
      {:lit, _n} -> true
      _other -> false
    end)
    if all_literals do
      list = Enum.map(list, fn {:lit, n} -> n end)
      {:lit, list}
    else
      {:list, list}
    end
  end
  def simplify(list) when is_list(list) do
    Enum.map(list, &simplify/1)
  end
  def simplify({:op, {:not, op}}) do
    case simplify(op) do
      {:lit, op} ->
        cond do
          op == "" ->
            {:lit, true}
          op ->
            {:lit, false}
          true ->
            {:lit, true}
        end
      other ->
        {:not, other}
    end
  end
  def simplify(other), do: other
end
