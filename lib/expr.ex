require Logger

defmodule ExoSQL.Expr do
  @moduledoc """
  Expression executor.

  Requires a simplified expression from `ExoSQL.Expr.simplify` that converts
  columns names to column positions, and then use as:

    ```
    iex> context = %{ row: [1,2,3,4,5] }
    iex> expr = {:op, {"*", {:column, 1}, {:column, 2}}}
    iex> ExoSQL.Expr.run_expr(expr, context)
    6
    ```
  """

  import ExoSQL.Utils, only: [to_number: 1]

  def run_expr({:op, {"AND", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)
    r1 && r2
  end

  def run_expr({:op, {"OR", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)
    r1 || r2
  end

  def run_expr({:op, {"=", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)

    {r1, r2} = match_types(r1, r2)

    is_equal(r1, r2)
  end

  def run_expr({:op, {"IS", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)

    r1 === r2
  end

  def run_expr({:op, {">", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)
    {r1, r2} = match_types(r1, r2)

    is_greater(r1, r2)
  end

  def run_expr({:op, {">=", op1, op2}}, context) do
    r1 = run_expr(op1, context)
    r2 = run_expr(op2, context)

    {r1, r2} = match_types(r1, r2)
    is_greater_or_equal(r1, r2)
  end

  def run_expr({:op, {"==", op1, op2}}, context), do: run_expr({:op, {"=", op1, op2}}, context)

  def run_expr({:op, {"!=", op1, op2}}, context),
    do: not run_expr({:op, {"=", op1, op2}}, context)

  def run_expr({:op, {"<", op1, op2}}, context),
    do: not run_expr({:op, {">=", op1, op2}}, context)

  def run_expr({:op, {"<=", op1, op2}}, context),
    do: not run_expr({:op, {">", op1, op2}}, context)

  def run_expr({:op, {"*", op1, op2}}, context) do
    op1 = run_expr(op1, context)
    op2 = run_expr(op2, context)

    case {op1, op2} do
      {{:range, {starta, enda}}, {:range, {startb, endb}}} ->
        if enda < startb or endb < starta do
          nil
        else
          {:range, {ExoSQL.Builtins.greatest(starta, startb), ExoSQL.Builtins.least(enda, endb)}}
        end

      _ ->
        {:ok, n1} = to_number(op1)
        {:ok, n2} = to_number(op2)

        n1 * n2
    end
  end

  def run_expr({:op, {"/", op1, op2}}, context) do
    {:ok, n1} = to_number(run_expr(op1, context))
    {:ok, n2} = to_number(run_expr(op2, context))

    n1 / n2
  end

  def run_expr({:op, {"%", op1, op2}}, context) do
    {:ok, n1} = to_number(run_expr(op1, context))
    {:ok, n2} = to_number(run_expr(op2, context))

    rem(n1, n2)
  end

  def run_expr({:op, {"+", op1, op2}}, context) do
    {:ok, n1} = to_number(run_expr(op1, context))
    {:ok, n2} = to_number(run_expr(op2, context))

    n1 + n2
  end

  def run_expr({:op, {"-", op1, op2}}, context) do
    {:ok, n1} = to_number(run_expr(op1, context))
    {:ok, n2} = to_number(run_expr(op2, context))

    n1 - n2
  end

  def run_expr({:op, {"||", op1, op2}}, context) do
    s1 = to_string(run_expr(op1, context))
    s2 = to_string(run_expr(op2, context))

    s1 <> s2
  end

  def run_expr({:op, {:not, op}}, context), do: run_expr({:not, op}, context)

  def run_expr({:not, op}, context) do
    n = run_expr(op, context)

    cond do
      n == "" -> true
      n -> false
      true -> true
    end
  end

  def run_expr({:op, {"IN", op1, op2}}, context) do
    op1 = run_expr(op1, context)
    op2 = run_expr(op2, context)

    case op2 do
      op2 when is_list(op2) ->
        Enum.any?(op2, fn el2 ->
          {op1, el2} = match_types(op1, el2)
          op1 == el2
        end)

      {:range, {start, end_}} ->
        op1 >= start and op1 <= end_

      other ->
        throw({:invalid_argument, {:in, other}})
    end
  end

  def run_expr({:op, {"LIKE", op1, op2}}, context) do
    op1 = run_expr(op1, context)
    op2 = run_expr(op2, context)

    like(op1, op2)
  end

  def run_expr({:op, {"ILIKE", op1, op2}}, context) do
    op1 = run_expr(op1, context)
    op2 = run_expr(op2, context)

    like(String.downcase(op1), String.downcase(op2))
  end

  def run_expr({:case, list}, context) do
    Enum.find_value(list, fn {condition, expr} ->
      case run_expr(condition, context) do
        "" ->
          nil

        val ->
          if val do
            run_expr(expr, context)
          else
            nil
          end
      end
    end)
  end

  def run_expr({:fn, {fun, exprs}}, context) do
    params = for e <- exprs, do: run_expr(e, context)
    ExoSQL.Builtins.call_function(fun, params)
  end

  def run_expr({:pass, val}, _context), do: val

  def run_expr({:lit, val}, _context), do: val

  def run_expr({:column, n}, %{row: row}) when is_number(n) do
    Enum.at(row, n)
  end

  def run_expr({:select, query}, context) do
    context = Map.put(context, :parent_row, context[:row])
    context = Map.put(context, :parent_columns, context[:columns])
    {:ok, res} = ExoSQL.Executor.execute(query, context)

    data =
      case res.rows do
        [[data]] ->
          data

        [_something | _] ->
          throw({:error, {:nested_query_too_many_columns, Enum.count(res.rows)}})

        [] ->
          nil
      end

    data
  end

  def run_expr({:list, data}, context) when is_list(data) do
    Enum.map(data, &run_expr(&1, context))
  end

  def run_expr({:alias, {expr, _}}, context), do: run_expr(expr, context)

  def like(str, str), do: true
  def like(_str, ""), do: false
  def like(_str, "%"), do: true

  def like(str, "%" <> more) do
    # Logger.debug("Like #{inspect {str, "%", more}}")
    length = String.length(str)

    Enum.any?(0..length, fn n ->
      like(String.slice(str, n, length), more)
    end)
  end

  def like(<<_::size(8)>> <> str, "_" <> more), do: like(str, more)

  def like(<<chr::size(8)>> <> str, <<chr::size(8)>> <> more), do: like(str, more)

  def like(_str, _expr) do
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

      {nil, _} ->
        {a, b}

      {_, nil} ->
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
  Unifies is greater comparison
  """
  def is_greater(nil, _b), do: false
  def is_greater(_a, nil), do: true

  def is_greater(%DateTime{} = r1, %DateTime{} = r2) do
    DateTime.compare(r1, r2) == :gt
  end

  def is_greater(r1, r2) do
    with {:ok, n1} <- to_number(r1),
         {:ok, n2} <- to_number(r2) do
      n1 > n2
    else
      {:error, _} ->
        r1 > r2
    end
  end

  @doc ~S"""
  Unifies equal comparison
  """
  def is_equal(%DateTime{} = r1, %DateTime{} = r2) do
    DateTime.compare(r1, r2) == :eq
  end

  def is_equal(r1, r2) when is_binary(r1) and is_binary(r2) do
    r1 == r2
  end

  def is_equal(r1, r2) do
    with {:ok, n1} <- to_number(r1),
         {:ok, n2} <- to_number(r2) do
      n1 == n2
    else
      {:error, _} ->
        r1 == r2
    end
  end

  @doc ~S"""
  Unifies greater or equal comparison
  """
  def is_greater_or_equal(nil, _b), do: false
  def is_greater_or_equal(_a, nil), do: true

  def is_greater_or_equal(%DateTime{} = r1, %DateTime{} = r2) do
    res = DateTime.compare(r1, r2)
    res == :gt or res == :eq
  end

  def is_greater_or_equal(r1, r2) do
    with {:ok, n1} <- to_number(r1),
         {:ok, n2} <- to_number(r2) do
      n1 >= n2
    else
      {:error, _} ->
        r1 >= r2
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
  def simplify({:lit, n}, _context), do: {:lit, n}

  def simplify({:op, {op, op1, op2}}, context) do
    op1 = simplify(op1, context)
    op2 = simplify(op2, context)

    case {op, op1, op2} do
      {"AND", {:lit, false}, _} ->
        {:lit, false}

      {"AND", _, {:lit, false}} ->
        {:lit, false}

      {_, {:lit, op1}, {:lit, op2}} ->
        {:lit, run_expr({:op, {op, {:lit, op1}, {:lit, op2}}}, [])}

      _other ->
        {:op, {op, op1, op2}}
    end
  end

  def simplify({:list, list}, context) do
    list = Enum.map(list, &simplify(&1, context))

    all_literals =
      Enum.all?(list, fn
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

  def simplify(list, context) when is_list(list) do
    Enum.map(list, &simplify(&1, context))
  end

  def simplify({:op, {:not, op}}, context) do
    case simplify(op, context) do
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

  @doc """
  Simplify the column ids to positions on the list of columns, to ease operations.

  This operation is required to change expressions from column names to column
  positions, so that `ExoSQL.Expr` can perform its operations on rows.
  """
  def simplify({:column, cn}, _context) when is_number(cn) do
    {:column, cn}
  end

  def simplify({:alias, {expr, alias_}}, context) do
    {:alias, {simplify(expr, context), alias_}}
  end

  def simplify({:column, cn}, context) do
    names = Map.get(context, :columns, [])
    i = Enum.find_index(names, &(&1 == cn))

    i =
      if i == nil do
        idx = Enum.find_index(Map.get(context, :parent_columns, []), &(&1 == cn))

        if idx != nil do
          val = Enum.at(Map.get(context, :parent_row, []), idx)

          if val do
            {:lit, val}
          else
            {:column, cn}
          end
        else
          nil
        end
      else
        {:column, i}
      end

    # Logger.debug("Simplify #{inspect cn} -> #{inspect i} | #{inspect context}")

    case i do
      nil ->
        # Logger.debug("Unknown column #{inspect cn} | #{inspect context}")
        {:column, cn}

      _other ->
        i
    end
  end

  def simplify({:var, cn}, %{"__vars__" => vars}) do
    {:lit, vars[cn]}
  end

  def simplify({:op, {op, op1, op2}}, context) do
    op1 = simplify(op1, context)
    op2 = simplify(op2, context)
    {:op, {op, op1, op2}}
  end

  def simplify({:fn, {f, params}}, context) do
    params = Enum.map(params, &simplify(&1, context))

    all_literals =
      Enum.all?(params, fn
        {:lit, _} -> true
        _ -> false
      end)

    if all_literals and not ExoSQL.Builtins.can_simplify(f) do
      {:lit, run_expr({:fn, {f, params}}, context)}
    else
      {:fn, {f, params}}
    end
  end

  def simplify({:case, list}, context) do
    list =
      Enum.map(list, fn {e, v} ->
        {simplify(e, context), simplify(v, context)}
      end)

    {:case, list}
  end

  def simplify({:alias, expr, _alias_}, context) do
    simplify(expr, context)
  end

  def simplify(other, _context) do
    other
  end
end
