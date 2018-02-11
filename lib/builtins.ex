require Logger

defmodule ExoSQL.Builtins do
  @moduledoc """
  Builtin functions.

  There are two categories, normal functions and aggregate functions. Aggregate
  functions receive as first parameter a ExoSQL.Result with a full table,
  and the rest of parameters are the function calling parameters, unsolved.

  These expressions must be first simplified with
  `ExoSQL.executor.simplify_expr_columns` and then executed on the rows with
  `ExoSQL.Expr.run_expr`.
  """
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

  def not_(a) do
    not bool(a)
  end

  def bool(nil), do: false
  def bool(0), do: false
  def bool(""), do: false
  def bool(false), do: false
  def bool(_), do: true

  def lower(s), do: String.downcase(s)
  def upper(s), do: String.upcase(s)

  def to_string_(%DateTime{} = d), do: DateTime.to_iso8601(d)
  def to_string_(s), do: to_string(s)

  def now(), do: DateTime.utc_now()
  def to_datetime(other), do: ExoSQL.DateTime.to_datetime(other)
  def to_timestamp(%DateTime{} = d), do: DateTime.to_unix(d)

  def substr(nil, _skip, _len) do
    ""
  end
  def substr(str, skip, len) do
    str = to_string_(str) # force string

    {:ok, skip} = to_number(skip)
    {:ok, len} = to_number(len)
    if len < 0 do
      String.slice(str, skip, String.length(str) + len - skip)
    else
      String.slice(str, skip, len)
    end
  end
  def substr(str, skip) do
    substr(str, skip, 10_000) # A upper limit on what to return, should be enought
  end

  @doc ~S"""
  Convert datetime to string.

  If no format is given, it is as to_string, which returns the ISO 8601.
  Format allows all substitutions from
  [Timex.format](https://hexdocs.pm/timex/Timex.Format.DateTime.Formatters.Strftime.html),
  for example:

  %d day of month: 00
  %H hour: 00-24
  %m month: 01-12
  %M minute: 00-59
  %s seconds since 1970-01-01
  %S seconds: 00-59
  %Y year: 0000-9999
  %i ISO 8601 format
  %V Week number
  %% %
  """
  def strftime(%DateTime{} = d), do: to_string_(d)
  def strftime(%DateTime{} = d, format), do: ExoSQL.DateTime.strftime(d, format)
  def strftime(other, format), do: strftime(to_datetime(other), format)



  ## Support for JSON Pointer (https://tools.ietf.org/html/rfc6901)
  ## queries.
  def jp(data, expr) do
    path = jp_path(expr)
    Logger.debug("JP #{inspect path} from #{inspect data}")

    [[d]] = data
    jp_walk(d, path)
  end
  defp jp_path({:op, {"/", a, b}}) do
    jp_path(a) ++ jp_path(b)
  end
  defp jp_path({:column, {nil, nil, col}}), do: [col]
  defp jp_walk(%{ key => value}, [key | rest]) do
    jp_walk(value, rest)
  end
  defp jp_walk(%{ key => value}, [key]) do
    value
  end
  defp jp_walk(_, _) do
    nil
  end

  ## This functions should pass the data and the non resolved parameters
  ## For example IF, JP
  def is_no_resolve("jp"), do: true
  def is_no_resolve(_other), do: false

  ### Aggregate functions
  def is_aggregate("count"), do: true
  def is_aggregate("avg"), do: true
  def is_aggregate("sum"), do: true
  def is_aggregate(_other), do: false

  def count(data, _) do
  # Logger.debug("Count #{inspect data}")
    Enum.count(data.rows)
  end

  def avg(data, expr) do
  # Logger.debug("Avg of #{inspect data} by #{inspect expr}")
    sum(data, expr) / count(data, nil)
  end

  def sum(data, expr) do
  # Logger.debug("Sum of #{inspect data} by #{inspect expr}")
    expr = ExoSQL.Executor.simplify_expr_columns(expr, data.columns, nil)
    # Logger.debug("Simplified expression #{inspect expr}")
    Enum.reduce(data.rows, 0, fn row, acc ->
      n = ExoSQL.Expr.run_expr(expr, row)
      {:ok, n} = ExoSQL.Utils.to_number(n)
      acc + n
    end)
  end


end
