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

  @doc ~S"""
  Convert datetime to string.

  If no format is given, it is as to_string, which returns the ISO 8601.
  Format allows these substitutions (subset from SQLITE):

  %d day of month: 00
  %H hour: 00-24
  %m month: 01-12
  %M minute: 00-59
  %s seconds since 1970-01-01
  %S seconds: 00-59
  %Y year: 0000-9999
  %i ISO 8601 format
  %% %
  """
  def strftime(%DateTime{} = d), do: to_string_(d)
  def strftime(%DateTime{} = d, format), do: ExoSQL.DateTime.strftime(d, format)
  def strftime(other, format), do: strftime(to_datetime(other), format)


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
