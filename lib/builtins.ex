require Logger

defmodule ExoSQL.Builtins do
  @moduledoc """
  Builtin functions.

  There are two categories, normal functions and aggregate functions. Aggregate
  functions receive as first parameter a ExoSQL.Result with a full table,
  and the rest of parameters are the function calling parameters, unsolved.

  These expressions must be first simplified with
  `ExoSQL.Expr.simplify` and then executed on the rows with
  `ExoSQL.Expr.run_expr`.
  """
  import ExoSQL.Utils, only: [to_number: 1, to_float: 1]

  @functions %{
    "round" => {ExoSQL.Builtins, :round},
    "bool" => {ExoSQL.Builtins, :bool},
    "lower" => {ExoSQL.Builtins, :lower},
    "upper" => {ExoSQL.Builtins, :upper},
    "split" => {ExoSQL.Builtins, :split},
    "join" => {ExoSQL.Builtins, :join},
    "to_string" => {ExoSQL.Builtins, :to_string_},
    "to_datetime" => {ExoSQL.Builtins, :to_datetime},
    "to_timestamp" => {ExoSQL.Builtins, :to_timestamp},
    "to_number" => {ExoSQL.Utils, :'to_number!'},
    "substr" => {ExoSQL.Builtins, :substr},
    "now" => {ExoSQL.Builtins, :now},
    "strftime" => {ExoSQL.Builtins, :strftime},
    "format" => {ExoSQL.Builtins, :format},
    "width_bucket" => {ExoSQL.Builtins, :width_bucket},
    "generate_series" => {ExoSQL.Builtins, :generate_series},
    "urlparse" => {ExoSQL.Builtins, :urlparse},
    "jp" => {ExoSQL.Builtins, :jp},
    "json" => {ExoSQL.Builtins, :json},
    "regex" => {ExoSQL.Builtins, :regex},
    "random" => {ExoSQL.Builtins, :random},
    "randint" => {ExoSQL.Builtins, :randint},
    "range" => {ExoSQL.Builtins, :range},
    "greatest" => {ExoSQL.Builtins, :greatest},
    "lowest" => {ExoSQL.Builtins, :lowest},
    "coalesce" => {ExoSQL.Builtins, :coalesce},
    "nullif" => {ExoSQL.Builtins, :nullif},
    "floor" => {ExoSQL.Builtins, :floor},
    "ceil" => {ExoSQL.Builtins, :ceil},
    "datediff" => {ExoSQL.DateTime, :datediff},

    ## Aggregates
    "count" => {ExoSQL.Builtins, :count},
    "sum" => {ExoSQL.Builtins, :sum},
    "avg" => {ExoSQL.Builtins, :avg},
    "max" => {ExoSQL.Builtins, :max_},
    "min" => {ExoSQL.Builtins, :min_},
  }
  def call_function({mod, fun, name}, params) do
    try do
      apply(mod, fun, params)
    rescue
      _excp ->
        # Logger.debug("Exception #{inspect _excp}: #{inspect {{mod, fun}, params}}")
        throw {:function, {name, params}}
    end
  end
  def call_function(name, params) do
    case @functions[name] do
      nil ->
      raise {:unknown_function, name}
    {mod, fun} ->
      try do
        apply(mod, fun, params)
      rescue
        _excp ->
          # Logger.debug("Exception #{inspect _excp}: #{inspect {{mod, fun}, params}}")
          throw {:function, {name, params}}
      end
    end
  end

  def round(n) do
    {:ok, n} = to_float(n)

    Kernel.round(n)
  end
  def round(n, 0) do
    {:ok, n} = to_float(n)

    Kernel.round(n)
  end
  def round(n, "0") do
    {:ok, n} = to_float(n)

    Kernel.round(n)
  end
  def round(n, r) do
    {:ok, n} = to_float(n)
    {:ok, r} = to_number(r)

    Float.round(n, r)
  end
  def random(), do: :rand.uniform()
  def randint(max_) do
    :rand.uniform(max_-1)
  end
  def randint(min_, max_) do
    :rand.uniform(max_ - min_ - 1) + min_ - 1
  end

  def bool(nil), do: false
  def bool(0), do: false
  def bool(""), do: false
  def bool(false), do: false
  def bool(_), do: true

  def lower(nil), do: nil
  def lower({:range, {a, _b}}), do: a
  def lower(s), do: String.downcase(s)

  def upper(nil), do: nil
  def upper({:range, {_a, b}}), do: b
  def upper(s), do: String.upcase(s)

  def to_string_(%DateTime{} = d), do: DateTime.to_iso8601(d)
  def to_string_(s), do: to_string(s)

  def now(), do: Timex.local()
  def now(tz), do: Timex.now(tz)
  def to_datetime(other), do: ExoSQL.DateTime.to_datetime(other)
  def to_datetime(other, mod), do: ExoSQL.DateTime.to_datetime(other, mod)
  def to_timestamp(%DateTime{} = d), do: DateTime.to_unix(d)

  def substr(nil, _skip, _len) do
    ""
  end
  def substr(str, skip, len) do
    str = to_string_(str) # force string

    {:ok, skip} = to_number(skip)
    {:ok, len} = to_number(len)
    if len < 0 do
      String.slice(str, skip, max(0, String.length(str) + len - skip))
    else
      String.slice(str, skip, len)
    end
  end
  def substr(str, skip) do
    substr(str, skip, 10_000) # A upper limit on what to return, should be enought
  end
  def join(str, sep \\ ",") do
    Enum.join(str, sep)
  end
  def split(str, sep) do
    String.split(str, sep)
  end
  def split(str) do
    String.split(str, [", ", ",", " "])
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

  @doc ~S"""
  sprintf style formatting.

  Known interpolations:

  %d - Integer
  %f - Float, 2 digits
  %.Nf - Float N digits
  %k - integer with k, M sufix
  %.k - float with k, M sufix, uses float part
  """
  def format(str), do: ExoSQL.Format.format(str, [])
  def format(str, args) when is_list(args) do
    ExoSQL.Format.format(str, args)
  end

  @doc ~S"""
  Very simple sprintf formatter. Knows this formats:

  * %%
  * %s
  * %d
  * %f (only two decimals)
  * %.{ndec}f
  """
  def format(str, arg1), do: format(str, [arg1])
  def format(str, arg1, arg2), do: format(str, [arg1, arg2])
  def format(str, arg1, arg2, arg3), do: format(str, [arg1, arg2, arg3])
  def format(str, arg1, arg2, arg3, arg4), do: format(str, [arg1, arg2, arg3, arg4])
  def format(str, arg1, arg2, arg3, arg4, arg5), do: format(str, [arg1, arg2, arg3, arg4, arg5])
  def format(str, arg1, arg2, arg3, arg4, arg5, arg6), do: format(str, [arg1, arg2, arg3, arg4, arg5, arg6])
  def format(str, arg1, arg2, arg3, arg4, arg5, arg6, arg7), do: format(str, [arg1, arg2, arg3, arg4, arg5, arg6, arg7])
  def format(str, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8), do: format(str, [arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8])
  def format(str, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9), do: format(str, [arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9])



  @doc ~S"""
  Returns to which bucket it belongs.

  Only numbers, but datetimes can be transformed to unix datetime.
  """
  def width_bucket(n, start_, end_, nbuckets) do
    import ExoSQL.Utils, only: [to_float!: 1, to_number!: 1]

    n = to_float!(n)
    start_ = to_float!(start_)
    end_ = to_float!(end_)
    nbuckets = to_number!(nbuckets)

    bucket = ((n - start_) * nbuckets / (end_- start_))
    bucket = bucket |> Kernel.round

    cond do
      bucket < 0 -> 0
      bucket >= nbuckets -> nbuckets - 1
      true -> bucket
    end
  end


  @doc ~S"""
  Performs a regex match

  May return a list of groups, or a dict with named groups, depending on
  the regex.

  As an optional third parameter it performs a jp query.

  Returns NULL if no match (which is falsy, so can be used for expressions)
  """
  def regex(str, regexs) when is_binary(regexs) do
    regex = Regex.compile!(regexs) # slow. Should have been precompiled (simplify)
    regex(str, regex, String.contains?(regexs, "(?<"))
  end
  def regex(str, %Regex{} = regex, captures) do
    if captures do
      Regex.named_captures(regex, str)
    else
      Regex.run(regex, str)
    end
  end
  def regex(str, regexs, query) when is_binary(regexs) do
    jp(regex(str, regexs), query)
  end

  @doc ~S"""
  Generates a table with the series of numbers as given. Use for histograms
  without holes.
  """
  def generate_series(end_), do: generate_series(1,end_,1)
  def generate_series(start_, end_), do: generate_series(start_,end_,1)
  def generate_series(%DateTime{} = start_, %DateTime{} = end_, days) do
    if start_ > end_ or days <= 0 do
      raise ArgumentError, "Start, end and step invalid. Will never reach end."
    end

    to_add = [days: days]
    rows = ExoSQL.Utils.generate(start_, fn value ->
      if DateTime.compare(value, end_) == :gt do
        :halt
      else
        {[value], Timex.shift(value, to_add)}
      end
    end)

    %{
      columns: ["generate_series"],
      rows: rows
    }
  end
  def generate_series(start_, end_, step) do
    import ExoSQL.Utils, only: [to_number!: 1]
    start_ = to_number!(start_)
    end_ = to_number!(end_)
    step = to_number!(step)

    if step < 0 and start_ < end_ do
      raise ArgumentError, "Start, end and step invalid. Will never reach end."
    end
    if step >= 0 and start_ > end_ do
      raise ArgumentError, "Start, end and step invalid. Will never reach end."
    end

    %{
      columns: ["generate_series"],
      rows: generate_series_range(start_, end_, step)
    }
  end
  defp generate_series_range(current, stop, step) do
    cond do
      step > 0 and current > stop ->
        []
      step < 0 and current < stop ->
        []
      true ->
        [ [current] | generate_series_range(current + step, stop, step)]
    end
  end


  @doc ~S"""
  Parses an URL and return some part of it.

  If not what is provided, returns a JSON object with:
  * host
  * port
  * scheme
  * path
  * query
  * user

  If what is passed, it performs a JSON Pointer search (jp function).

  It must receive a url with scheme://server or the result may not be well
  formed.

  For example, for emails, just use "email://connect@serverboards.io" or
  similar.

  """
  def urlparse(url), do: urlparse(url, nil)
  def urlparse(nil, what), do: urlparse("", what)
  def urlparse(url, what) do
    parsed = URI.parse(url)

    query = case parsed.query do
      nil -> nil
      q -> URI.decode_query(q)
    end

    json = %{
      "host" => parsed.host,
      "port" => parsed.port,
      "scheme" => parsed.scheme,
      "path" => parsed.path,
      "query" => query,
      "user" => parsed.userinfo,
      "domain" => get_domain(parsed.host)
    }

    if what do
      jp(json, what)
    else
      json
    end
  end

  @doc ~S"""
  Gets the domain from the domain name.

  This means "google" from "www.google.com" or "google" from "www.google.co.uk"

  The algorithm disposes the tld (.uk) and the skips unwanted names (.co).
  Returns the first thats rest, or a default that is originally the full domain
  name or then each disposed part.
  """
  def get_domain(nil), do: nil
  def get_domain(hostname) do
    [_tld | rparts] = hostname |> String.split(".") |> Enum.reverse

    # always remove last part
    get_domainr(rparts, hostname)
  end

  # list of strings that are never domains.
  defp get_domainr([ head | rest], candidate) do
    nodomains = ~w(com org net www co)
    if head in nodomains do
      get_domainr(rest, candidate)
    else
      head
    end
  end
  defp get_domainr([], candidate), do: candidate

  @doc ~S"""
  Performs a JSON Pointer search on JSON data.

  It just uses / to separate keys.
  """
  def jp(nil, _), do: nil
  def jp(json, idx) when is_list(json) and is_number(idx), do: Enum.at(json, idx)
  def jp(json, str) when is_binary(str), do: jp(json, String.split(str, "/"))
  def jp(json, [ head | rest]) when is_list(json) do
    n = ExoSQL.Utils.to_number!(head)
    jp(Enum.at(json, n), rest)
  end
  def jp(json, ["" | rest]), do: jp(json, rest)
  def jp(json, [head | rest]), do: jp(Map.get(json, head, nil), rest)
  def jp(json, []), do: json

  @doc ~S"""
  Convert from a string to a JSON object
  """
  def json(nil), do: nil
  def json(str) when is_binary(str) do
    Poison.decode!(str)
  end

  @doc ~S"""
  Creates a range, which can later be used in:

  * `IN` -- Subset / element contains
  * `*` -- Interesection -> nil if no intersection, the intersected range if any.
  """
  def range(a, b), do: {:range, {a,b}}

  @doc ~S"""
  Get the greatest of arguments
  """
  def greatest(a, nil), do: a
  def greatest(nil, b), do: b
  def greatest(a, b) do
    if a > b do
      a
    else
      b
    end
  end
  def greatest(a, b, c), do: Enum.reduce([a, b, c], nil, &greatest/2)
  def greatest(a, b, c, d), do: Enum.reduce([a, b, c, d], nil, &greatest/2)
  def greatest(a, b, c, d, e), do: Enum.reduce([a, b, c, d, e], nil, &greatest/2)

  @doc ~S"""
  Get the least of arguments.

  Like min, for not for aggregations.
  """
  def least(a, nil), do: a
  def least(nil, b), do: b
  def least(a, b) do
    if a < b do
      a
    else
      b
    end
  end
  def least(a, b, c), do: Enum.reduce([a, b, c], nil, &least/2)
  def least(a, b, c, d), do: Enum.reduce([a, b, c, d], nil, &least/2)
  def least(a, b, c, d, e), do: Enum.reduce([a, b, c, d, e], nil, &least/2)

  @doc ~S"""
  Returns the first not NULL
  """
  def coalesce(a, b), do: Enum.find([a, b], &(&1 != nil))
  def coalesce(a, b, c), do: Enum.find([a, b, c], &(&1 != nil))
  def coalesce(a, b, c, d), do: Enum.find([a, b, c, d], &(&1 != nil))
  def coalesce(a, b, c, d, e), do: Enum.find([a, b, c, d, e], &(&1 != nil))

  @doc ~S"""
  Returns NULL if both equal, first argument if not.
  """
  def nullif(a, a), do: nil
  def nullif(a, _), do: a

  def floor(n) when is_float(n), do: trunc(Float.floor(n))
  def floor(n) when is_number(n), do: n
  def floor(n), do: floor(ExoSQL.Utils.to_number!(n))

  def ceil(n) when is_float(n), do: trunc(Float.ceil(n))
  def ceil(n) when is_number(n), do: n
  def ceil(n), do: ceil(ExoSQL.Utils.to_number!(n))

  ### Aggregate functions
  def is_aggregate("count"), do: true
  def is_aggregate("avg"), do: true
  def is_aggregate("sum"), do: true
  def is_aggregate("max"), do: true
  def is_aggregate("min"), do: true
  def is_aggregate(_other), do: false

  def count(data, {:lit, '*'}) do
    Enum.count(data.rows)
  end
  def count(data, {:distinct, expr}) do
    expr = ExoSQL.Expr.simplify(expr, %{ columns: data.columns })
    Enum.reduce(data.rows, MapSet.new(), fn row, acc ->
      case ExoSQL.Expr.run_expr(expr, %{ row: row }) do
        nil -> acc
        val -> MapSet.put(acc, val)
      end
    end) |> Enum.count
  end
  def count(data, expr) do
    expr = ExoSQL.Expr.simplify(expr, %{ columns: data.columns})
    Enum.reduce(data.rows, 0, fn row, acc ->
      case ExoSQL.Expr.run_expr(expr, %{ row: row }) do
        nil -> acc
        _other -> 1 + acc
      end
    end)
  end

  def avg(data, expr) do
  # Logger.debug("Avg of #{inspect data} by #{inspect expr}")
    if data.columns == [] do
      nil
    else
      sum(data, expr) / count(data, {:lit, '*'})
    end
  end

  def sum(data, expr) do
  # Logger.debug("Sum of #{inspect data} by #{inspect expr}")
    expr = ExoSQL.Expr.simplify(expr, %{ columns: data.columns})
    # Logger.debug("Simplified expression #{inspect expr}")
    Enum.reduce(data.rows, 0, fn row, acc ->
      n = ExoSQL.Expr.run_expr(expr, %{ row: row })
      n = case ExoSQL.Utils.to_number(n) do
        {:ok, n} -> n
        {:error, nil} -> 0
      end
      acc + n
    end)
  end

  def max_(data, expr) do
    expr = ExoSQL.Expr.simplify(expr, %{ columns: data.columns})
    Enum.reduce(data.rows, nil, fn row, acc ->

      n = ExoSQL.Expr.run_expr(expr, %{ row: row })
      {acc, n} = ExoSQL.Expr.match_types(acc, n)

      if ExoSQL.Expr.is_greater(acc, n) do
        acc
      else
        n
      end

    end)
  end
  def min_(data, expr) do
    expr = ExoSQL.Expr.simplify(expr, %{ columns: data.columns})
    Enum.reduce(data.rows, nil, fn row, acc ->

      n = ExoSQL.Expr.run_expr(expr, %{ row: row })
      {acc, n} = ExoSQL.Expr.match_types(acc, n)

      res = if acc != nil and ExoSQL.Expr.is_greater(n, acc) do
        acc
      else
        n
      end

      res
    end)
  end

  ## Simplications.

  # Precompile regex
  def simplify("format", [{:lit, format} | rest]) when is_binary(format) do
    compiled = ExoSQL.Format.compile_format(format)
    # Logger.debug("Simplify format: #{inspect compiled}")
    simplify("format", [{:lit, compiled} | rest])
  end
  def simplify("regex", [str, {:lit, regexs}]) when is_binary(regexs) do
    regex = Regex.compile!(regexs)
    captures = String.contains?(regexs, "(?<")

    simplify("regex", [str, {:lit, regex}, {:lit, captures}])
  end
  def simplify("regex", [str, {:lit, regexs}, {:lit, query}]) when is_binary(regexs) do
    regex = Regex.compile!(regexs)
    captures = String.contains?(regexs, "(?<")

    # this way jq can be simplified too
    params = [
      simplify("regex", [str, {:lit, regex}, {:lit, captures}]),
      {:lit, query}
    ]

    simplify("jp", params)
  end
  def simplify("jp", [json, {:lit, path}]) when is_binary(path) do
    # Logger.debug("JP #{inspect json}")
    simplify("jp", [json, {:lit, String.split(path, "/")}])
  end

  # default: convert to {mod fun name} tuple
  def simplify(name, params) when is_binary(name) do
    # Logger.debug("Simplify #{inspect name} #{inspect params}")
    if (not is_aggregate(name)) and Enum.all?(params, &is_lit(&1)) do
      # Logger.debug("All is literal for #{inspect {name, params}}.. just do it once")
      params = Enum.map(params, fn {:lit, n} -> n end)
      ret = ExoSQL.Builtins.call_function(name, params)
      {:lit, ret}
    else
      case @functions[name] do
        nil ->
        throw {:unknown_function, name}
      {mod, fun} ->
        {:fn, {{mod, fun, name}, params}}
      end
    end
  end
  def simplify(modfun, params), do: {:fn, {modfun, params}}

  def is_lit({:lit, '*'}), do: false
  def is_lit({:lit, _n}), do: true
  def is_lit(_), do: false

end
