require Logger

defmodule ExoSQL.DateTime do
  @moduledoc """
  Helpers for datetime
  """

  def strftime(dt, format) do
    dt = to_datetime(dt)

    # Simplifications, %i from sqlite is supported
    format = format
      |> String.replace("%i", "%FT%T%z")

    Timex.format!(dt, format, :strftime)
      |> String.replace("+0000", "Z")
  end

  def to_datetime(n) when is_number(n) do
    {:ok, dt} = DateTime.from_unix(n)
    dt
  end
  def to_datetime(n) when is_binary(n) do
    if String.contains?(n, "-") do
      case String.length(n) do
        10 ->
          n = "#{n} 00:00:00Z"
          {:ok, td, 0} = DateTime.from_iso8601(n)
          td
        16 ->
          n = "#{n}:00Z"
          {:ok, td, 0} = DateTime.from_iso8601(n)
          td
        19 ->
          {:ok, td, 0} = DateTime.from_iso8601(n <> "Z")
          td
        20 ->
          {:ok, td, 0} = DateTime.from_iso8601(n)
          td
        24 ->
          {:ok, td, _} = DateTime.from_iso8601(n)
          td
      end
    else
      {:ok, unixtime} = ExoSQL.Utils.to_number(n)
      # Logger.debug("To datetime #{inspect unixtime}")
      to_datetime(unixtime)
    end
  end
  def to_datetime(%DateTime{} = d), do: d
  def to_datetime(other) do
    raise ArgumentError, message: "cant convert #{inspect other} to date"
  end
  def to_datetime(dt, "-" <> mod = orig) do
    dt = to_datetime(dt)
    ExoSQL.DateTime.Duration.datetime_add(dt, orig)
  end
  def to_datetime(dt, "+" <> mod = orig) do
    dt = to_datetime(dt)
    ExoSQL.DateTime.Duration.datetime_add(dt, orig)
  end
  def to_datetime(dt, tz) do
    dt = to_datetime(dt)

    Timex.to_datetime(dt, tz)
  end

  @units %{
    "minutes" => 60,
    "hours" => 60 * 60,
    "days" => 24 * 60 * 60,
    "weeks" => 7 * 24 * 60 * 60,
  }

  def datediff({:range, {start, end_}}), do: datediff(start, end_, "days")
  def datediff({:range, {start, end_}}, units), do: datediff(start, end_, units)
  def datediff(start, end_), do: datediff(start, end_, "days")
  def datediff(%DateTime{} = start, %DateTime{} = end_, "years") do
    if start > end_ do
      -datediff(end_, start, :years)
    else
      years = end_.year - start.year

      if end_.month < start.month do
        years - 1
      else
        years
      end
    end
  end
  def datediff(%DateTime{} = start, %DateTime{} = end_, "months") do
    if start > end_ do
      -datediff(end_, start, :months)
    else
      years = datediff(start, end_, "years")

      months = if start.month < end_.month do
        end_.month - start.month
      else
        end_.month + 12 - start.month
      end

      years * 12 + months
    end
  end
  def datediff(%DateTime{} = start, %DateTime{} = end_, "seconds") do
    DateTime.diff(end_, start, :second)
  end
  def datediff(%DateTime{} = start, %DateTime{} = end_, unit) do
    div( DateTime.diff(end_, start, :second), Map.get(@units, unit))
  end
  def datediff(start, end_, unit) do
    start = to_datetime(start)
    end_ = to_datetime(end_)

    datediff(start, end_, unit)
  end
end


defmodule ExoSQL.DateTime.Duration do
  @moduledoc """
  Parses and manipulates 8601 durations

  https://en.wikipedia.org/wiki/ISO_8601#Durations
  """
  defstruct seconds: 0, days: 0, months: 0, years: 0

  def parse(<<?P, rest::binary>>) do
    parse(rest)
  end
  def parse(<<?+, rest::binary>>) do
    parse(rest)
  end
  def parse(<<?-, rest::binary>>) do
    case parse(rest) do
      {:ok, res} ->
        {:ok, %ExoSQL.DateTime.Duration{
          seconds: -res.seconds,
          days: -res.days,
          months: -res.months,
          years: -res.years,
        }}
      other -> other
    end
  end
  # no need for P
  def parse(rest) do
    case parse_split(rest) do
      {:ok, parsed} ->
        {:ok, parse_date(parsed, %ExoSQL.DateTime.Duration{})}
      other -> other
    end
  end

  def parse!(str) do
    case parse(str) do
      {:ok, ret} -> ret
      other -> throw other
    end
  end

  defp parse_split(str) do
    case Regex.run(~r/^(\d+[YMWDHS]|T)+$/, str) do
      nil ->
        {:error, :invalid_duration}
      _other ->
        ret = Regex.scan(~r/(\d+)([YMWDHS])|(T)/, str) |> Enum.map(fn
          [_, a, b] ->
            {n, ""} = Integer.parse(a)
            {n,b}
          ["T" | _rest] -> :t
        end)
        {:ok, ret}
    end
  end

  defp parse_date([], acc), do: acc
  defp parse_date([{n, "Y"} | rest], acc), do: parse_date(rest, Map.update(acc, :years, n,  &(&1 + n)))
  defp parse_date([{n, "M"} | rest], acc), do: parse_date(rest, Map.update(acc, :months, n, &(&1 + n)))
  defp parse_date([{n, "W"} | rest], acc), do: parse_date(rest, Map.update(acc, :days, n, &(&1 + n*7)))
  defp parse_date([{n, "D"} | rest], acc), do: parse_date(rest, Map.update(acc, :days, n, &(&1 + n)))
  defp parse_date([:t | rest], acc), do: parse_time(rest, acc)

  defp parse_time([], acc), do: acc
  defp parse_time([{n, "H"} | rest], acc), do: parse_time(rest, Map.update(acc, :seconds, n * 60 * 60, &(&1 + n * 60 * 60)))
  defp parse_time([{n, "M"} | rest], acc), do: parse_time(rest, Map.update(acc, :seconds, n * 60, &(&1 + n * 60)))
  defp parse_time([{n, "S"} | rest], acc), do: parse_time(rest, Map.update(acc, :seconds, n, &(&1 + n)))

  def is_negative(%ExoSQL.DateTime.Duration{ seconds: seconds, days: days, months: months, years: years}) do
    seconds < 0 or days < 0 or months < 0 or years < 0
  end

  def datetime_add(date, duration) when is_binary(duration) do
    datetime_add(date, parse!(duration))
  end
  def datetime_add(date, %ExoSQL.DateTime.Duration{} = duration) do
    date = case duration.seconds do
      0 -> date
      seconds ->
        Timex.shift(date, seconds: seconds)
    end
    date = case duration.days do
      0 -> date
      days ->
        Timex.shift(date, days: days)
    end
    date = case duration.months do
      0 -> date
      months ->
        Timex.shift(date, months: months)
    end
    date = case duration.years do
      0 -> date
      years ->
        date = Timex.shift(date, years: years)
    end

    date
  end
end
