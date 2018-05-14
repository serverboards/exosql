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
  def to_datetime(dt, "-" <> mod) do
    dt = to_datetime(dt)
    mod = if String.starts_with?(mod, "P") do
      mod
    else
      "P" <> mod
    end
    duration = Timex.Duration.parse!(mod)
    Timex.subtract(dt, duration)
  end
  def to_datetime(dt, "+" <> mod) do
    dt = to_datetime(dt)
    mod = if String.starts_with?(mod, "P") do
      mod
    else
      "P" <> mod
    end
    duration = Timex.Duration.parse!(mod)
    Timex.add(dt, duration)
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
