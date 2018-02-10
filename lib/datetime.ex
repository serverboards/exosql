require Logger

defmodule ExoSQL.DateTime do
  @moduledoc """
  Helpers for datetime
  """

  @replacement_re ~r/%i/
  def strftime(dt, format) do
    dt = to_datetime(dt)

    # Simplifications, %i from sqlite is supported
    format = format
      |> String.replace("%i", "%FT%T%z")

    res = Timex.format!(dt, format, :strftime)
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
        19 ->
          {:ok, td, 0} = DateTime.from_iso8601(n <> "Z")
          td
        20 ->
          {:ok, td, 0} = DateTime.from_iso8601(n)
          td
        24 ->
          {:ok, td, 0} = DateTime.from_iso8601(n)
          td
      end
    else
      {:ok, unixtime} = ExoSQL.Utils.to_number(n)
      Logger.debug("To datetime #{inspect unixtime}")
      to_datetime(unixtime)
    end
  end
  def to_datetime(%DateTime{} = d), do: d
  def to_datetime(other) do
    raise ArgumentError, message: "cant convert #{inspect other} to date"
  end

end
