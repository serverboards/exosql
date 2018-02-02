require Logger

defmodule ExoSQL.DateTime do
  @moduledoc """
  Helpers for datetime
  """

  @replacement_re ~r/%./
  def strftime(dt, format) do
    Regex.replace(@replacement_re, format, fn
      "%%" -> "%"
      "%i" -> DateTime.to_iso8601(dt)
      "%Y" -> "#{dt.year}"
      "%m" -> String.pad_leading(to_string(dt.month), 2, "0")
      "%d" -> String.pad_leading(to_string(dt.day), 2, "0")
      "%H" -> String.pad_leading(to_string(dt.hour), 2, "0")
      "%M" -> String.pad_leading(to_string(dt.minute), 2, "0")
      "%S" -> String.pad_leading(to_string(dt.second), 2, "0")
      "%s" -> to_string(DateTime.to_unix(dt))
      other -> other
    end)
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
        20 ->
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
