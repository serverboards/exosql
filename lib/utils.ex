require Logger

defmodule ExoSQL.Utils do
  @moduledoc """
  Various assorted utility functions.
  """

  def to_number(nil), do: {:error, nil}
  def to_number(n) when is_number(n), do: {:ok, n}
  def to_number(n) when is_binary(n) do # Weak typing
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
  def to_number!(n) do
    {:ok, number} = to_number(n)
    number
  end
  def to_float(n) when is_number(n), do: {:ok, n + 0.0} # Maybe better way??
  def to_float(n) when is_binary(n) do # Weak typing
    {n, rem} = Float.parse(n)
    if rem == "" do
      {:ok, n}
    else
      {:error, :bad_number}
    end
  end
  def to_float!(n) do
    {:ok, number} = to_float(n)
    number
  end

  def format_result(res) do
    s = for {h, n} <- Enum.with_index(res.columns) do
      case h do
        {db, table, column} ->
          "#{db}.#{table}.#{column}"
        str when is_binary(str) -> str
        _ -> "?COL#{n+1}"
      end
    end
    widths = Enum.map(s, &String.length/1)
    s = [ s |> Enum.join(" | ")]
    s = [s,  "\n"]
    totalw = (Enum.count(s) * 3) + Enum.reduce(widths, 0, &(&1 + &2))
    # Logger.debug("#{inspect widths} #{inspect totalw}")
    s = [s, String.duplicate("-",  totalw)]
    s = [s,  "\n"]
    widths = Enum.drop(widths, -1) ++ [0]

    data = for r <- res.rows do
      c = Enum.join(Enum.map( Enum.zip(widths, r), fn {w, r} ->
        r = case r do
          r when is_list(r) -> "[#{Enum.join(r, ", ")}]"
          other -> other
        end

        String.pad_trailing(to_string(r), w)
      end), " | ")
      [c, "\n"]
    end

    s = [s, data]

    # Logger.debug(inspect s)
    to_string(s)
  end
end
