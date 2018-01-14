require Logger

defmodule ExoSQL.Utils do
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
  def to_float(n) when is_number(n), do: {:ok, n + 0.0} # Maybe better way??
  def to_float(n) when is_binary(n) do # Weak typing
    {n, rem} = Float.parse(n)
    if rem == "" do
      {:ok, n}
    else
      {:error, :bad_number}
    end
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
    s = ["\n", s |> Enum.join(" | ")]
    s = [s,  "\n"]
    totalw = (Enum.count(s) * 3) + Enum.reduce(widths, 0, &(&1 + &2))
    Logger.debug("#{inspect widths} #{inspect totalw}")
    s = [s, String.duplicate("-",  totalw)]
    s = [s,  "\n"]

    data = for r <- res.rows do
      c = Enum.join(Enum.map( Enum.zip(widths, r), fn {w, r} ->
        String.pad_trailing(to_string(r), w)
      end), " | ")
      [c, "\n"]
    end

    s = [s, data, "\n"]

    # Logger.debug(inspect s)
    to_string(s)
  end
end
