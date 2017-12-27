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
end
