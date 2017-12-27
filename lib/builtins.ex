defmodule ExoSQL.Builtins do
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
end
