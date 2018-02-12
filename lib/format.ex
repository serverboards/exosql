require Logger

defmodule ExoSQL.Format do
  @format_re ~r/%[\d\.\-]*[%fsd]/
  @format_re_one ~r/([\d\.\-]*)([fsd])/

  def format(str, params) do
    {str, []} = Regex.split(@format_re, str, include_captures: true)
      |> Enum.reduce({"", params}, fn
        ("%%", {acc, params}) ->
          {acc <> "%", params}
        ("%" <> fs, {acc, params}) ->
          [head | rest] = params
          repl = format_one(fs, head)
          {acc <> repl, rest}
        (other, {acc, params}) ->
          {acc <> other, params}
    end)

    str
  end

  def format_one(type, data) do
    to_string(data)
    Regex.replace(@format_re_one, type, fn
      _, "", "s" ->
        to_string(data)
      _, "", "f" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        Float.to_string(data, decimals: 2)
      _, "." <> decimals, "f" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        {:ok, decimals} = ExoSQL.Utils.to_number(decimals)
        Float.to_string(data, decimals: decimals)
      _, "", "d" ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        "#{data}"
    end)
  end

end
