require Logger

defmodule ExoSQL.Format do
  @format_re ~r/%[\d\.\-]*[%fsdk]/
  @format_re_one ~r/([\d\.\-]*)([fsdk])/

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
        data = Kernel.trunc(data)
        "#{data}"
      _, ".", "k" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        Logger.debug(" > #{inspect data}")
        {data, sufix} = cond do
          data >= 100_000 ->
            data = data / 1_000_000
            data = Float.to_string(data, decimals: 2)
            {data, "M"}
          data >= 100 ->
            data = data / 1_000
            data = Float.to_string(data, decimals: 2)
            {data, "k"}
          true ->
            data = Float.to_string(data, decimals: 2)
            {data, ""}
        end
        "#{data}#{sufix}"
      _, "", "k" ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        data = Kernel.trunc(data)
        {data, sufix} = cond do
          data >= 1_000_000 ->
            {div(data, 1_000_000), "M"}
          data >= 1_000 ->
            {div(data, 1_000), "k"}
          true ->
            {data, ""}
        end
        "#{data}#{sufix}"
    end)
  end

end
