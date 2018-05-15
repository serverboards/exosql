require Logger

defmodule ExoSQL.Format do
  @format_re ~r/%[\d\.,\-+]*[%fsdk]/
  @format_re_one ~r/([\d\.,\-+]*)([fsdk])/

  def format(str, params) do
    res = Regex.split(@format_re, str, include_captures: true)
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
    case res do
      {str, []} ->
        str
      {_str, other} ->
        throw {:error, {:format, {:pending, Enum.count(other)}}}
    end

  end


  def localized_number(number) do
    localized_number(number, 0)
  end
  def localized_number(number, 0) do
    int = :erlang.float_to_binary(Float.floor(number), decimals: 0)
    head = localized_number_str_comma(int)
    "#{head}"
  end
  def localized_number(number, decimals) do
    dec = :erlang.float_to_binary(number, decimals: decimals)
    {int, dec} = String.split_at(dec, String.length(dec) - decimals)
    head = localized_number_str_comma(String.slice(int, 0, String.length(int) - 1))
    if dec == String.duplicate("0", decimals) do
      "#{head}"
    else
      "#{head},#{dec}"
    end
  end

  defp localized_number_str_comma(lit) do
    if String.length(lit) > 3 do
      {head, tail} = String.split_at( lit, String.length(lit) - 3 )
      head = localized_number_str_comma( head )
      "#{head}.#{tail}"
    else
      lit
    end
  end

  def format_one(type, data) do
    to_string(data)
    Regex.replace(@format_re_one, type, fn
      _, "", "s" ->
        to_string(data)
      _, "-" <> count, "s" ->
        data = to_string(data)
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          data <> String.duplicate(" ", count) 
        else
          data
        end
      _, count, "s" ->
        data = to_string(data)
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          String.duplicate(" ", count) <> data
        else
          data
        end
      _, "", "f" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        :erlang.float_to_binary(data, decimals: 2)
      _, "." <> decimals, "f" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        {:ok, decimals} = ExoSQL.Utils.to_number(decimals)
        :erlang.float_to_binary(data, decimals: decimals)
      _, "+", "f" ->
        {:ok, datan} = ExoSQL.Utils.to_float(data)
        data = :erlang.float_to_binary(datan, decimals: 2)
        if datan <= 0 do
          "#{data}"
        else
          "+#{data}"
        end
      _, "", "d" ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        data = Kernel.trunc(data)
        "#{data}"
      _, "+", "d" ->
        datan = ExoSQL.Utils.to_number!(data)
        datan = Kernel.trunc(data)
        if datan <= 0 do
          "#{data}"
        else
          "+#{data}"
        end
      _, <<fill::size(8)>> <> count, "d" ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        data = Kernel.trunc(data)
        data = "#{data}"
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          String.duplicate(<<fill::utf8>>, count) <> data
        else
          data
        end
      _, ".", "k" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        {data, sufix} = cond do
          data >= 1_000_000 ->
            data = data / 1_000_000
            data = :erlang.float_to_binary(data, decimals: 1)
            {data, "M"}
          data >= 100_000 ->
            data = data / 1_000
            data = :erlang.float_to_binary(data, decimals: 1)
            {data, "K"}
          data >= 1_000 ->
            data = Kernel.trunc(data)
            {data, ""}
          true ->
            data = :erlang.float_to_binary(data, decimals: 2)
            {data, ""}
        end
        "#{data}#{sufix}"
      _, ",", "k" ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        {data, sufix} = cond do
          data >= 1_000_000 ->
            data = data / 1_000_000
            data = localized_number(data, 1)
            {data, "MM"}
          data >= 100_000 ->
            data = data / 1_000
            data = localized_number(data, 1)
            {data, "K"}
          data >= 1_000 ->
            data = localized_number(data, 0)
            {data, ""}
          true ->
            data = localized_number(data, 2)
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
