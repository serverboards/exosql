require Logger

defmodule ExoSQL.Format do
  @format_re ~r/%[\d\.,\-+]*[%fsdk]/
  @format_re_one ~r/([\d\.,\-+]*)([fsdk])/

  def compile_format(str) do
    Regex.split(@format_re, str, include_captures: true)
      |> Enum.reduce([], fn
        ("%%", acc) ->
          ["%" | acc]
        ("%" <> fs, acc) ->
          [_all, mod, type] = Regex.run(@format_re_one, fs)
          [{mod, type} | acc]
        ("", acc) ->
          acc
        (other, acc) ->
          [other | acc]
      end)
  end

  def format(str, params) when is_binary(str) do
    # Logger.debug("Compile str #{inspect str}")
    precompiled = compile_format(str)
    # Logger.debug("Precompiled: #{inspect precompiled}")
    format(precompiled, params)
  end

  def format([], _params), do: ""
  def format(precompiled, params) when is_list(precompiled) do
    # Logger.debug("Format #{inspect {precompiled, params}}")
    res = Enum.reduce(precompiled, {[], Enum.reverse(params)}, fn
        ({mod, type}, {acc, params}) ->
          [head | rest] = params
          repl = format_one(mod, type, head)
          {[repl | acc], rest}
        (str, {acc, params}) ->
          {[str | acc], params}
    end)
    # Logger.debug("Result #{inspect {precompiled, params}} -> #{inspect res}")
    case res do
      {str, []} ->
        to_string(str)
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

  def format_one(mod, type, data) do
    # to_string(data)
    case {mod, type} do
      {"", "s"} ->
        to_string(data)
      {"-" <> count, "s"} ->
        data = to_string(data)
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          data <> String.duplicate(" ", count)
        else
          data
        end
      {count, "s"} ->
        data = to_string(data)
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          String.duplicate(" ", count) <> data
        else
          data
        end
      {"", "f"} ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        :erlang.float_to_binary(data, decimals: 2)
      {"." <> decimals, "f"} ->
        {:ok, data} = ExoSQL.Utils.to_float(data)
        {:ok, decimals} = ExoSQL.Utils.to_number(decimals)
        :erlang.float_to_binary(data, decimals: decimals)
      {"+", "f"} ->
        {:ok, datan} = ExoSQL.Utils.to_float(data)
        data = :erlang.float_to_binary(datan, decimals: 2)
        if datan <= 0 do
          "#{data}"
        else
          "+#{data}"
        end
      {"", "d"} ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        data = Kernel.trunc(data)
        "#{data}"
      {"+", "d"} ->
        datan = ExoSQL.Utils.to_number!(data)
        datan = Kernel.trunc(datan)
        if datan <= 0 do
          "#{data}"
        else
          "+#{data}"
        end
      {<<fill::size(8)>> <> count, "d"} ->
        {:ok, data} = ExoSQL.Utils.to_number(data)
        data = Kernel.trunc(data)
        data = "#{data}"
        count = ExoSQL.Utils.to_number!(count) - String.length(data)
        if count > 0 do
          String.duplicate(<<fill::utf8>>, count) <> data
        else
          data
        end
      {".", "k"} ->
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
      {",", "k"} ->
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
      {"", "k"} ->
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
    end
  end
end
