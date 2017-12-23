require Logger

defmodule Esql.Csv do
  def execute(db, table, quals, columns) do
    # Logger.debug("Get #{inspect table}#{inspect columns} | #{inspect quals}")

    filename = "#{Path.join(db[:path], table)}.csv"
    # Logger.debug("filename #{inspect filename}")
    csv_data = File.stream!(filename) |> CSV.decode

    data = for l <- csv_data do
      {:ok, l} = l
      l
    end

    [headers | rows] = data

    {:ok, %{ headers: headers, rows: rows}}
  end
end
