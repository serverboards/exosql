require Logger

defmodule ExoSQL.Csv do
  def schema(db) do
    {:ok, files} = File.ls(db[:path])
    files = files
      |> Enum.filter(&String.ends_with?(&1, ".csv"))
      |> Enum.map(&String.slice(&1, 0, String.length(&1)-4))
    {:ok, files}
  end

  def schema(db, table) do
    filename = "#{Path.join(db[:path], table)}.csv"
    [{:ok, headers}] = File.stream!(filename) |> CSV.decode |> Enum.take(1)

    {:ok, %{ headers: headers}}
  end

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
