require Logger

defmodule ExoSQL.Json do
  def schema(db) do
    {:ok, files} = File.ls(db[:path])
    files = files
      |> Enum.filter(&String.ends_with?(&1, ".json"))
      |> Enum.map(&String.slice(&1, 0, String.length(&1)-5))
    {:ok, files}
  end

  def schema(db, table) do
    filename = "#{Path.join(db[:path], table)}.json"
    {:ok, columns} = File.stream!(filename) |> Enum.take(1) |> Poison.decode

    {:ok, %{ columns: columns}}
  end

  def execute(db, table, quals, columns) do
    # Logger.debug("Get #{inspect table}#{inspect columns} | #{inspect quals}")

    filename = "#{Path.join(db[:path], table)}.json"
    Logger.debug("filename #{inspect filename}")
    csv_data = File.stream!(filename)

    stream = File.stream!(filename)

    data = for l <- stream do
      {:ok, l} = Poison.decode(l)
      l
    end

    [columns | rows] = data

    {:ok, %ExoSQL.Result{ columns: columns, rows: rows}}
  end
end
