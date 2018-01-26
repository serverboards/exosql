defmodule ExoSQL.Node do
  @moduledoc """
  Example extractor that gather information from the system

  Currently only supports the `passwd` table.
  """

  def schema(_db), do: {:ok, ["passwd"]}
  def schema(_db, "passwd") do
    {:ok, %{ columns: [
      "user", "x", "uid", "gid", "name", "home", "shell"
      ]}}
  end

  def execute(_db, "passwd", _quals, _columns) do

    csv_data = File.stream!("/etc/passwd") |> CSV.decode(separator: ?:)

    rows = for l <- csv_data do
      {:ok, l} = l
      l
    end

    {:ok, %{ columns: [
      "user", "x", "uid", "gid", "name", "home", "shell"
      ], rows: rows}}

  end
end
