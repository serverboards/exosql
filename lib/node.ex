defmodule ExoSQL.Node do
  def execute(db, "passwd", _quals, columns) do

    csv_data = File.stream!("/etc/passwd") |> CSV.decode(separator: ?:)

    rows = for l <- csv_data do
      {:ok, l} = l
      l
    end

    {:ok, %{ headers: [
      "user", "x", "uid", "gid", "name", "home", "shell"
      ], rows: rows}}

  end
end
