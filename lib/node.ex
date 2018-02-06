defmodule ExoSQL.Node do
  @moduledoc """
  Example extractor that gather information from the system

  Currently only supports the `passwd` table.
  """

  def schema(_db), do: {:ok, ["passwd", "proc"]}
  def schema(_db, "passwd") do
    {:ok, %{ columns: [
      "user", "x", "uid", "gid", "name", "home", "shell"
      ]}}
  end
  def schema(_db, "proc") do
    {:ok, %{ columns: [
      "pid", "cmd", "args"
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

  def execute(_config, "proc", _quals, columns) do
    known_columns = ["pid", "cmd", "args"]
    for c <- columns do
      if not c in known_columns do
        raise MatchError, {:unknown_column, c}
      end
    end

    {:ok, proc} = File.ls("/proc/")
    # Keep only numeric procs
    proc = Enum.flat_map(proc, fn n ->
      case Integer.parse(n) do
        {n, ""} -> [n]
        _ -> []
      end
    end)

    rows = Enum.map(proc, &({File.open("/proc/#{&1}/cmdline"), &1})) |> Enum.flat_map(fn
      {{:ok, fd}, pid} ->
        data = case IO.read(fd, 1024) do
          :eof ->
            []
          data ->
            [cmd | args] = String.split(data, "\0")
            [[pid, cmd, args]]
        end
      _ ->
        []
    end)

    {:ok, %{
      columns: known_columns,
      rows: rows
    }}
  end
end
