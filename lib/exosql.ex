require Logger

defmodule ExoSQL do
  @moduledoc """
  Creates a Generic universal parser that can access many tabular databases,
  and perform SQL queries.

  The databases can be heterogenic, so you can perform searches mixing
  data from postgres, mysql, csv or Google Analytics.
  """

  defmodule Query do
    defstruct [
      select: [],
      from: [],
      where: []
    ]
  end


  @doc """
  """
  def parse(sql) do
    sql = String.to_charlist(sql)
    {:ok, lexed, 1} = :sql_lexer.string(sql)
    {:ok, parsed} = :sql_parser.parse(lexed)
    {select, from, where} = parsed

    from = for {:table, table} <- from, do: table

    {:ok, %Query{
      select: select,
      from: from,
      where: where
    }}
  end

  defp get_vars(db, table, [expr | tail]) do
    get_vars(db, table, expr) ++ get_vars(db, table, tail)
  end
  defp get_vars(db, table, []), do: []

  defp get_vars(db, table, {db, table, column}) do
    [column]
  end
  defp get_vars(_db, _table, _other) do
    []
  end

  defp execute_select_where(select, [], [], cur) do
    [for s <- select do
      ExoSQL.Expr.run_expr(s, cur)
    end]
  end


  defp execute_select_where(select, [expr], [], cur) do
    # Logger.debug("Check row #{inspect cur} | #{inspect expr}")
    if ExoSQL.Expr.run_expr(expr, cur) do
      execute_select_where(select, [], [], cur)
    else
      []
    end
  end

  defp execute_select_where(select, where, data, cur) do
    [head | rest ] = data
    %{ headers: headers, rows: rows} = head
    Enum.flat_map(rows, fn row ->
      myrows = Enum.zip(headers, row)
      execute_select_where(select, where, rest, cur ++ myrows)
    end)
  end

  def execute(query, context) when is_map(context) do
    # Logger.debug("Execute #{inspect query} #{inspect context}")

    plan = for {db, table} <- query.from do
      columns = get_vars(db, table, query.select)
      quals = []
      {db, table, quals, columns}
    end

    # Logger.debug("My plan is #{inspect plan, pretty: true}")

    data = for {db, table, quals, columns} <- plan do
      # Logger.debug("Plan: #{inspect db} ( #{inspect {table, quals, columns}})")
      {dbmod, context} = context[db]
      {:ok, data} = apply(dbmod, :execute, [context, table, quals, columns])

      %{ headers: headers, rows: rows} = data

      headers = for h <- headers, do: {db, table, h}

      %{headers: headers, rows: rows}
    end
    # Logger.debug("My data: #{inspect data, pretty: true}")

    rows = execute_select_where(query.select, query.where, data, [])
      |> Enum.filter(&(&1))


    {:ok, %{ headers: query.select, rows: rows }}
  end


  def query(sql, context) do
    Logger.debug(inspect sql)
    {:ok, parsed} = parse(sql)
    execute(parsed, context)
  end

  def format_result(res) do
    s = for {db, table, column} <- res.headers do
      "#{db}.#{table}.#{column}"
    end |> Enum.join(" | ")
    s = [s,  "\n"]
    s = [s, String.duplicate("-", Enum.count(s))]
    s = [s,  "\n"]

    data = for r <- res.rows do
      c = Enum.join(r, " | ")
      [c, "\n"]
    end

    s = [s, data, "\n"]



    Logger.debug(inspect s)


    to_string(s)
  end
end
