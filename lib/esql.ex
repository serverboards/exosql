require Logger

defmodule Esql do
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

  defp get_item([{k, v} | rest], k), do: v
  defp get_item([{nk, _} | rest], k), do: get_item(rest, k)
  defp get_item([], _k), do: nil

  defp run_expr({:and, op1, op2}, cur) do
    r1 = run_expr(op1, cur)
    r2 = run_expr(op2, cur)
    Logger.debug("and1: #{inspect op1} => #{inspect r1}")
    Logger.debug("and2: #{inspect op2} => #{inspect r2}")
    r1 && r2
  end
  defp run_expr({:eq, op1, op2}, cur), do: run_expr(op1, cur) == run_expr(op2, cur)
  defp run_expr({:gt, op1, op2}, cur) do
    {n1, ""} = Integer.parse(run_expr(op1, cur))
    {n2, ""} = Integer.parse(run_expr(op2, cur))

    n1 > n2
  end
  defp run_expr(val, cur) when is_binary(val), do: val
  defp run_expr({db, _, _} = k, cur) when is_binary(db) do
    v = get_item(cur, k)
    Logger.info("Get item #{inspect k} => #{inspect v}")
    v
  end

  defp execute_select_where(select, [], [], cur) do
    [for s <- select do
      get_item(cur, s)
    end]
  end


  defp execute_select_where(select, expr, [], cur) do
    if run_expr(expr, cur) do
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

  def execute(query, context) do
    Logger.debug("Execute #{inspect query} #{inspect context}")

    plan = for {db, table} <- query.from do
      columns = get_vars(db, table, query.select)
      quals = []
      {db, table, quals, columns}
    end

    Logger.debug("My plan is #{inspect plan, pretty: true}")

    data = for {db, table, quals, columns} <- plan do
      Logger.debug("Plan: #{inspect db} ( #{inspect {table, quals, columns}})")
      {dbmod, context} = context[db]
      {:ok, data} = apply(dbmod, :execute, [context, table, quals, columns])

      %{ headers: headers, rows: rows} = data

      headers = for h <- headers, do: {db, table, h}

      %{headers: headers, rows: rows}
    end
    Logger.debug("My data: #{inspect data, pretty: true}")

    rows = execute_select_where(query.select, query.where, data, [])
      |> Enum.filter(&(&1))


    {:ok, %{ headers: query.select, rows: rows }}
  end
end
