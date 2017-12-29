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

  defmodule CrossJoinTables do
    defstruct [
      headers: [], # List of list of headers
      rows: []  # List of list of rows
    ]
    def new([]) do
      %CrossJoinTables{}
    end
    def new([first | rest]) do
      rest = new(rest)
      %CrossJoinTables{
        headers: first.headers ++ rest.headers,
        rows: [first.rows] ++ rest.rows
      }
    end

    defimpl Enumerable do
      defp count([head | tail], acc) do
        # Logger.debug("Count #{inspect head} #{inspect acc}")
        count( tail, Enum.count(head) * acc )
      end
      defp count([], acc), do: acc

      def count(%CrossJoinTables{ rows: rows}) do
        {:ok, count(rows, 1)}
      end
      def count(%CrossJoinTables{ rows: []}), do: 0


      def reduce(_,       {:halt, acc}, _fun),   do: {:halted, acc}
      def reduce(list,    {:suspend, acc}, fun), do: {:suspended, acc, &reduce(list, &1, fun)}


      def reduce(%CrossJoinTables{ rows: []}, {:cont, acc}, _fun),   do: {:done, acc}
      def reduce(%CrossJoinTables{ rows: rows }, {:cont, acc}, fun) do
        state = for [hrow | trow] <- rows do
          {[hrow], trow}
        end
        firstr = reducer_current_row(state)


        # Logger.debug("First row: #{inspect firstr}")
        # first all of reducer, then recurse
        reduce(state, fun.(firstr, acc), fun )
      end

      def reduce(state, {:cont, acc}, fun) do
        # Logger.debug("Reduce state #{inspect state}")
        nextstate = reducer_nextstate(state)
        # Logger.debug("Next state #{inspect nextstate}")

        case nextstate do
          :empty -> {:stop, acc}
          other ->
            crow = reducer_current_row(nextstate)
            # Logger.debug("Generated row is: #{inspect crow}")

            reduce(nextstate, fun.(crow, acc), fun )
        end
      end

      def reducer_current_row(state) do
        Enum.flat_map(state, fn {[hrow | _rest], _trow} -> hrow end)
      end

      def reducer_rotate_rstate({head, [prev | next]}) do
        {[prev | head], next} # I pass one from next to prev head
      end
      def reducer_reset_rstate([]), do: []
      def reducer_reset_rstate([{next, []} | tail]) do
        [h | t] = Enum.reverse(next) # was reversed during processing forperfomance reasons
        [{[h], t} | reducer_reset_rstate(tail)]
      end

      def reducer_nextstate([{prev, []}]) do
        :empty
      end
      def reducer_nextstate([rstate]) do
        [reducer_rotate_rstate(rstate)]
      end
      def reducer_nextstate([head | rest]) do
        case reducer_nextstate(rest) do
          :empty ->
            # Logger.debug("Empty at #{inspect head}")
            {prev, next} = head
            if next == [] do
              :empty
            else
              [reducer_rotate_rstate(head) | reducer_reset_rstate(rest)]
            end
          rest ->
            [head | rest]
        end
      end

    end

  end


  @doc """
  """
  def parse(sql) do
    sql = String.to_charlist(sql)
    {:ok, lexed, 1} = :sql_lexer.string(sql)
    {:ok, parsed} = :sql_parser.parse(lexed)
    Logger.debug("parsed #{inspect parsed}")
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


  def convert_column_names({:column, cn}, names) do
    i = Enum.find_index(names, &(&1 == cn))
    {:column, i}
  end
  def convert_column_names({:op, {op, op1, op2}}, names) do
    op1 = convert_column_names(op1, names)
    op2 = convert_column_names(op2, names)
    {:op, {op, op1, op2}}
  end

  # The where filtering has passed, run the expressions for the select, and returns this row
  defp execute_select_where(select, [], [], cur) do
    [for s <- select do
      ExoSQL.Expr.run_expr(s, cur)
    end]
  end


  # I have a full row at cur, as a map with the header name as key, perform the where filtering, only one expr always
  defp execute_select_where(select, [expr], [], cur) do
    # Logger.debug("Check row #{inspect cur} | #{inspect expr}")
    if ExoSQL.Expr.run_expr(expr, cur) do
      execute_select_where(select, [], [], cur)
    else
      []
    end
  end

  # for each table, get each of the rows, and use as cur, then do the rest of tables
  defp execute_select_where(select, where, [head | rest ], cur) do
    %{ headers: headers, rows: rows} = head
    Enum.flat_map(rows, fn row ->
      myrows = Enum.zip(headers, row)
      execute_select_where(select, where, rest, cur ++ myrows)
    end)
  end

  def execute(query, context) when is_map(context) do
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

    rows = CrossJoinTables.new(data) # this is an enumerable
    # Logger.debug("rows #{inspect rows, pretty: true}")
    # Logger.debug("Total count: #{Enum.count(rows)}")
    # Logger.debug("Data: #{inspect data}")
    select = for expr <- query.select, do: convert_column_names(expr, rows.headers)
    rows = if query.where do
      [expr] = query.where
      expr = convert_column_names(expr, rows.headers)
      # Logger.debug("expr #{inspect expr}")
      rows = Enum.filter(rows, fn row ->
        # Logger.debug(row)
        ExoSQL.Expr.run_expr(expr, row)
      end) |> Enum.map( fn row ->
        for expr <- select do
          ExoSQL.Expr.run_expr(expr, row)
        end
      end)
    end

    # rows = execute_select_where(query.select, query.where, data, [])
    #   |> Enum.filter(&(&1))

    {:ok, %{ headers: query.select, rows: rows }}
  end


  def query(sql, context) do
    Logger.debug(inspect sql)
    {:ok, parsed} = parse(sql)
    execute(parsed, context)
  end

  def format_result(res) do
    s = for {h, n} <- Enum.with_index(res.headers) do
      case h do
        {:column, {db, table, column}} ->
          "#{db}.#{table}.#{column}"
        _ -> "?COL#{n+1}"
      end
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
