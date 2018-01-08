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
      where: nil,
      groupby: nil
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
    {:ok, lexed, _lines} = :sql_lexer.string(sql)
    {:ok, parsed} = :sql_parser.parse(lexed)
    Logger.debug("parsed #{inspect parsed}")
    {select, from, where, groupby} = parsed

    from = for {:table, table} <- from, do: table

    {:ok, %Query{
      select: select,
      from: from,
      where: where,
      groupby: groupby
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
  def convert_column_names({:fn, {f, params}}, names) do
    params = Enum.map(params, &convert_column_names(&1, names))
    {:fn, {f, params}}
  end
  def convert_column_names(other, _names), do: other


  def convert_column_names_nofn({:column, cn}, names) do
    i = Enum.find_index(names, &(&1 == cn))
    {:column, i}
  end
  def convert_column_names_nofn({:op, {op, op1, op2}}, names) do
    op1 = convert_column_names(op1, names)
    op2 = convert_column_names(op2, names)
    {:op, {op, op1, op2}}
  end
  def convert_column_names_nofn(other, _names), do: other

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

  def do_group_by(query, cjt, rows) do
    groupby = for expr <- query.groupby, do: convert_column_names(expr, cjt.headers)
    groups = Enum.reduce(rows, %{}, fn row, acc ->
      key = Enum.map(groupby, &(ExoSQL.Expr.run_expr( &1, row )))
      # Logger.debug("Group by #{inspect key}")
      Map.put(acc, key, [row] ++ Map.get(acc, key, []))
    end)
    # Logger.debug("Grouped: #{inspect groups}")

    # now I have other headers, and other select behaviour
    headers = for {:column, col} <- query.groupby, do: col
    # Logger.debug("Prepare headers for #{inspect headers}")
    #headers = for expr <- query.groupby, do: convert_column_names(expr, headers)
    # Logger.debug("New headers are: #{inspect headers}")
    select = for expr <- query.select do
      nn = convert_column_names_nofn(expr, headers)
      # Logger.debug("#{inspect {expr, headers, nn}}")
      nn
    end
    # Logger.debug("New select is: #{inspect select} // #{inspect headers}")

    rows = Enum.map(groups, fn {row, data} ->
      for expr <- select do
        case expr do
          {:fn, {fun, ['*']}} ->
            apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [nil, data])
          {:fn, {fun, [expr]}} ->
            expr = convert_column_names(expr, cjt.headers)
            # Logger.debug("Do #{inspect fun} ( #{inspect expr} )  // #{inspect data}")
            apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [expr, data])
          expr ->
            ExoSQL.Expr.run_expr(expr, row)
        end
      end
    end)
    rows
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

    cjt = CrossJoinTables.new(data) # this is an enumerable
    # Logger.debug("rows #{inspect rows, pretty: true}")
    # Logger.debug("Total count: #{Enum.count(rows)}")
    # Logger.debug("Data: #{inspect data}")
    rows = cjt
    rows = if query.where do
      [expr] = query.where
      expr = convert_column_names(expr, rows.headers)
      # Logger.debug("expr #{inspect expr}")
      rows = Enum.filter(rows, fn row ->
        # Logger.debug(row)
        ExoSQL.Expr.run_expr(expr, row)
      end)
    else
      rows
    end
    # group by
    rows = if query.groupby do
      do_group_by(query, cjt, rows)
    else
      # aggregates full result
      is_aggretate_no_group = Enum.all?(query.select, fn
        {:fn, {f, _params}} -> ExoSQL.Builtins.is_aggregate(String.downcase(f))
        other -> false
      end)
      if is_aggretate_no_group do
        res = for expr <- query.select do
          {:fn, {fun, [expr]}} = convert_column_names(expr, cjt.headers)
          apply(ExoSQL.Builtins, String.to_existing_atom(String.downcase(fun)), [expr, rows])
        end
        [res]
      else
        # just plain old select
        select = for expr <- query.select, do: convert_column_names(expr, cjt.headers)
        # Logger.debug(inspect select)
        rows = Enum.map(rows, fn row ->
          for expr <- select do
            ExoSQL.Expr.run_expr(expr, row)
          end
        end)
      end
    end

    # rows = execute_select_where(query.select, query.where, data, [])
    #   |> Enum.filter(&(&1))

    {:ok, %{ headers: query.select, rows: rows }}
  end


  def query(sql, context) do
    # Logger.debug(inspect sql)
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

    # Logger.debug(inspect s)
    to_string(s)
  end

  def schema(db, context) do
    {db, opts} = context[db]

    apply(db, :schema, [opts])
  end
  def schema(db, table, context) do
    {db, opts} = context[db]

    apply(db, :schema, [opts, table])
  end

  def resolve_table({:table, {nil, name}}, context) when is_binary(name) do
    options = Enum.flat_map(context, fn {dbname, _db} ->
      {:ok, tables} = schema(dbname, context)
      tables
        |> Enum.filter(&(&1 == name))
        |> Enum.map(&{:table, {dbname, &1}})
    end)


    case options do
      [table] -> {:ok, table}
      l when length(l) == 0 -> {:error, :not_found}
      other -> {:error, :ambiguous_table_name}
    end
  end
  def resolve_table({:table, {db, name}} = orig, context), do: {:ok, orig}

  def resolve_column({:column, {nil, nil, column}}, tables, context) do
    matches = Enum.flat_map(tables, fn {:table, {db, table}} ->
      {:ok, table_schema} = schema(db, table, context)
      Enum.flat_map(table_schema.headers, fn name ->
        if name == column do
          [{:column, {db, table, name}}]
        else
          []
        end
      end)
    end)

    case matches do
      [{:column, data}] -> {:ok, {:column, data}}
      l when length(l) == 0 -> {:error, :not_found}
      other -> {:error, :ambiguous_column_name}
    end
  end

  def resolve_column({:column, {nil, table, column}}, tables, context) do
    matches = Enum.flat_map(tables, fn
      {:table, {db, ^table}} ->
        [{:column, {db, table, column}}]
      _ -> []
    end)

    case matches do
      [{:column, data}] -> {:ok, {:column, data}}
      l when length(l) == 0 -> {:error, :not_found}
      other -> {:error, :ambiguous_column_name}
    end
  end
  def resolve_column({:column, _} = column, _tables, _context), do: {:ok, column}

end
