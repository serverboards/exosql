require Logger

defmodule ExoSQL.Parser do
  @moduledoc """
  Parsed an SQL statement into a ExoSQL.Query.

  The Query then needs to be planned and executed.

  It also resolves partial column and table names using data from the context
  and its schema functions.

  Uses leex and yecc to perform a first phase parsing, and then
  convert an dprocess the structure using more context knowledge to return
  a proper Query struct.
  """

  ~S"""
  Parses from the yeec provided parse tree to a more realistic and complete parse
  tree with all data resolved.
  """

  defp real_parse(parsed, context) do
    %{
      with: with_,
      select: select,
      from: from,
      where: where,
      groupby: groupby,
      join: join,
      orderby: orderby,
      limit: limit,
      offset: offset,
      union: union
    } = parsed

    {select, select_options} = select

    # Logger.debug("Real parse #{inspect parsed, pretty: true} #{inspect context}")

    context =
      if with_ != [] do
        # Logger.debug("Parsed #{inspect parsed, pretty: true}")
        context = Map.put(context, :with, %{})

        Enum.reduce(with_, context, fn {name, select}, context ->
          {:ok, parsed} = real_parse(select, context)
          # Logger.debug("parse with #{inspect parsed}")
          Map.put(context, :with, Map.put(context.with, name, parsed))
        end)
      else
        context
      end

    all_tables_at_context = resolve_all_tables(context)
    # Logger.debug("All tables #{inspect all_tables_at_context}")

    # Logger.debug("Resolve tables #{inspect(from, pretty: true)}")
    {from, cross_joins} =
      case from do
        [] ->
          {nil, []}

        [from | cross_joins] ->
          from = resolve_table(from, all_tables_at_context, context)

          cross_joins =
            Enum.map(cross_joins, fn
              # already a cross join lateral, not change
              {:cross_join_lateral, opts} ->
                {:cross_join_lateral, opts}

              # dificult for erlang parser, needs just redo here.
              {:alias, {{:cross_join_lateral, cjl}, alias_}} ->
                {:cross_join_lateral, {:alias, {cjl, alias_}}}

              # functions are always lateral
              {:fn, f} ->
                {:cross_join_lateral, {:fn, f}}
              {:alias, {{:fn, f}, al}} ->
                {:cross_join_lateral, {:alias, {{:fn, f}, al}}}

              # Was using , operator -> cross joins
              other ->
                {:cross_join, other}
            end)

          {from, cross_joins}
      end

    # Logger.debug("from #{inspect (cross_joins ++ join), pretty: true}")

    join =
      Enum.map(cross_joins ++ join, fn
        {:cross_join_lateral, table} ->
          context =
            Map.put(
              context,
              "__parent__",
              resolve_all_columns([from], context) ++ Map.get(context, "__parent__", [])
            )

          # Logger.debug("Resolve table, may need my columns #{inspect table} #{inspect context}")
          resolved = resolve_table(table, all_tables_at_context, context)
          {:cross_join_lateral, resolved}

        {type, {:select, query}} ->
          {:ok, parsed} = real_parse(query, context)
          # Logger.debug("Resolved #{inspect parsed}")
          {type, parsed}

        {type, {{:select, query}, ops}} ->
          {:ok, parsed} = real_parse(query, context)
          # Logger.debug("Resolved #{inspect parsed}")
          {type, {parsed, ops}}

        {type, {:table, table}} ->
          resolved = resolve_table({:table, table}, all_tables_at_context, context)
          {type, resolved}

        {type, {{:table, table}, ops}} ->
          # Logger.debug("F is #{inspect {table, ops}}")
          resolved = resolve_table({:table, table}, all_tables_at_context, context)
          {type, {resolved, ops}}

        {_type, {{:alias, {{:fn, _}, _}}, _}} = orig ->
          orig

        {type, {{:alias, {orig, alias_}}, ops}} ->
          # Logger.debug("Table is #{inspect orig}")
          resolved = resolve_table(orig, all_tables_at_context, context)
          {type, {{:alias, {resolved, alias_}}, ops}}
      end)

    # Logger.debug("Prepare join tables #{inspect(join, pretty: true)}")
    all_tables =
      if join != [] do
        [from] ++
          Enum.map(join, fn
            {_type, {:table, from}} ->
              {:table, from}

            {_type, {:alias, {from, alias_}}} ->
              {:alias, {from, alias_}}

            {_type, {:fn, args}} ->
              {:fn, args}

            {_type, {from, _on}} ->
              from

            {_type, from} ->
              from
          end)
      else
        if from == nil do
          []
        else
          [from]
        end
      end

    Logger.debug("All tables at all columns #{inspect(all_tables)}")
    all_columns = resolve_all_columns(all_tables, context)
    Logger.debug("Resolved columns at query: #{inspect(all_columns)}")

    # Now resolve references to tables, as in FROM xx, LATERAL nested(xx.json, "a")
    from = resolve_column(from, all_columns, context)

    groupby =
      if groupby do
        Enum.map(groupby, &resolve_column(&1, all_columns, context))
      else
        nil
      end

    # Logger.debug("All tables #{inspect all_tables}")
    join =
      Enum.map(join, fn
        {type, {:fn, {func, params}}} ->
          # Logger.debug("params #{inspect params}")
          params = Enum.map(params, &resolve_column(&1, all_columns, context))
          {type, {:fn, {func, params}}}

        {type, {:alias, {{:fn, {func, params}}, alias_}}} ->
          # Logger.debug("params #{inspect params}")
          params = Enum.map(params, &resolve_column(&1, all_columns, context))
          {type, {:alias, {{:fn, {func, params}}, alias_}}}

        {type, {{:table, table}, expr}} ->
          {type,
           {
             {:table, table},
             resolve_column(expr, all_columns, context)
           }}

        {type, {any, expr}} ->
          {type,
           {
             any,
             resolve_column(expr, all_columns, context)
           }}

        {type, %ExoSQL.Query{} = query} ->
          {type, query}
      end)

    # the resolve all expressions as we know which tables to use
    # Logger.debug("Get select resolved: #{inspect select}")
    select =
      case select do
        [{:all_columns}] ->
          Enum.flat_map(all_tables, fn
            %ExoSQL.Query{select: select} ->
              Enum.with_index(select) |> Enum.map(fn {_, col} -> {:column, col} end)

            {:fn, {table, _args}} ->
              [{:column, {:tmp, table, table}}]

            {:alias, {%ExoSQL.Query{select: select}, _}} ->
              Enum.with_index(select)
              |> Enum.map(fn
                {{:alias, {_orig, col_alias}}, col} ->
                  {:alias, {{:column, col}, col_alias}}

                {_orig, col} ->
                  {:column, col}
              end)

            {:alias, {{_db, _table}, alias_}} ->
              columns = get_table_columns({:tmp, alias_}, all_columns)
              Enum.map(columns, &{:column, {:tmp, alias_, &1}})

            {:table, {db, table}} ->
              columns = get_table_columns({db, table}, all_columns)
              Enum.map(columns, &{:column, {db, table, &1}})
          end)

        _other ->
          Enum.map(select, &resolve_column(&1, all_columns, context))
      end

    # Logger.debug("Resolved: #{inspect select}")
    distinct =
      case Keyword.get(select_options, :distinct) do
        nil -> nil
        other -> resolve_column(other, all_columns, context)
      end

    crosstab = Keyword.get(select_options, :crosstab)

    where =
      if where do
        resolve_column(where, all_columns, context)
      else
        nil
      end

    # Resolve orderby
    orderby =
      Enum.map(orderby, fn {type, expr} ->
        {type, resolve_column(expr, all_columns, context)}
      end)

    # resolve union
    union =
      if union do
        {type, other} = union
        {:ok, other} = real_parse(other, context)
        {type, other}
      end

    with_ = Map.get(context, :with, %{})

    {:ok,
     %ExoSQL.Query{
       select: select,
       distinct: distinct,
       crosstab: crosstab,
       # all the tables it gets data from, but use only the frist and the joins.
       from: from,
       where: where,
       groupby: groupby,
       join: join,
       orderby: orderby,
       limit: limit,
       offset: offset,
       union: union,
       with: with_
     }}
  end

  @doc """
  Parses an SQL statement and returns the parsed ExoSQL struct.
  """
  def parse(sql, context) do
    try do
      sql = String.to_charlist(sql)

      lexed =
        case :sql_lexer.string(sql) do
          {:ok, lexed, _lines} -> lexed
          {:error, {other, _}} -> throw(other)
        end

      parsed =
        case :sql_parser.parse(lexed) do
          {:ok, parsed} -> parsed
          {:error, any} -> throw(any)
        end

      # Logger.debug("Yeec parsed: #{inspect parsed, pretty: true}")
      real_parse(parsed, context)
    catch
      {line_number, :sql_lexer, msg} ->
        {:error, {:syntax, {msg, line_number}}}

      {line_number, :sql_parser, msg} ->
        {:error, {:syntax, {to_string(msg), line_number}}}

      any ->
        Logger.debug("Generic error at SQL parse: #{inspect(any)}")
        {:error, any}
    end
  end

  @doc ~S"""
  Calculates the list of all FQcolumns.

  This simplifies later the gathering of which table has which column and so on,
  specially when aliases are taken into account
  """
  def resolve_all_columns(tables, context) do
    # Logger.debug("Resolve all tables #{inspect tables}")
    all_columns = Enum.flat_map(tables, &resolve_columns(&1, context))
    parent_columns = Map.get(context, "__parent__", [])

    all_columns ++ parent_columns
  end

  def resolve_columns({:alias, {{:fn, {"unnest", [_expr | columns]}}, alias_}}, _context) do
    columns |> Enum.map(fn {:lit, col} -> {:tmp, alias_, col} end)
  end

  def resolve_columns({:alias, {any, alias_}}, context) do
    case resolve_columns(any, context) do
      # only one answer, same name as "table", alias it
      [{_, a, a}] ->
        [{:tmp, alias_, alias_}]

      other ->
        Enum.map(other, fn {_db, _table, column} -> {:tmp, alias_, column} end)
    end
  end

  def resolve_columns({:table, {:with, table}}, context) do
    # Logger.debug("Get :with columns: #{inspect table} #{inspect context, pretty: true}")
    query = context[:with][table]

    get_query_columns(query)
    |> Enum.map(fn {_db, _table, column} -> {:with, table, column} end)
  end

  def resolve_columns({:table, nil}, _context) do
    []
  end

  def resolve_columns({:table, {db, table}}, context) do
    # Logger.debug("table #{inspect {db, table}}")
    {:ok, schema} = ExoSQL.schema(db, table, context)
    Enum.map(schema[:columns], &{db, table, &1})
  end

  # no column names given, just unnest
  def resolve_columns({:fn, {"unnest", [_expr]}}, _context) do
    [{:tmp, "unnest", "unnest"}]
  end

  def resolve_columns({:fn, {"unnest", [_expr | columns]}}, _context) do
    columns |> Enum.map(fn {:lit, col} -> {:tmp, "unnest", col} end)
  end

  def resolve_columns({:fn, {function, _params}}, _context) do
    [{:tmp, function, function}]
  end

  def resolve_columns({:lateral, something}, context) do
    resolve_columns(something, context)
  end

  def resolve_columns({:select, query}, _context) do
    {columns, _} =
      Enum.reduce(query[:select], {[], 1}, fn column, {acc, count} ->
        # Logger.debug("Resolve column name for: #{inspect column}")
        column =
          case column do
            {:column, {_db, _table, column}} ->
              {:tmp, :tmp, column}

            {:alias, {_, alias_}} ->
              {:tmp, :tmp, alias_}

            _expr ->
              {:tmp, :tmp, "col_#{count}"}
          end

        # Logger.debug("Resolved: #{inspect column}")

        {acc ++ [column], count + 1}
      end)

    # Logger.debug("Get column from select #{inspect query[:select]}: #{inspect columns}")
    columns
  end

  def resolve_columns(%ExoSQL.Query{} = q, _context) do
    get_query_columns(q)
  end

  def get_table_columns({db, table}, all_columns) do
    for {^db, ^table, column} <- all_columns, do: column
  end

  @doc ~S"""
  Resolves all known tables at this context. This helps to fully qualify tables.

  TODO Could be more efficient accessing as little as possible the schemas, but
  maybe not possible.
  """
  def resolve_all_tables(context) do
    Enum.flat_map(context, fn
      {:with, with_} ->
        Map.keys(with_) |> Enum.map(&{:with, &1})

      {db, _config} ->
        {:ok, tables} = ExoSQL.schema(db, context)
        tables |> Enum.map(&{db, &1})
    end)
  end

  @doc ~S"""
  Given a table-like tuple, returns the real table names

  The table-like can be a function, a lateral join, or a simple table. It
  resolves unknown parts, as for example {:table, {nil, "table"}}, will fill
  which db.

  It returns the same form, but with more data, and calling again will result
  in the same result.
  """
  def resolve_table({:table, {nil, name}}, all_tables, _context) when is_binary(name) do
    # Logger.debug("Resolve #{inspect name} at #{inspect all_tables}")
    options = for {db, ^name} <- all_tables, do: {db, name}
    # Logger.debug("Options are #{inspect options}")

    case options do
      [table] -> {:table, table}
      l when l == [] -> raise "Cant find table #{inspect(name)}"
      _other -> raise "Ambigous table name #{inspect(name)}"
    end
  end

  def resolve_table({:table, {_db, _name}} = orig, _all_tables, _context) do
    orig
  end

  def resolve_table({:select, query}, _all_tables, context) do
    {:ok, parsed} = real_parse(query, context)
    parsed
  end

  def resolve_table({:fn, _function} = orig, _all_tables, _context), do: orig

  def resolve_table({:alias, {table, alias_}}, all_tables, context) do
    {:alias, {resolve_table(table, all_tables, context), alias_}}
  end

  def resolve_table({:lateral, table}, all_tables, context) do
    resolved = resolve_table(table, all_tables, context)
    {:lateral, resolved}
  end

  def resolve_table(other, _all_tables, _context) do
    Logger.error("Cant resolve table #{inspect(other)}")
    # maybe it do not have the type tagged at other ({:table, other}). Typical fail here.
    raise "Cant resolve table #{inspect(other)}"
  end

  @doc ~S"""
  From the list of tables, and context, and an unknown column, return the
  FQN of the column.
  """
  def resolve_column({:column, {nil, nil, column}}, all_columns, context) do
    found =
      Enum.filter(all_columns, fn
        {_db, _table, ^column} -> true
        _other -> false
      end)

    found =
      case found do
        [one] ->
          {:column, one}

        [] ->
          parent_schema = Map.get(context, "__parent__", false)

          if parent_schema do
            {:column, found} =
              resolve_column({:column, {nil, nil, column}}, parent_schema, context)

            # Logger.debug("Found column from parent #{inspect found}")
            {:column, found}
          else
            raise "Not found #{inspect column} in #{inspect all_columns}"
          end

        _many ->
          raise "Ambiguous column #{inspect column} in #{inspect all_columns}"
      end

    if found do
      found
    else
      raise "Not found #{inspect column} in #{inspect all_columns}"
    end
  end

  def resolve_column({:column, {nil, table, column}}, all_columns, context) do
    # Logger.debug("Find #{inspect {nil, table, column}} at #{inspect all_columns} + #{inspect Map.get(context, "__parent__", [])}")
    found =
      Enum.find(all_columns, fn
        {_db, ^table, ^column} -> true
        _other -> false
      end)

    if found do
      {:column, found}
    else
      parent_schema = Map.get(context, "__parent__", [])

      if parent_schema != [] do
        context = Map.drop(context, ["__parent__"])

        case resolve_column({:column, {nil, table, column}}, parent_schema, context) do
          {:column, found} ->
            {:column, found}

          _ ->
            throw({:not_found, {table, column}, :in, all_columns})
        end
      else
        throw({:not_found, {table, column}, :in, all_columns})
      end
    end
  end

  def resolve_column({:column, _} = column, _schema, _context), do: column

  def resolve_column({:op, {op, ex1, ex2}}, all_columns, context) do
    {:op,
     {op, resolve_column(ex1, all_columns, context), resolve_column(ex2, all_columns, context)}}
  end

  def resolve_column({:fn, {f, params}}, all_columns, context) do
    params = Enum.map(params, &resolve_column(&1, all_columns, context))
    {:fn, {f, params}}
  end

  def resolve_column({:distinct, expr}, all_columns, context) do
    {:distinct, resolve_column(expr, all_columns, context)}
  end

  def resolve_column({:case, list}, all_columns, context) do
    # Logger.debug("Resolve case #{inspect list, pretty: true}")
    list =
      Enum.map(list, fn
        {c, e} ->
          {resolve_column(c, all_columns, context), resolve_column(e, all_columns, context)}
        {e} ->
          {resolve_column(e, all_columns, context)}
      end)

    {:case, list}
  end

  def resolve_column({:case, expr, list}, all_columns, context) do
    expr = resolve_column(expr, all_columns, context)
    list =
      Enum.map(list, fn
        {c, e} ->
          {resolve_column(c, all_columns, context), resolve_column(e, all_columns, context)}
        {e} ->
          {resolve_column(e, all_columns, context)}
      end)

    {:case, expr, list}
  end

  def resolve_column({:alias, {expr, alias_}}, all_columns, context) do
    {:alias, {resolve_column(expr, all_columns, context), alias_}}
  end

  def resolve_column({:select, query}, all_columns, context) do
    all_columns = all_columns ++ Map.get(context, "__parent__", [])
    context = Map.put(context, "__parent__", all_columns)
    {:ok, parsed} = real_parse(query, context)
    # Logger.debug("Parsed query #{inspect query} -> #{inspect parsed, pretty: true}")
    {:select, parsed}
  end

  def resolve_column({:lateral, expr}, all_columns, context) do
    {:lateral, resolve_column(expr, all_columns, context)}
  end

  def resolve_column(other, _schema, _context) do
    other
  end

  defp get_query_columns(%ExoSQL.Query{select: select, crosstab: false}) do
    get_column_names_or_alias(select, 1)
  end
  defp get_query_columns(%ExoSQL.Query{select: select, crosstab: :all_columns}) do
    [hd get_column_names_or_alias(select, 1)] # only the first is sure.. the rest too dynamic to know
  end
  defp get_query_columns(%ExoSQL.Query{select: select, crosstab: crosstab}) when crosstab != nil do
    first = hd get_column_names_or_alias(select, 1)
    more = crosstab |> Enum.map(&{:tmp, :tmp, &1})
    [first | more]
  end

  defp get_column_names_or_alias([{:column, column} | rest], count) do
    [column | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([{:alias, {_column, alias_}} | rest], count) do
    [{:tmp, :tmp, alias_} | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([{:fn, {"unnest", [_from | columns]}} | rest], count) do
    Enum.map(columns, fn {:lit, name} -> {:tmp, :tmp, name} end) ++ get_column_names_or_alias(rest, count + 1)
  end

  defp get_column_names_or_alias([_head | rest], count) do
    [{:tmp, :tmp, "col_#{count}"} | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([], _count), do: []
end
