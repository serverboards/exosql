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

    Logger.debug("Resolve tables #{inspect(from, pretty: true)}")
    {from, cross_joins} = case from do
      [] -> {nil, []}
      [from | cross_joins] ->
        resolved = resolve_table(from, all_tables_at_context, context)
        Logger.debug("Resolved #{inspect resolved}")
        from = case resolved do
          {:alias, {{:fn, _}, _}} = orig ->
            orig
          {:alias, {%ExoSQL.Query{}, _}} = orig ->
            orig
          {:alias, {from, alias_}} ->
            {:alias, {{:table, from}, alias_}}
          {:fn, f} ->
            {:fn, f}
          %ExoSQL.Query{} = q ->
            q
          other ->
            {:table, resolved}
        end
        cross_joins = Enum.map(cross_joins, fn
          {:table, t} -> {:cross_join, {:table, t}}
          {:alias, {{:cross_join_lateral, cc}, alias_}} -> {:cross_join_lateral, {:alias, {cc, alias_}}}
          {:select, select} -> {:cross_join, {:select, select}}
          other -> other
          # {:cross_join_lateral, t} -> {:cross_join_lateral, t}
        end)
        {from, cross_joins}
    end
    Logger.debug("from #{inspect {cross_joins, join}, pretty: true}")

    join =
      Enum.map(cross_joins ++ join, fn
        {:cross_join_lateral, table} ->
          context = Map.put(context, "__parent__",
            resolve_all_columns([from], context) ++
            Map.get(context, "__parent__", [])
          )
          # Logger.debug("Resolve table, may need my columns #{inspect table} #{inspect context}")
          resolved = resolve_table(table, all_tables_at_context, context)
          {:cross_join_lateral, resolved}
        {type, {:select, query}} ->
          {:ok, parsed} = real_parse(query, context)
          Logger.debug("Resolved #{inspect parsed}")
          {type, parsed}
        {type, {{:select, query}, ops}} ->
          {:ok, parsed} = real_parse(query, context)
          Logger.debug("Resolved #{inspect parsed}")
          {type, {parsed, ops}}
        {type, {:table, table}} ->
          resolved = resolve_table({:table, table}, all_tables_at_context, context)
          {type, {:table, resolved}}
        {type, {{:table, table}, ops}} ->
          resolved = resolve_table({:table, table}, all_tables_at_context, context)
          {type, {{:table, resolved}, ops}}
        {type, {{:alias, {{:fn, _}, _}}, _}} = orig ->
          Logger.debug("F is #{inspect orig}")
          orig
        {type, {{:alias, {orig, alias_}}, ops}} ->
          Logger.debug("Table is #{inspect orig}")
          resolved = resolve_table(orig, all_tables_at_context, context)
          {type, {{:alias, {{:table, resolved}, alias_}}, ops}}
      end)

    Logger.debug("Prepare join tables #{inspect(join, pretty: true)}")
    all_tables =
      if join != [] do
        [from] ++
          Enum.map(join, fn
            {_type, {:fn, {_name, _params}} = func} ->
              func
            {_type, {:alias, {_what, _alias}} = alias_} ->
              alias_
            {_type, {:table, table}} ->
              {:table, table}
            {_type, {{:table, table}, _op}} ->
              {:table, table}
            {_type, {{:alias, _} = alias_, _expr}} ->
              alias_
            {_type, %ExoSQL.Query{} = query} ->
              query
            {_type, {%ExoSQL.Query{} = query, _ops}} ->
              query
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
           }
          }
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
    Logger.debug("Resolve all tables #{inspect tables}")
    all_columns =
      Enum.flat_map(tables, fn
        {:alias, {{:fn, {"unnest", [_expr | columns]}}, alias_}} ->
          columns |> Enum.map(fn col -> {:tmp, alias_, col} end)

        {:alias, {table, alias_}} ->
          case resolve_all_columns([table], context) do
            # only one answer, same name as "table", alias it
            [{_, a, a}] ->
              [{:tmp, alias_, alias_}]

            other ->
              Enum.map(other, fn {_db, _table, column} -> {:tmp, alias_, column} end)
          end

        {:table, {:with, table}} ->
          Logger.debug("Get :with columns: #{inspect table} #{inspect context, pretty: true}")
          query = context[:with][table]

          get_query_columns(query)
          |> Enum.map(fn {_db, _table, column} -> {:with, table, column} end)
        {:table, nil} ->
          []
        {:table, {db, table}} ->
          Logger.debug("table #{inspect {db, table}}")
          {:ok, schema} = ExoSQL.schema(db, table, context)
          Enum.map(schema[:columns], &{db, table, &1})

        # no column names given, just unnest
        {:fn, {"unnest", [_expr]}} ->
          [{:tmp, "unnest", "unnest"}]

        {:fn, {"unnest", [_expr | columns]}} ->
          columns |> Enum.map(fn {:lit, col} -> {:tmp, "unnest", col} end)

        {:fn, {function, _params}} ->
          [{:tmp, function, function}]

        {:lateral, something} ->
          resolve_all_columns([something], context)

        other ->
          resolve_columns(other)
      end)

    parent_columns = Map.get(context, "__parent__", [])

    all_columns ++ parent_columns
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

  def resolve_columns({:select, query}) do
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

  def resolve_columns(%ExoSQL.Query{} = q) do
    get_query_columns(q)
  end

  def resolve_columns({:lateral, expr}), do: resolve_columns(expr)

  def resolve_columns({:fn, {"unnest", [_expr | columns]}}) do
    Enum.map(columns, fn {:lit, col} -> {:tmp, "unnest", col} end)
  end

  def get_table_columns({db, table}, all_columns) do
    for {^db, ^table, column} <- all_columns, do: column
  end

  @doc ~S"""
  Given a table-like tuple, returns the real table names

  The table-like can be a function, a lateral join, or a simple table
  """
  def resolve_table({:table, {nil, name}}, all_tables, _context) when is_binary(name) do
    # Logger.debug("Resolve #{inspect name} at #{inspect all_tables}")
    options = for {db, ^name} <- all_tables, do: {db, name}
    # Logger.debug("Options are #{inspect options}")

    case options do
      [table] -> table
      l when length(l) == 0 -> throw({:not_found, name})
      _other -> throw({:ambiguous_table_name, name})
    end
  end

  def resolve_table({:table, {_db, _name} = orig}, _all_tables, _context), do: orig

  def resolve_table({:select, query}, all_tables, context) do
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

  def resolve_table(other, all_tables, _context) do
    Logger.error("Cant resolve table #{inspect(other)}")
    raise "cant_resolve_table" # maybe it do not have the type. Typical fail here.
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
            throw({:not_found, column, :in, all_columns})
          end

        many ->
          throw({:ambiguous_column, column, :in, many})
      end

    if found do
      found
    else
      throw({:not_found, column, :in, all_columns})
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
    list =
      Enum.map(list, fn {c, e} ->
        {resolve_column(c, all_columns, context), resolve_column(e, all_columns, context)}
      end)

    {:case, list}
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

  defp get_query_columns(%ExoSQL.Query{select: select}), do: get_column_names_or_alias(select, 1)

  defp get_column_names_or_alias([{:column, column} | rest], count) do
    [column | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([{:alias, {_column, alias_}} | rest], count) do
    [{:tmp, :tmp, alias_} | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([_head | rest], count) do
    [{:tmp, :tmp, "col_#{count}"} | get_column_names_or_alias(rest, count + 1)]
  end

  defp get_column_names_or_alias([], _count), do: []
end
