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
      select: select,
      from: from,
      where: where,
      groupby: groupby,
      join: join,
      orderby: orderby,
      limit: limit,
      offset: offset,
      union: union,
    } = parsed
    {select, select_options} = select

    from = Enum.map(from, &resolve_table(&1, context))

    all_tables = if join != [] do
      from ++ Enum.map(join, fn {_type, {table, _expr}} -> resolve_table(table, context) end)
    else
      from
    end

    all_schemas = resolve_all_columns(all_tables, context)
    # Logger.debug("All tables: #{inspect all_tables}")
    # Logger.debug("Resolved schemas: #{inspect all_schemas}")

    groupby = if groupby do
      Enum.map(groupby, &resolve_column(&1, all_schemas))
    else nil end

    # Logger.debug("All tables #{inspect all_tables}")
    join = Enum.map(join, fn {type, {table, expr}} ->
      {type, {
        resolve_table(table, context),
        resolve_column(expr, all_schemas)
      }}
    end)


    # the resolve all expressions as we know which tables to use
    # Logger.debug("Get select resolved: #{inspect select}")
    select = case select do
      [{:all_columns}] ->
        Enum.flat_map(all_tables, fn
          %ExoSQL.Query{ select: select } ->
            Enum.with_index(select) |> Enum.map(fn {_, col} -> {:column, col} end)
          {:fn, {table, _args}} ->
            [{:column, {:tmp, table, table}}]
          {:alias, {%ExoSQL.Query{ select: select }, _}} ->
            Enum.with_index(select) |> Enum.map(fn
              {{:alias, {_orig, col_alias}}, col} ->
                {:alias, {{:column, col}, col_alias}}
              {_orig, col} ->
                {:column, col}
            end)
          {:alias, {{db, table}, alias_}} ->
            {:ok, %{ columns: columns }} = ExoSQL.schema(db, table, context)
            Enum.map(columns, &{:column, {:tmp, alias_, &1}})
          {db, table} ->
            {:ok, %{ columns: columns }} = ExoSQL.schema(db, table, context)
            Enum.map(columns, &{:column, {db, table, &1}})
        end)
      _other  ->
        Enum.map(select, &resolve_column(&1, all_schemas))
    end
    # Logger.debug("Resolved: #{inspect select}")
    distinct = case Keyword.get(select_options, :distinct) do
      nil -> nil
      other -> resolve_column(other, all_schemas)
    end

    where = if where do
      resolve_column(where, all_schemas)
    else nil end

    # Resolve orderby
    orderby = Enum.map(orderby, fn
      {type, expr} ->
        {type, resolve_column(expr, all_schemas)}
    end)

    # resolve union
    union = if union do
      {type, other} = union
      {:ok, other} = real_parse(other, context)
      {type, other}
    end

    {:ok, %ExoSQL.Query{
      select: select,
      distinct: distinct,
      from: from, # all the tables it gets data from, but use only the frist and the joins.
      where: where,
      groupby: groupby,
      join: join,
      orderby: orderby,
      limit: limit,
      offset: offset,
      union: union
    }}
  end

  @doc """
  Parses an SQL statement and returns the parsed ExoSQL struct.
  """
  def parse(sql, context) do
    try do
      sql = String.to_charlist(sql)
      lexed = case :sql_lexer.string(sql) do
        {:ok, lexed, _lines} -> lexed
        {:error, other, _} -> throw other
      end
      parsed = case :sql_parser.parse(lexed) do
        {:ok, parsed} -> parsed
        {:error, any} -> throw any
      end
      # Logger.debug("Yeec parsed: #{inspect parsed, pretty: true}")
      real_parse(parsed, context)
    catch
      {line_number, :sql_lexer, msg} ->
        {:error, {[syntax: msg, at_line: line_number]}}
      {line_number, :sql_parser, msg} ->
        {:error, {[syntax: to_string(msg), at_line: line_number]}}
      any ->
        Logger.debug("Generic error at SQL parse: #{inspect any}")
        {:error, any}
    end
  end

  @doc ~S"""
  Calculates the list of all FQcolumns.

  This simplifies later the gathering of which table has which column and so on,
  specially when aliases are taken into account
  """
  def resolve_all_columns(tables, context) do
    context_tables_columns = Enum.flat_map(context, fn {db, _config} ->
      {:ok, tables} = ExoSQL.schema(db, context)
      Enum.flat_map(tables, fn table ->
        {:ok, schema} = ExoSQL.schema(db, table, context)
        Enum.map(schema[:columns], &({db, table, &1}))
      end)
    end)

    Enum.flat_map(tables, &resolve_columns(&1, context_tables_columns))
  end

  # if alias is for a function, also re-alias column names for ease of use (NON SQL)
  def resolve_columns({:alias, {{:fn, _} = aliased, alias_}}, context_tables_columns) do
    columns = resolve_columns(aliased, context_tables_columns)
    Enum.map(columns, fn {_, _, _} ->
      {:tmp, alias_, alias_}
    end)
  end
  def resolve_columns({:alias, {aliased, alias_}}, context_tables_columns) do
    columns = resolve_columns(aliased, context_tables_columns)
    # Logger.debug("Get column from aliased #{inspect {aliased, alias_}}: #{inspect columns}")
    Enum.map(columns, fn {_, _, column} ->
      {:tmp, alias_, column}
    end)
  end
  def resolve_columns({:fn, {function, _params}}, _context_tables_columns) do
    [{:tmp, function, function}]
  end
  def resolve_columns({nil, table}, context_tables_columns) do
    columns = Enum.flat_map(context_tables_columns, fn
      {db, ^table, column} -> [{db, table, column}]
      _other -> []
    end)
    # Logger.debug("Get column from table #{inspect table}: #{inspect columns}")
    columns
  end
  def resolve_columns({:select, query}, _context_tables_columns) do
    {columns, _} = Enum.reduce(query[:select], {[], 1}, fn column, {acc, count} ->
      # Logger.debug("Resolve column name for: #{inspect column}")
      column = case column do
        {:column, {_db, _table, column}} -> {:tmp, :tmp, column}
        {:alias, {_, alias_}} -> {:tmp, :tmp, alias_}
        _expr ->
          {:tmp, :tmp, "col_#{count}"}
      end
      # Logger.debug("Resolved: #{inspect column}")

      {acc ++ [column], count+1}
    end)
    # Logger.debug("Get column from select #{inspect query[:select]}: #{inspect columns}")
    columns
  end
  def resolve_columns({:table, {db, table}}, context_tables_columns) do
    resolve_columns({db, table}, context_tables_columns)
  end
  def resolve_columns({db, table}, context_tables_columns) do
    columns = Enum.flat_map(context_tables_columns, fn
      {^db, ^table, column} -> [{db, table, column}]
      _other -> []
    end)
    # Logger.debug("Get column from table #{inspect {db,table}}: #{inspect columns}")
    columns
  end
  def resolve_columns(%ExoSQL.Query{  } = q, _context_tables_columns) do
    get_query_columns(q)
  end

  def resolve_table({:table, {nil, name}}, context) when is_binary(name) do
    options = Enum.flat_map(context, fn {dbname, _db} ->
      {:ok, tables} = ExoSQL.schema(dbname, context)
      tables
        |> Enum.filter(&(&1 == name))
        |> Enum.map(&{dbname, &1})
    end)


    case options do
      [table] -> table
      l when length(l) == 0 -> throw {:not_found, name}
      _other -> throw {:ambiguous_table_name, name}
    end
  end
  def resolve_table({:table, {_db, _name} = orig} , _context), do: orig
  def resolve_table({:select, query}, context) do
    {:ok, parsed} = real_parse(query, context)
    parsed
  end
  def resolve_table({:fn, _function} = orig, _context), do: orig
  def resolve_table({:alias, {table, alias_}}, context) do
    {:alias, {resolve_table(table, context), alias_}}
  end

  @doc ~S"""
  From the list of tables, and context, and an unknown column, return the
  FQN of the column.
  """
  def resolve_column({:column, {nil, nil, column}}, schema) do
    found = Enum.filter(schema, fn
      {_db, _table, ^column} -> true
      _other -> false
    end)

    found = case found do
      [one] -> one
      [] -> throw {:not_found, column, :in, schema}
      _many -> throw {:ambiguous_column, column, :in, schema}
    end

    if found do
      {:column, found}
    else
      throw {:not_found, column, :in, schema}
    end
  end

  def resolve_column({:column, {nil, table, column}}, schema) do
    found = Enum.find(schema, fn
      {_db, ^table, ^column} -> true
      _other -> false
    end)

    if found do
      {:column, found}
    else
      throw {:not_found, {table, column}, :in, schema}
    end
  end
  def resolve_column({:column, _} = column, _schema), do: column

  def resolve_column({:op, {op, ex1, ex2}}, schema) do
    {:op, {op, resolve_column(ex1, schema), resolve_column(ex2, schema)}}
  end

  def resolve_column({:distinct, expr}, schema) do
    {:distinct, resolve_column(expr, schema)}
  end

  def resolve_column({:case, list}, schema) do
    list = Enum.map(list, fn {c, e} ->
      {resolve_column(c, schema), resolve_column(e, schema)}
    end)
    {:case, list}
  end

  def resolve_column({:fn, {f, params}}, schema) do
    params = Enum.map(params, &resolve_column(&1, schema))
    {:fn, {f, params}}
  end

  def resolve_column({:alias, {expr, alias_}}, schema) do
    {:alias, {resolve_column(expr, schema), alias_}}
  end

  def resolve_column(other, _schema) do
    other
  end

  defp get_query_columns(%ExoSQL.Query{ select: select }), do: get_column_names_or_alias(select, 1)
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
