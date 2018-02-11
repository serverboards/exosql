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
    # Logger.debug("Real parse #{inspect parsed}")
    %{
      select: select,
      from: from,
      where: where,
      groupby: groupby,
      join: join,
      orderby: orderby
    } = parsed

    from = Enum.map(from, &resolve_table(&1, context))

    groupby = if groupby do
      Enum.map(groupby, &resolve_column(&1, from, context))
    else nil end

    all_tables = if join != [] do
      from ++ Enum.map(join, fn {_type, {table, _expr}} -> resolve_table(table, context) end)
    else
      from
    end
    # Logger.debug("All tables #{inspect all_tables}")
    join = Enum.map(join, fn {type, {table, expr}} ->
      {type, {
        resolve_table(table, context),
        resolve_column(expr, all_tables, context)
      }}
    end)


    # the resolve all expressions as we know which tables to use
    select = case select do
      [{:all_columns}] ->
        Enum.flat_map(all_tables, fn
          %ExoSQL.Query{ select: select } ->
            Enum.with_index(select) |> Enum.map(fn {_, col} -> {:column, col} end)
          {db, table} ->
            {:ok, %{ columns: columns }} = ExoSQL.schema(db, table, context)
            # Enum.with_index(columns) |> Enum.map(fn {_, col} -> {:column, col} end)
            Enum.map(columns, &{:column, {db, table, &1}})
        end)
      _other  ->
        Enum.map(select, &resolve_column(&1, all_tables, context))
    end


    where = if where do
      resolve_column(where, from, context)
    else nil end

    # Resolve orderby
    orderby = Enum.map(orderby, fn
      {type, expr} ->
        {type, resolve_column(expr, all_tables, context)}
    end)

    {:ok, %ExoSQL.Query{
      select: select,
      from: from, # all the tables it gets data from, but use only the frist and the joins.
      where: where,
      groupby: groupby,
      join: join,
      orderby: orderby
    }}
  end

  @doc """
  Parses an SQL statement and returns the parsed ExoSQL struct.
  """
  def parse(sql, context) do
    try do
      sql = String.to_charlist(sql)
      {:ok, lexed, _lines} = :sql_lexer.string(sql)
      {:ok, parsed} = :sql_parser.parse(lexed)
      real_parse(parsed, context)
    catch
      any -> {:error, any}
    end
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

  def resolve_column({:column, {nil, nil, column}}, tables, context) do
    matches = Enum.flat_map(tables, fn {db, table} ->
      {:ok, table_schema} = ExoSQL.schema(db, table, context)
      Enum.flat_map(table_schema.columns, fn name ->
        if name == column do
          [{:column, {db, table, name}}]
        else
          []
        end
      end)
    end)

    case matches do
      [{:column, data}] -> {:column, data}
      l when length(l) == 0 -> throw {:not_found, column}
      _other -> throw {:ambiguous_column_name, column}
    end
  end

  def resolve_column({:column, {nil, table, column}}, tables, _context) do
    # Logger.debug("Look for #{table}.#{column} at #{inspect tables}")
    matches = Enum.flat_map(tables, fn
      {db, ^table} ->
        [{:column, {db, table, column}}]
      _other ->
        []
    end)

    case matches do
      [{:column, data}] -> {:column, data}
      l when length(l) == 0 ->
        throw {:not_found, {table, column}}
      _other -> throw {:ambiguous_column_name, {table, column}}
    end
  end
  def resolve_column({:column, _} = column, _tables, _context), do: column

  def resolve_column({:op, {op, ex1, ex2}}, tables, context) do
    {:op, {op, resolve_column(ex1, tables, context), resolve_column(ex2, tables, context)}}
  end

  def resolve_column({:fn, {f, params}}, tables, context) do
    params = Enum.map(params, &resolve_column(&1, tables, context))
    {:fn, {f, params}}
  end

  def resolve_column(other, _tables, _context) do
    other
  end
end
