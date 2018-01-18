require Logger

defmodule ExoSQL.Parser do
  @moduledoc """
  Uses leex and yecc to perform a first phase parsing, and then
  convert an dprocess the structure using more context knowledge to return
  a proper Query struct.
  """
  @doc """
  """
  def parse(sql, context) do
    sql = String.to_charlist(sql)
    {:ok, lexed, _lines} = :sql_lexer.string(sql)
    {:ok, parsed} = :sql_parser.parse(lexed)
    %{select: select, from: from, where: where, groupby: groupby, join: join} = parsed

    # first resolve all tables
    # convert from to cross joins
    from = Enum.map(from, &resolve_table(&1, context))

    groupby = if groupby do
      Enum.map(groupby, &resolve_column(&1, from, context))
    else nil end
    join = Enum.map(join, fn {type, {table, expr}} ->
      {type, {
        resolve_table(table, context),
        expr
      }}
    end)

    # the resolve all expressions as we know which tables to use
    select = Enum.map(select, &resolve_column(&1, from, context))
    where = if where do
      resolve_column(where, from, context)
    else nil end

    {:ok, %ExoSQL.Query{
      select: select,
      from: from, # all the tables it gets data from, but use only the frist and the joins.
      where: where,
      groupby: groupby,
      join: join
    }}
  end


  def get_vars(db, table, [expr | tail]) do
    get_vars(db, table, expr) ++ get_vars(db, table, tail)
  end
  def get_vars(db, table, []), do: []

  def get_vars(db, table, {db, table, column}) do
    [column]
  end
  def get_vars(_db, _table, _other) do
    []
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
      other -> throw {:ambiguous_table_name, name}
    end
  end
  def resolve_table({:table, {db, name} = orig} , context), do: orig

  def resolve_column({:column, {nil, nil, column}}, tables, context) do
    matches = Enum.flat_map(tables, fn {db, table} ->
      {:ok, table_schema} = ExoSQL.schema(db, table, context)
      Enum.flat_map(table_schema.headers, fn name ->
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
      other -> throw {:ambiguous_column_name, column}
    end
  end

  def resolve_column({:column, {nil, table, column}}, tables, context) do
    # Logger.debug("Look for #{table}.#{column} at #{inspect tables}")
    matches = Enum.flat_map(tables, fn
      {db, ^table} ->
        [{:column, {db, table, column}}]
      other ->
        []
    end)

    case matches do
      [{:column, data}] -> {:column, data}
      l when length(l) == 0 ->
        throw {:not_found, {table, column}}
      other -> throw {:ambiguous_column_name, {table, column}}
    end
  end
  def resolve_column({:column, _} = column, _tables, _context), do: column

  def resolve_column({:op, {op, ex1, ex2}}, tables, context) do
    {:op, {op, resolve_column(ex1, tables, context), resolve_column(ex2, tables, context)}}
  end

  def resolve_column(other, _tables, _context) do
    other
  end
end
