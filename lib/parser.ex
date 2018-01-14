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
    Logger.debug("#{inspect lexed, pretty: true}")
    {:ok, parsed} = :sql_parser.parse(lexed)
    Logger.debug("parsed #{inspect parsed, pretty: true}")
    %{select: select, from: initial_from, where: where, groupby: groupby, join: join} = parsed

    # first resolve all tables
    # convert from to cross joins
    [from | rest_from] = initial_from
    from = resolve_table(from, context)
    {from_tables, cross_joins} = Enum.reduce(rest_from, {[from], []}, fn {from_tables, cjoins}, {:table, table} ->
      table = resolve_table(table, context)
      from_tables = from_tables ++ [table]
      join_expr = {:lit, true} # always merge the cross join, WHERE will resolve as required later.
      cjoins = cjoins ++ [{:cross_join, {table, join_expr}}]
      {from_tables, cjoins}
    end)
    Logger.debug("From #{inspect from}, cross join #{inspect cross_joins}")

    groupby = if groupby do
      Enum.map(groupby, &resolve_column(&1, from_tables, context))
    else nil end
    join = Enum.map(join, fn {type, {table, expr}} ->
      {type, {
        resolve_table(table, context),
        expr
      }}
    end)

    # the resolve all expressions as we know which tables to use
    select = Enum.map(select, &resolve_column(&1, from_tables, context))
    where = if where do
      resolve_column(where, from_tables, context)
    else nil end

    {:ok, %ExoSQL.Query{
      select: select,
      from: from_tables, # all the tables it gets data from, but use only the frist and the joins.
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
        |> Enum.map(&{:table, {dbname, &1}})
    end)


    case options do
      [table] -> table
      l when length(l) == 0 -> throw {:not_found, name}
      other -> throw {:ambiguous_table_name, name}
    end
  end
  def resolve_table({:table, {db, name}} = orig, context), do: orig

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
