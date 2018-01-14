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
      groupby: nil,
      join: nil,
    ]
  end

  defmodule Result do
    defstruct [
      columns: [],
      rows: []
    ]
  end

  def parse(sql, context), do: ExoSQL.Parser.parse(sql, context)
  def execute(sql, context), do: ExoSQL.Executor.execute(sql, context)

  def query(sql, context) do
    # Logger.debug(inspect sql)
    {:ok, parsed} = ExoSQL.Parser.parse(sql, context)
    ExoSQL.Executor.execute(parsed, context)
  end

  def explain(sql, context) do
    Logger.info("Explain #{inspect sql}")
    {:ok, parsed} = ExoSQL.Parser.parse(sql, context)
    {:ok, plan} = ExoSQL.Parser.plan(parsed, context)
    Logger.info(inspect plan, pretty: true)
  end

  def format_result(res), do: ExoSQL.Utils.format_result(res)

  def schema(db, context) do
    {db, opts} = context[db]

    apply(db, :schema, [opts])
  end
  def schema(db, table, context) do
    {db, opts} = context[db]

    apply(db, :schema, [opts, table])
  end
end
