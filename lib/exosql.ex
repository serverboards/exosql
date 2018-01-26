require Logger

defmodule ExoSQL do
  @moduledoc """
  Creates a Generic universal parser that can access many tabular databases,
  and perform SQL queries.

  The databases can be heterogenic, so you can perform searches mixing
  data from postgres, mysql, csv or Google Analytics.

  For example:

  ```
    iex> {:ok, result} = ExoSQL.query(
    ...>   "SELECT urls.url, status_code FROM urls INNER JOIN request ON request.url = urls.url",
    ...>    %{
    ...>      "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    ...>      "B" => {ExoSQL.HTTP, []}
    ...>    })
    ...> ExoSQL.format_result(result)
    '''
    A.urls.url | B.request.status_code
    -------------------------------------
    https://serverboards.io/e404 | 404
    http://www.facebook.com | 302
    https://serverboards.io | 200
    http://www.serverboards.io | 301
    http://www.google.com | 302
    ''' |> to_string

  ```

  It also contains functions for all the steps of the process:
  `parse` |> `plan` |> `execute`. They can be useful for debugging pourposes.

  Finally there are helper functions as `explain` that prints out an explanation
  of the plan, and `format_result` for pretty printing results.
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
  def plan(parsed, context), do: ExoSQL.Planner.plan(parsed, context)
  def execute(plan, context), do: ExoSQL.Executor.execute(plan, context)

  def query(sql, context) do
    # Logger.debug(inspect sql)
    with {:ok, parsed} <- ExoSQL.Parser.parse(sql, context),
         {:ok, plan} <- ExoSQL.Planner.plan(parsed) do
         ExoSQL.Executor.execute(plan, context)
    end
    # Logger.debug("parsed #{inspect parsed, pretty: true}")
    # Logger.debug("planned #{inspect plan, pretty: true}")
  end

  def explain(sql, context) do
    Logger.info("Explain #{inspect sql}")
    {:ok, parsed} = ExoSQL.Parser.parse(sql, context)
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
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
