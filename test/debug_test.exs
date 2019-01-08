require Logger

defmodule DebugTest do
  use ExUnit.Case
  import ExUnit.CaptureLog
  @moduletag :capture_log
  @moduletag timeout: 5_000

  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
    "__vars__" => %{"debug" => true}
  }

  def analyze_query!(query, context \\ @context) do
    Logger.debug("Query is:\n\n#{query}")
    {:ok, parsed} = ExoSQL.parse(query, context)
    Logger.debug("Parsed is #{inspect(parsed, pretty: true)}")
    {:ok, plan} = ExoSQL.Planner.plan(parsed)
    Logger.debug("Plan is #{inspect(plan, pretty: true)}")
    {:ok, result} = ExoSQL.Executor.execute(plan, context)
    # Logger.debug("Raw result is #{inspect(result, pretty: true)}")
    Logger.debug("Result:\n#{ExoSQL.format_result(result)}")
    result
  end

  test "Simple parse SQL" do
    assert capture_log(fn ->
             analyze_query!("SELECT * FROM purchases")
           end) =~ "ExoSQL Executor"

    flunk(1)
  end
end
