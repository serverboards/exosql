require Logger

defmodule ParserTest do
  use ExUnit.Case
  doctest ExoSQL.Parser
  @moduletag :capture_log


  @context %{
    "A" => {ExoSQL.Csv, path: "test/data/csv/"},
  }


  test "Lex and parse" do
    {:ok, res, 1} = :sql_lexer.string('SELECT A.products.name, A.products.stock FROM A.products WHERE (A.products.price > 0) and (a.products.stock >= 1)')
    Logger.debug("Lexed: #{inspect res}")

    {:ok, res} = :sql_parser.parse(res)

    Logger.debug("Parsed: #{inspect res}")
  end

  test "Elixir parsing to proper struct" do
    {:ok, res} = ExoSQL.Parser.parse("SELECT A.products.name, A.products.stock FROM A.products WHERE (A.products.price > 0) and (A.products.stock >= 1)", @context)

    Logger.debug("Parsed: #{inspect res}")
  end

end
