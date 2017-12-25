require Logger

defmodule ParserTest do
  use ExUnit.Case
  doctest Esql

  test "Lex and parse" do
    {:ok, res, 1} = :sql_lexer.string('SELECT A.products.name, A.products.stock FROM A.products WHeRE (A.products.price > 0) and (a.products.stock >= 1)')
    Logger.debug("Lexed: #{inspect res}")

    {:ok, res} = :sql_parser.parse(res)

    Logger.debug("Parsed: #{inspect res}")

  end

end
