require Logger

defmodule ExoSQL.BuiltinsTest do
  use ExUnit.Case
  doctest ExoSQL.Builtins
  @moduletag :capture_log

  test "Datetime options" do
    orig_time = 60*60*1 + 60*2 + 3 # 01:02:03 AM
    dt = ExoSQL.Builtins.to_datetime(orig_time)

    assert ExoSQL.Builtins.to_string_(dt) == ExoSQL.Builtins.strftime(dt)
    assert ExoSQL.Builtins.to_string_(dt) == ExoSQL.Builtins.strftime(dt, "%i")
    assert "1970-01-01" == ExoSQL.Builtins.strftime(dt, "%Y-%m-%d")
    assert "01:02:03" == ExoSQL.Builtins.strftime(dt, "%H:%M:%S")
    assert "#{orig_time}" == ExoSQL.Builtins.strftime(dt, "%s")
    assert "%s #{orig_time}" == ExoSQL.Builtins.strftime(dt, "%%s %s")
    assert "01" == ExoSQL.Builtins.strftime(dt, "%V")
  end

  test "String substr" do
    assert ExoSQL.Builtins.substr("test", 1) == "est"
    assert ExoSQL.Builtins.substr("test", 1,2) == "es"
    assert ExoSQL.Builtins.substr("test", 1,-2) == "e"
    assert ExoSQL.Builtins.substr(nil, 1) == ""

    dt = ExoSQL.Builtins.to_datetime("2018-02-10T11:54:34")
    assert ExoSQL.Builtins.substr(dt, 0, 10) == "2018-02-10"
  end
end
