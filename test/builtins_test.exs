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
  end
end
