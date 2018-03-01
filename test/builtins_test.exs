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

    # all formats
    dt = ExoSQL.DateTime.to_datetime("2018-02-22")
    assert dt == ExoSQL.DateTime.to_datetime("2018-02-22 00:00")
    assert dt == ExoSQL.DateTime.to_datetime("2018-02-22T00:00")
    assert dt == ExoSQL.DateTime.to_datetime("2018-02-22T00:00:00")
    assert dt == ExoSQL.DateTime.to_datetime("2018-02-22 00:00:00")
  end

  test "String substr" do
    assert ExoSQL.Builtins.substr("test", 1) == "est"
    assert ExoSQL.Builtins.substr("test", 1,2) == "es"
    assert ExoSQL.Builtins.substr("test", 1,-2) == "e"
    assert ExoSQL.Builtins.substr(nil, 1) == ""

    dt = ExoSQL.Builtins.to_datetime("2018-02-10T11:54:34")
    assert ExoSQL.Builtins.substr(dt, 0, 10) == "2018-02-10"
  end

  test "IF test" do
    assert ExoSQL.Builtins.if_(true, "test", 1) == "test"
    assert ExoSQL.Builtins.if_(false, "test", 1) == 1
  end

  test "jp test" do
    json = %{
      "first_name" => "Anonymous",
      "last_name" => "--",
      "addresses" => [
        %{
          "street" => "Main Rd"
        },
        %{
          "street" => "Side Rd"
        }
      ],
      "email" => "admin@example.org"
    }

    assert ExoSQL.Builtins.jp(json, "") == json
    assert ExoSQL.Builtins.jp(json, "/first_name") == json["first_name"]
    assert ExoSQL.Builtins.jp(json, "none") == nil
    assert ExoSQL.Builtins.jp(json, "/addresses") == json["addresses"]
    assert ExoSQL.Builtins.jp(json, "/addresses/0/street") == "Main Rd"
    assert ExoSQL.Builtins.jp(json, "/addresses/10/street") == nil
  end

  test "urlparse" do
    url = "https://serverboards.io/download/"
    email = "connect@serverboards.io"
    email2 = "mailto://connect@serverboards.io"
    urlq = "https://serverboards.io/download/?q=test&utm_campaign=exosql"

    assert ExoSQL.Builtins.urlparse(url, "scheme") == "https"
    assert ExoSQL.Builtins.urlparse(url, "host") == "serverboards.io"

    assert ExoSQL.Builtins.urlparse(email, "scheme") == nil
    assert ExoSQL.Builtins.urlparse(email, "host") == nil
    assert ExoSQL.Builtins.urlparse(email, "path") == "connect@serverboards.io"

    assert ExoSQL.Builtins.urlparse(email2, "scheme") == "mailto"
    assert ExoSQL.Builtins.urlparse(email2, "host") == "serverboards.io"
    assert ExoSQL.Builtins.urlparse(email2, "user") == "connect"

    parsed = ExoSQL.Builtins.urlparse(urlq)
    Logger.debug(inspect parsed)
    assert ExoSQL.Builtins.jp(parsed, "host") == "serverboards.io"
    assert ExoSQL.Builtins.jp(parsed, "query/q") == "test"
    assert ExoSQL.Builtins.urlparse(urlq, "query/utm_campaign") == "exosql"

    assert ExoSQL.Builtins.urlparse(nil, "query/utm_campaign") == nil
  end
end
