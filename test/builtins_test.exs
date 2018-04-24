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
    # Logger.debug(inspect parsed)
    assert ExoSQL.Builtins.jp(parsed, "host") == "serverboards.io"
    assert ExoSQL.Builtins.jp(parsed, "query/q") == "test"
    assert ExoSQL.Builtins.urlparse(urlq, "query/utm_campaign") == "exosql"

    assert ExoSQL.Builtins.urlparse(nil, "query/utm_campaign") == nil


    assert ExoSQL.Builtins.urlparse("https://www.google.com", "domain") == "google"
    assert ExoSQL.Builtins.urlparse("https://linktr.ee", "domain") == "linktr"
    assert ExoSQL.Builtins.urlparse("https://www.google.co.uk", "domain") == "google"
    assert ExoSQL.Builtins.urlparse("https://beta.serverboards.io", "domain") == "serverboards"
    assert ExoSQL.Builtins.urlparse("https://www.csail.mit.edu/", "domain") == "mit"
    assert ExoSQL.Builtins.urlparse("https://en.wikipedia.org/", "domain") == "wikipedia"

  end

  test "format test" do
    assert ExoSQL.Builtins.format("%d €", 2.22) == "2 €"
    assert ExoSQL.Builtins.format("%.2f €", 2) == "2.00 €"

    assert ExoSQL.Builtins.format("%k €", 2) == "2 €"
    assert ExoSQL.Builtins.format("%k €", 2.33) == "2 €"
    assert ExoSQL.Builtins.format("%k €", 2000) == "2k €"
    assert ExoSQL.Builtins.format("%k €", 22_000) == "22k €"
    assert ExoSQL.Builtins.format("%k €", 222_000) == "222k €"
    assert ExoSQL.Builtins.format("%k €", 2_000_000) == "2M €"

    assert ExoSQL.Builtins.format("%.k €",         0.00) == "0.00 €"
    assert ExoSQL.Builtins.format("%.k €",         0.53) == "0.53 €"
    assert ExoSQL.Builtins.format("%.k €",         2.53) == "2.53 €"
    assert ExoSQL.Builtins.format("%.k €",        24.53) == "24.53 €"
    assert ExoSQL.Builtins.format("%.k €",       200.53) == "200.53 €"
    assert ExoSQL.Builtins.format("%.k €",     2_000.53) == "2000 €"
    assert ExoSQL.Builtins.format("%.k €",    20_200.53) == "20200 €"
    assert ExoSQL.Builtins.format("%.k €",   200_400.53) == "200.4K €"
    assert ExoSQL.Builtins.format("%.k €", 2_200_000.53) == "2.2M €"

    assert ExoSQL.Builtins.format("%,k €",         0.00) == "0 €"
    assert ExoSQL.Builtins.format("%,k €",         0.53) == "0,53 €"
    assert ExoSQL.Builtins.format("%,k €",         2.53) == "2,53 €"
    assert ExoSQL.Builtins.format("%,k €",        24.53) == "24,53 €"
    assert ExoSQL.Builtins.format("%,k €",        81.50) == "81,50 €"
    assert ExoSQL.Builtins.format("%,k €",       200.53) == "200,53 €"
    assert ExoSQL.Builtins.format("%,k €",     2_000.53) == "2.000 €"
    assert ExoSQL.Builtins.format("%,k €",    20_200.53) == "20.200 €"
    assert ExoSQL.Builtins.format("%,k €",   200_400.53) == "200,4K €"
    assert ExoSQL.Builtins.format("%,k €",   200_000.53) == "200K €"
    assert ExoSQL.Builtins.format("%,k €", 2_001_000.53) == "2MM €"
    assert ExoSQL.Builtins.format("%,k €", 2_200_000.53) == "2,2MM €"
  end
end
