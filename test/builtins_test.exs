require Logger

defmodule ExoSQL.BuiltinsTest do
  use ExUnit.Case
  doctest ExoSQL.Builtins
  @moduletag :capture_log

  test "Datetime options" do
    # 01:02:03 AM
    orig_time = 60 * 60 * 1 + 60 * 2 + 3
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

    dt = ExoSQL.DateTime.to_datetime("2018-07-01T04:25:50Z")
    assert dt == ExoSQL.DateTime.to_datetime("2018-07-01T06:25:50+02:00")
  end

  test "String substr" do
    assert ExoSQL.Builtins.substr("test", 1) == "est"
    assert ExoSQL.Builtins.substr("test", 1, 2) == "es"
    assert ExoSQL.Builtins.substr("test", 1, -2) == "e"
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

    assert ExoSQL.Builtins.format("%.k €", 0.00) == "0.00 €"
    assert ExoSQL.Builtins.format("%.k €", 0.53) == "0.53 €"
    assert ExoSQL.Builtins.format("%.k €", 2.53) == "2.53 €"
    assert ExoSQL.Builtins.format("%.k €", 24.53) == "24.53 €"
    assert ExoSQL.Builtins.format("%.k €", 200.53) == "200.53 €"
    assert ExoSQL.Builtins.format("%.k €", 2_000.53) == "2000 €"
    assert ExoSQL.Builtins.format("%.k €", 20_200.53) == "20200 €"
    assert ExoSQL.Builtins.format("%.k €", 200_400.53) == "200.4K €"
    assert ExoSQL.Builtins.format("%.k €", 2_200_000.53) == "2.2M €"

    assert ExoSQL.Builtins.format("%,k €", 0.00) == "0 €"
    assert ExoSQL.Builtins.format("%,k €", 0.53) == "0,53 €"
    assert ExoSQL.Builtins.format("%,k €", 2.53) == "2,53 €"
    assert ExoSQL.Builtins.format("%,k €", 24.53) == "24,53 €"
    assert ExoSQL.Builtins.format("%,k €", 81.50) == "81,50 €"
    assert ExoSQL.Builtins.format("%,k €", 200.53) == "200,53 €"
    assert ExoSQL.Builtins.format("%,k €", 2_000.53) == "2.000 €"
    assert ExoSQL.Builtins.format("%,k €", 20_200.53) == "20.200 €"
    assert ExoSQL.Builtins.format("%,k €", 200_400.53) == "200,4K €"
    assert ExoSQL.Builtins.format("%,k €", 200_000.53) == "200K €"
    assert ExoSQL.Builtins.format("%,k €", 2_001_000.53) == "2MM €"
    assert ExoSQL.Builtins.format("%,k €", 2_200_000.53) == "2,2MM €"
  end

  test "datediff" do
    assert ExoSQL.DateTime.datediff("2018-01-01", "2018-02-01", "months") == 1
    assert ExoSQL.DateTime.datediff("2018-01-01", "2018-02-01", "years") == 0
    assert ExoSQL.DateTime.datediff("2018-01-01", "2018-02-01", "seconds") == 31 * 24 * 60 * 60
    assert ExoSQL.DateTime.datediff("2018-01-01", "2018-02-01", "days") == 31
    assert ExoSQL.DateTime.datediff("2018-01-01", "2018-02-01", "weeks") == 4

    assert ExoSQL.DateTime.datediff("2017-01-01", "2018-02-01", "months") == 13
    assert ExoSQL.DateTime.datediff("2017-01-01", "2018-02-01", "years") == 1

    assert ExoSQL.DateTime.datediff("2018-01-01", "2017-02-01", "months") == -11
    assert ExoSQL.DateTime.datediff("2018-01-01", "2017-02-01", "years") == -1

    assert ExoSQL.DateTime.datediff("2018-03-01", "2018-02-01", "seconds") == -2_419_200

    assert ExoSQL.DateTime.datediff(ExoSQL.Builtins.range("2018-01-01", "2018-01-02"), "seconds") ==
             24 * 60 * 60

    assert ExoSQL.DateTime.datediff(ExoSQL.Builtins.range("2018-01-01", "2018-01-02")) == 1
    assert ExoSQL.DateTime.datediff(ExoSQL.Builtins.range("2018-01-01", "2018-01-08")) == 7

    assert ExoSQL.DateTime.datediff(ExoSQL.Builtins.range("2018-01-01", "2018-01-08"), "days") ==
             7

    assert ExoSQL.DateTime.datediff(ExoSQL.Builtins.range("2018-01-01", "2018-01-08"), "seconds") ==
             7 * 24 * 60 * 60
  end

  test "format string" do
    assert ExoSQL.Format.format("%02d-%02d-%02d", [2018, 10, 1]) == "2018-10-01"
    assert ExoSQL.Format.format("%+d %+d %+d", [2018, "-10", "0"]) == "+2018 -10 0"

    assert ExoSQL.Format.format("%+f %+f %+f %+f", [2018, -0.43, 0.43, 0]) ==
             "+2018.00 -0.43 +0.43 0.00"

    assert ExoSQL.Format.format("%10s|%-5s|%3s", ["spaces", "hash", "slash"]) ==
             "    spaces|hash |slash"
  end

  test "Builtin simplifications" do
    assert ExoSQL.Builtins.simplify("json", [{:lit, nil}]) == {:lit, nil}
    assert ExoSQL.Builtins.simplify("json", [{:lit, "1"}]) == {:lit, 1}

    assert ExoSQL.Builtins.simplify("regex", [{:column, {:tmp, :tmp, "a"}}, {:lit, ".*"}]) ==
             {:fn,
              {{ExoSQL.Builtins, :regex, "regex"},
               [column: {:tmp, :tmp, "a"}, lit: ~r/.*/, lit: false]}}

    assert ExoSQL.Builtins.simplify("jp", [{:column, {:tmp, :tmp, "a"}}, {:lit, "a/b/c"}]) ==
             {:fn,
              {{ExoSQL.Builtins, :jp, "jp"}, [column: {:tmp, :tmp, "a"}, lit: ["a", "b", "c"]]}}

    assert ExoSQL.Builtins.simplify("format", [
             {:lit, "W%02, %,k €"},
             {:column, {:tmp, :tmp, "a"}},
             {:column, {:tmp, :tmp, "b"}}
           ]) ==
             {:fn,
              {{ExoSQL.Builtins, :format, "format"},
               [
                 lit: [" €", {",", "k"}, "W%02, "],
                 column: {:tmp, :tmp, "a"},
                 column: {:tmp, :tmp, "b"}
               ]}}
  end

  test "Duration parsing IS(8601)" do
    duration = ExoSQL.DateTime.Duration.parse!("P1Y2M3D4WT10H20M30S")

    assert duration.years == 1
    assert duration.months == 2
    # may be more than a month, but I dont know how much, so I will add days.
    assert duration.days == 7 * 4 + 3

    assert duration.seconds == 10 * 60 * 60 + 20 * 60 + 30

    duration = ExoSQL.DateTime.Duration.parse!("-T10M")
    assert duration.seconds == -10 * 60

    duration = ExoSQL.DateTime.Duration.parse!("1Y")
    assert duration == %ExoSQL.DateTime.Duration{years: 1}

    {:error, _} = ExoSQL.DateTime.Duration.parse("nonsense")

    duration = ExoSQL.DateTime.Duration.parse!("-30DT30M")
    assert duration == %ExoSQL.DateTime.Duration{days: -30, seconds: -30 * 60}
  end

  test "Add durations" do
    date = ExoSQL.DateTime.to_datetime("2016-02-01T10:30:00")

    duration = ExoSQL.DateTime.Duration.parse!("1D")
    ndate = ExoSQL.DateTime.Duration.datetime_add(date, duration)
    assert date.year == ndate.year
    assert date.month == ndate.month
    assert date.day + 1 == ndate.day
    assert date.hour == ndate.hour
    assert date.minute == ndate.minute
    assert date.second == ndate.second

    ndate = ExoSQL.DateTime.Duration.datetime_add(date, "1Y1M")
    assert date.year + 1 == ndate.year
    assert date.month + 1 == ndate.month
    assert date.day == ndate.day
    assert date.hour == ndate.hour
    assert date.minute == ndate.minute
    assert date.second == ndate.second

    ndate = ExoSQL.DateTime.Duration.datetime_add(date, "T29M10S")
    assert date.year == ndate.year
    assert date.month == ndate.month
    assert date.day == ndate.day
    assert date.hour == ndate.hour
    assert date.minute + 29 == ndate.minute
    assert date.second + 10 == ndate.second

    ndate = ExoSQL.DateTime.Duration.datetime_add(date, "30D")
    assert date.year == ndate.year
    assert date.month + 1 == ndate.month
    assert date.day + 1 == ndate.day
    assert date.hour == ndate.hour
    assert date.minute == ndate.minute
    assert date.second == ndate.second

    ndate = ExoSQL.DateTime.Duration.datetime_add(date, "-31DT2360M20S")
    assert 2015 == ndate.year
    assert 12 == ndate.month
    assert 31 == ndate.day
    assert 19 == ndate.hour
    assert 9 == ndate.minute
    assert 40 == ndate.second
  end

  test "Math test" do
    epsilon = 0.000005
    e = 2.71828

    assert -1 == ExoSQL.Builtins.sign(-32)
    assert -1 == ExoSQL.Builtins.sign("-32")
    assert 1 == ExoSQL.Builtins.sign("32")
    assert 1 == ExoSQL.Builtins.sign(32)

    assert 4 == ExoSQL.Builtins.power(-2, 2)
    assert 4 == ExoSQL.Builtins.power(2, "2")
    assert 4 == ExoSQL.Builtins.power("2", "2")
    assert abs( 2 - ExoSQL.Builtins.power(4, 0.5) ) < epsilon

    assert 2 == ExoSQL.Builtins.sqrt(4)
    assert 4 == ExoSQL.Builtins.sqrt(16)

    assert 1 == ExoSQL.Builtins.log(10)
    assert 2 == ExoSQL.Builtins.log(100)

    assert abs(1 - ExoSQL.Builtins.ln(e)) < epsilon

    assert 0 == ExoSQL.Builtins.mod(10, 2)
    assert 0 == ExoSQL.Builtins.mod(10, 2.5)
    assert 1 == ExoSQL.Builtins.mod(10, 3)
  end
end
