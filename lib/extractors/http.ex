require Logger

defmodule ExoSQL.HTTP do
  @moduledoc """
  Example Extractor that performs HTTP requests

  This is a virtual extractor that requires an `url` to operate with.
  """

  def schema(_db) do
    {:ok, ["request"]}
  end

  def schema(_db, "request"), do: {:ok, %{columns: ["url", "status_code", "body"]}}

  def execute(_db, "request", quals, _columns) do
    # Logger.debug("Get request #{inspect quals} #{inspect columns}")

    urls =
      Enum.find_value(quals, [], fn
        {"url", "IN", urls} -> urls
        _other -> false
      end)

    rows =
      Enum.map(urls, fn url ->
        res = HTTPoison.get(url)

        case res do
          {:ok, res} ->
            [url, res.status_code, res.body]

          {:error, error} ->
            [url, 0, IO.inspect(error.reason)]
        end
      end)

    {:ok,
     %{
       columns: ["url", "status_code", "body"],
       rows: rows
     }}
  end
end
