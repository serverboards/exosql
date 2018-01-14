defmodule ExoSQL.CrossJoinTables do
  alias ExoSQL.CrossJoinTables
  
  defstruct [
    headers: [], # List of list of headers
    rows: []  # List of list of rows
  ]
  def new([]) do
    %CrossJoinTables{}
  end
  def new([first | rest]) do
    rest = new(rest)
    %CrossJoinTables{
      headers: first.headers ++ rest.headers,
      rows: [first.rows] ++ rest.rows
    }
  end

  defimpl Enumerable do
    defp count([head | tail], acc) do
      # Logger.debug("Count #{inspect head} #{inspect acc}")
      count( tail, Enum.count(head) * acc )
    end
    defp count([], acc), do: acc

    def count(%CrossJoinTables{ rows: rows}) do
      {:ok, count(rows, 1)}
    end
    def count(%CrossJoinTables{ rows: []}), do: 0


    def reduce(_,       {:halt, acc}, _fun),   do: {:halted, acc}
    def reduce(list,    {:suspend, acc}, fun), do: {:suspended, acc, &reduce(list, &1, fun)}


    def reduce(%CrossJoinTables{ rows: []}, {:cont, acc}, _fun),   do: {:done, acc}
    def reduce(%CrossJoinTables{ rows: rows }, {:cont, acc}, fun) do
      state = for [hrow | trow] <- rows do
        {[hrow], trow}
      end
      firstr = reducer_current_row(state)


      # Logger.debug("First row: #{inspect firstr}")
      # first all of reducer, then recurse
      reduce(state, fun.(firstr, acc), fun )
    end

    def reduce(state, {:cont, acc}, fun) do
      # Logger.debug("Reduce state #{inspect state}")
      nextstate = reducer_nextstate(state)
      # Logger.debug("Next state #{inspect nextstate}")

      case nextstate do
        :empty -> {:stop, acc}
        other ->
          crow = reducer_current_row(nextstate)
          # Logger.debug("Generated row is: #{inspect crow}")

          reduce(nextstate, fun.(crow, acc), fun )
      end
    end

    def reducer_current_row(state) do
      Enum.flat_map(state, fn {[hrow | _rest], _trow} -> hrow end)
    end

    def reducer_rotate_rstate({head, [prev | next]}) do
      {[prev | head], next} # I pass one from next to prev head
    end
    def reducer_reset_rstate([]), do: []
    def reducer_reset_rstate([{next, []} | tail]) do
      [h | t] = Enum.reverse(next) # was reversed during processing forperfomance reasons
      [{[h], t} | reducer_reset_rstate(tail)]
    end

    def reducer_nextstate([{prev, []}]) do
      :empty
    end
    def reducer_nextstate([rstate]) do
      [reducer_rotate_rstate(rstate)]
    end
    def reducer_nextstate([head | rest]) do
      case reducer_nextstate(rest) do
        :empty ->
          # Logger.debug("Empty at #{inspect head}")
          {prev, next} = head
          if next == [] do
            :empty
          else
            [reducer_rotate_rstate(head) | reducer_reset_rstate(rest)]
          end
        rest ->
          [head | rest]
      end
    end

  end

end
