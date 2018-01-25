Nonterminals
query
  select
  from table_list
  where expr expr_list
  column table groupby
  join join_type.

Terminals
id comma dot lit int op open_par close_par
'SELECT' 'FROM' 'INNER' 'CROSS' 'JOIN' 'ON' 'WHERE' 'GROUP' 'BY'
.

Rootsymbol query.

query -> select from join where groupby: #{select => '$1', from => '$2', join => '$3', where => '$4', groupby => '$5'}.

select -> 'SELECT' expr_list : '$2'.

from -> 'FROM' table_list : '$2'.
table_list -> table : [{table, '$1'}].
table_list -> table comma table_list : [{table, '$1'}] ++ '$3'.

join -> '$empty' : [].
join -> join_type table 'ON' expr join : [{'$1', {'$2', '$4'}}] ++ '$5'.

join_type -> 'JOIN' : inner_join.
join_type -> 'INNER' 'JOIN' : inner_join.
join_type -> 'CROSS' 'JOIN' : cross_join.

where -> '$empty' : nil.
where -> 'WHERE' expr : '$2'.

groupby -> '$empty' : nil.
groupby -> 'GROUP' 'BY' expr_list : '$3'.

expr_list -> expr : ['$1'].
expr_list -> expr comma expr_list: ['$1'] ++ '$3'.

expr -> column : {column, '$1'}.
expr -> lit : {lit, unwrap('$1')}.
expr -> open_par expr close_par : '$2'.
expr -> expr op expr: {op, {unwrap('$2'), '$1', '$3'}}.
expr -> id open_par expr_list close_par : {fn, {unwrap('$1'), '$3'}}.
expr -> id open_par op close_par: tag('$3', "*"), {fn, {unwrap('$1'), [{lit, "*"}]}}.

column -> id dot id dot id : {unwrap('$1'), unwrap('$3'), unwrap('$5')}.
column -> id dot id : {nil, unwrap('$1'), unwrap('$3')}.
column -> id : {nil, nil, unwrap('$1')}.
table -> id dot id : {unwrap('$1'), unwrap('$3')}.
table -> id : {nil, unwrap('$1')}.

select -> column comma select: [unwrap('$1')] ++ '$3'.

Erlang code.

unwrap({_,_,V}) -> 'Elixir.List':to_string(V).
tag(A, B) ->
  A1 = unwrap(A),
  %% io:format("DEBUG: ~p == ~p", [A1, B]),
  A2 = 'Elixir.String':upcase(A1),
  A2 = 'Elixir.List':to_string(B).
