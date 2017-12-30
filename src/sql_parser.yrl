Nonterminals
query
  select select_list
  from table_list
  where expr expr_list
  column table groupby.

Terminals
id comma dot lit int op open_par close_par.

Rootsymbol query.

query -> select from where: {'$1', '$2', '$3', nil}.
query -> select from: {'$1', '$2', nil, nil}.
query -> select from where groupby: {'$1', '$2', '$3', '$4'}.
query -> select from groupby: {'$1', '$2', nil, '$3'}.

select -> id select_list : tag('$1', "SELECT"), '$2'.
select_list -> expr : ['$1'].
select_list -> expr comma select_list : ['$1'] ++ '$3'.

from -> id table_list : tag('$1', "FROM"), '$2'.
table_list -> table : [{table, '$1'}].
table_list -> table comma table_list : [{table, '$1'}] ++ '$3'.

where -> id expr_list : tag('$1',"WHERE"), '$2'.

groupby -> id id expr_list : tag('$1',"GROUP"), tag('$2', "BY"), '$3'.

expr_list -> expr : ['$1'].
expr_list -> expr comma expr_list: ['$1'] ++ '$3'.

expr -> column : {column, '$1'}.
expr -> open_par expr close_par : '$2'.
expr -> id open_par expr_list close_par : {fn, {unwrap('$1'), '$3'}}.
expr -> lit : {lit, unwrap('$1')}.
expr -> expr op expr: {op, {unwrap('$2'), '$1', '$3'}}.
expr -> id open_par op close_par: tag('$3', "*"), {fn, {unwrap('$1'), ["*"]}}.

column -> id dot id dot id : {unwrap('$1'), unwrap('$3'), unwrap('$5')}.
table -> id dot id : {unwrap('$1'), unwrap('$3')}.

select -> column comma select: [unwrap('$1')] ++ '$3'.

Erlang code.

unwrap({_,_,V}) -> 'Elixir.List':to_string(V).
tag(A, B) ->
  A1 = unwrap(A),
  %% io:format("DEBUG: ~p == ~p", [A1, B]),
  A2 = string:uppercase(A1),
  A2 = 'Elixir.List':to_string(B).
