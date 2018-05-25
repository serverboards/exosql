Nonterminals
  query simple_query complex_query
  with_list with
  select select_expr select_expr_list
  from table_list
  where expr_list
  column table tableid groupby
  join join_type cross_join
  orderby order_expr_list order_expr asc_desc
  limit offset
  expr expr_l2 expr_l3 expr_l4 expr_l5 expr_l6 expr_l7 expr_atom
  case_expr_list case_expr if_expr_list
  .

Terminals
id comma dot lit litn litf var
open_par close_par open_br close_br open_sqb close_sqb
op1 op2 op3 op4 op5 op6
'SELECT' 'FROM' 'AS' 'WITH'
'OUTER' 'LEFT' 'RIGHT' 'INNER' 'CROSS' 'JOIN' 'ON' 'LATERAL'
'WHERE' 'GROUP' 'BY' 'ORDER' 'ASC' 'DESC'
'TRUE' 'FALSE' 'NOT' 'NULL'
'DISTINCT' 'LIMIT' 'ALL' 'OFFSET'
'CASE' 'WHEN' 'THEN' 'ELSE' 'END'
'IF' 'ELIF'
'UNION'
.

Rootsymbol query.

query -> 'WITH' with_list complex_query: maps:put(with, '$2', '$3').
query -> complex_query: '$1'.

complex_query -> simple_query 'UNION' complex_query: maps:put(union, {all, '$3'}, '$1').
complex_query -> simple_query 'UNION' 'ALL' complex_query: maps:put(union, {distinct, '$4'}, '$1').

complex_query -> simple_query: '$1'.

simple_query -> select from join where groupby orderby offset limit:
    #{select => '$1', from => '$2', join => '$3', where => '$4',
      groupby => '$5', orderby => '$6', offset => '$7', limit => '$8', union => nil, with => []}.
simple_query -> select:
    #{select => '$1', from => [], join => [], where => nil, groupby => nil,
      orderby => [], limit => nil, offset => nil, union => nil, with => []}.

with_list -> with: ['$1'].
with_list -> with comma with_list: ['$1' | '$3'].

with -> id 'AS' open_par complex_query close_par: {unwrap('$1'), '$4'}.

select -> 'SELECT' 'DISTINCT' 'ON' open_par expr close_par select_expr_list : {'$7', [{distinct, '$5'}]}.
select -> 'SELECT' 'DISTINCT' select_expr_list : {'$3', [{distinct, all_columns}]}.
select -> 'SELECT' select_expr_list : {'$2', []}.

select_expr_list -> select_expr : ['$1'].
select_expr_list -> select_expr comma select_expr_list: ['$1'] ++ '$3'.
select_expr -> expr: '$1'.
select_expr -> expr 'AS' id: {alias, {'$1', unwrap('$3')}}.
select_expr -> expr id: {alias, {'$1', unwrap('$2')}}.
select_expr -> op5: tag('$1', "*"), {all_columns}.

from -> 'FROM' table_list : '$2'.
table_list -> 'LATERAL' expr: [{lateral, '$2'}].
table_list -> 'LATERAL' expr 'AS' id: [{alias, {{lateral, '$2'}, unwrap('$4')}}].
table_list -> 'LATERAL' expr comma table_list: [{lateral, '$2'}] ++ '$4'.
table_list -> 'LATERAL' expr 'AS' id comma table_list: [{alias, {{lateral, '$2'}, unwrap('$4')}}] ++ '$4'.
table_list -> table : ['$1'].
table_list -> table comma table_list : ['$1'] ++ '$3'.

join -> '$empty' : [].
join -> cross_join table join : [{'$1', '$2'}] ++ '$3'.
join -> join_type table 'ON' expr join : [{'$1', {'$2', '$4'}}] ++ '$5'.

cross_join -> 'CROSS' 'JOIN' 'LATERAL': cross_join_lateral.
cross_join -> 'CROSS' 'JOIN'          : cross_join.

join_type -> 'LEFT'         'JOIN' 'LATERAL': left_join_lateral.
join_type -> 'LEFT' 'OUTER' 'JOIN' 'LATERAL': left_join_lateral.

join_type -> 'JOIN' : inner_join.
join_type -> 'INNER' 'JOIN' : inner_join.
join_type -> 'LEFT' 'JOIN' : left_join.
join_type -> 'LEFT' 'OUTER' 'JOIN' : left_join.
join_type -> 'RIGHT' 'JOIN' : right_join.
join_type -> 'RIGHT' 'OUTER' 'JOIN' : right_join.

where -> '$empty' : nil.
where -> 'WHERE' 'expr' : '$2'.

groupby -> '$empty' : nil.
groupby -> 'GROUP' 'BY' expr_list : '$3'.

orderby -> '$empty' : [].
orderby -> 'ORDER' 'BY' order_expr_list : '$3'.
order_expr_list -> order_expr: ['$1'].
order_expr_list -> order_expr comma order_expr_list: ['$1'] ++ '$3'.
order_expr -> column asc_desc: {'$2', {column, '$1'}}.
order_expr -> litn asc_desc: {'$2', {lit, unwrap_raw('$1')}}.
asc_desc -> '$empty' : asc.
asc_desc -> 'ASC' : asc.
asc_desc -> 'DESC' : desc.

limit -> '$empty': nil.
limit -> 'LIMIT' litn: unwrap_raw('$2').
limit -> 'LIMIT' 'ALL': nil.

offset -> '$empty': nil.
offset -> 'OFFSET' litn: unwrap_raw('$2').

expr_list -> expr : ['$1'].
expr_list -> expr comma expr_list: ['$1'] ++ '$3'.

expr -> expr_l2 op1 expr: {op, {unwrap('$2'), '$1', '$3'}}.
expr -> expr_l2: '$1'.

expr_l2 -> expr_l3 op2 expr_l2: {op, {unwrap('$2'), '$1', '$3'}}.
expr_l2 -> expr_l3: '$1'.

expr_l3 -> expr_l4 op3 expr_l3: {op, {unwrap('$2'), '$1', '$3'}}.
expr_l3 -> expr_l4: '$1'.

expr_l4 -> expr_l5 op4 expr_l4: {op, {unwrap('$2'), '$1', '$3'}}.
expr_l4 -> expr_l5: '$1'.

expr_l5 -> expr_l6 op5 expr_l5: {op, {unwrap('$2'), '$1', '$3'}}.
expr_l5 -> expr_l6: '$1'.

expr_l6 -> expr_l7 op6 expr_l6: {op, {unwrap_u('$2'), '$1', '$3'}}.
expr_l6 -> expr_l7: '$1'.

expr_l7 -> 'NOT' expr_l7: {op, {'not', '$2'}}.
expr_l7 -> expr_atom: '$1'.

expr_atom -> column : {column, '$1'}.
expr_atom -> lit : {lit, unwrap('$1')}.
expr_atom -> litn : {lit, unwrap_raw('$1')}.
expr_atom -> litf : {lit, unwrap_raw('$1')}.
expr_atom -> 'TRUE' : {lit, true}.
expr_atom -> 'FALSE' : {lit, false}.
expr_atom -> 'NULL' : {lit, nil}.
expr_atom -> var : {var, unwrap('$1')}.
expr_atom -> open_par simple_query close_par : {select, '$2'}.
expr_atom -> open_par expr close_par : '$2'.
expr_atom -> id open_par close_par : {fn, {unwrap_d('$1'), []}}.
expr_atom -> id open_par expr_list close_par : {fn, {unwrap_d('$1'), '$3'}}.
expr_atom -> 'JOIN' open_par expr_list close_par : {fn, {'Elixir.List':to_string("join"), '$3'}}.
expr_atom -> id open_par op5 close_par: tag('$3', "*"), {fn, {unwrap_d('$1'), [{lit, "*"}]}}.
expr_atom -> id open_par 'DISTINCT' expr close_par: {fn, {unwrap_d('$1'), [{distinct, '$4'}]}}.
expr_atom -> open_sqb expr_list close_sqb: {list, '$2'}.
expr_atom -> 'CASE' case_expr_list: {'case', '$2'}.
expr_atom -> 'IF' expr 'THEN' expr if_expr_list: {'case', [ {'$2', '$4'} | '$5' ]}.

case_expr_list -> case_expr case_expr_list: ['$1' | '$2'].
case_expr_list -> 'ELSE' expr 'END': [{{lit, true}, '$2'}].
case_expr_list -> 'END': [].

case_expr -> 'WHEN' expr 'THEN' expr: {'$2', '$4'}.

if_expr_list -> 'ELIF' expr 'THEN' expr if_expr_list: [{'$2', '$4'} | '$5'].
if_expr_list -> 'ELSE' expr 'END': [{{lit, true}, '$2'}].
if_expr_list -> 'END': [].


column -> id dot id dot id : {unwrap('$1'), unwrap('$3'), unwrap('$5')}.
column -> id dot id : {nil, unwrap('$1'), unwrap('$3')}.
column -> id : {nil, nil, unwrap('$1')}.

table -> tableid 'AS' id : {alias, {'$1', unwrap('$3')}}.
table -> tableid id : {alias, {'$1', unwrap('$2')}}.
table -> tableid : '$1'.
tableid -> id dot id : {table, {unwrap('$1'), unwrap('$3')}}.
tableid -> id : {table, {nil, unwrap('$1')}}.
tableid -> open_par complex_query close_par : {select, '$2'}.
tableid -> id open_par expr_list close_par : {fn, {unwrap_d('$1'), '$3'}}.

select -> column comma select: [unwrap('$1')] ++ '$3'.

Erlang code.

unwrap_d({_,_,V}) -> 'Elixir.String':downcase('Elixir.List':to_string(V)).
unwrap_u({_,_,V}) -> 'Elixir.String':upcase('Elixir.List':to_string(V)).
unwrap({_,_,V}) -> 'Elixir.List':to_string(V).
unwrap_raw({_,_,V}) -> V.
tag(A, B) ->
  A1 = unwrap(A),
  %% io:format("DEBUG: ~p == ~p", [A1, B]),
  A2 = 'Elixir.String':upcase(A1),
  A2 = 'Elixir.List':to_string(B).
