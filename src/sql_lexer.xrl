Definitions.

INT    = [0-9]+
MINUS  = -
RESERVEDL = (select|where|from|as|inner|cross|left|right|outer|join|on|group|by|order|asc|desc|true|false|not|distinct|limit|offset|all|null|case|if|elif|when|then|else|end|union)
RESERVEDU = (SELECT|WHERE|FROM|AS|INNER|CROSS|LEFT|RIGHT|OUTER|JOIN|ON|GROUP|BY|ORDER|ASC|DESC|TRUE|FALSE|NOT|DISTINCT|LIMIT|OFFSET|ALL|NULL|CASE|IF|ELIF|WHEN|THEN|ELSE|END|UNION)
ID     = [_a-zA-Z][_a-zA-Z0-9]*
COMMA  = ,
DOT    = \.
OP1     = (and|AND)
OP2     = (or|OR)
OP3     = (<|>|<=|>=|==|!=|<>|<|>|=|\|\|)
OP4     = (-|\+)
OP5     = (\*|/|%)
OP6     = (IN|IS|LIKE|ILIKE|CASE|in|is|like|ilike)
SPACE  = [\n\t\s]+
OPEN_PAR = \(
CLOSE_PAR = \)
OPEN_SQB = \[
CLOSE_SQB = \]
OPEN_BR = \{
CLOSE_BR = \}
QUOTED_STRING = ("([^"])*"|'([^'])*')
%% "
VAR    =  \$[_a-zA-Z][_a-zA-Z0-9]*

Rules.

{SPACE}  : skip_token.
{RESERVEDL} : {token, {list_to_atom(string:to_upper(TokenChars)), TokenLine}}.
{RESERVEDU} : {token, {list_to_atom(TokenChars), TokenLine}}.
{OP1}     : {token, {op1, TokenLine, TokenChars}}.
{OP2}     : {token, {op2, TokenLine, TokenChars}}.
{OP3}     : {token, {op3, TokenLine, TokenChars}}.
{OP4}     : {token, {op4, TokenLine, TokenChars}}.
{OP5}     : {token, {op5, TokenLine, TokenChars}}.
{OP6}     : {token, {op6, TokenLine, TokenChars}}.
{ID}     : {token, {id, TokenLine, TokenChars}}.
{COMMA}  : {token, {comma, TokenLine, TokenChars}}.
{DOT}    : {token, {dot, TokenLine, TokenChars}}.
{OPEN_PAR}      : {token, {open_par, TokenLine, TokenChars}}.
{CLOSE_PAR}     : {token, {close_par, TokenLine, TokenChars}}.
{OPEN_SQB}      : {token, {open_sqb, TokenLine, TokenChars}}.
{CLOSE_SQB}     : {token, {close_sqb, TokenLine, TokenChars}}.
{OPEN_BR}      : {token, {open_br, TokenLine, TokenChars}}.
{CLOSE_BR}     : {token, {close_br, TokenLine, TokenChars}}.
{INT}{DOT}{INT} : {token, {litf, TokenLine, to_number(TokenChars)}}.
{INT}           : {token, {litn, TokenLine, to_number(TokenChars)}}.
{MINUS}{INT}{DOT}{INT} : {token, {litf, TokenLine, to_number(TokenChars)}}.
{MINUS}{INT}           : {token, {litn, TokenLine, to_number(TokenChars)}}.
{QUOTED_STRING} : {token, {lit, TokenLine, string:substr(TokenChars, 2, string:len(TokenChars)-2)}}.
{VAR}           : {token, {var, TokenLine, string:substr(TokenChars, 2, string:len(TokenChars))}}.

Erlang code.

to_number(S) ->
  S2 = list_to_binary(S),
  'Elixir.ExoSQL.Utils':'to_number!'(S2).
