Definitions.

INT    = [0-9]+
MINUS  = -
RESERVEDL = (select|where|from|inner|cross|join|on|group|by|order|asc|desc)
RESERVEDU = (SELECT|WHERE|FROM|INNER|CROSS|JOIN|ON|GROUP|BY|ORDER|ASC|DESC)
ID     = [_a-zA-Z][_a-zA-Z0-9]*
COMMA  = ,
DOT    = \.
OP     = (<=|>=|==|!=|<>|-|\+|<|>|=|and|or|AND|OR|\|\||\*|/)
SPACE  = [\n\t\s]+
OPEN_PAR = \(
CLOSE_PAR = \)
QUOTED_STRING = ("([^"])*"|'([^'])*')
%% "
VAR    =  \$[_a-zA-Z][_a-zA-Z0-9]*

Rules.

{SPACE}  : skip_token.
{RESERVEDL} : {token, {list_to_atom(string:to_upper(TokenChars)), TokenLine}}.
{RESERVEDU} : {token, {list_to_atom(TokenChars), TokenLine}}.
{OP}     : {token, {op, TokenLine, TokenChars}}.
{ID}     : {token, {id, TokenLine, TokenChars}}.
{COMMA}  : {token, {comma, TokenLine, TokenChars}}.
{DOT}    : {token, {dot, TokenLine, TokenChars}}.
{OPEN_PAR}      : {token, {open_par, TokenLine, TokenChars}}.
{CLOSE_PAR}     : {token, {close_par, TokenLine, TokenChars}}.
{INT}{DOT}{INT} : {token, {lit, TokenLine, TokenChars}}.
{INT}           : {token, {lit, TokenLine, TokenChars}}.
{MINUS}{INT}{DOT}{INT} : {token, {lit, TokenLine, TokenChars}}.
{MINUS}{INT}           : {token, {lit, TokenLine, TokenChars}}.
{QUOTED_STRING} : {token, {lit, TokenLine, string:substr(TokenChars, 2, string:len(TokenChars)-2)}}.
{VAR}           : {token, {var, TokenLine, string:substr(TokenChars, 2, string:len(TokenChars))}}.

Erlang code.
