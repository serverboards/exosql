Definitions.

INT    = [0-9]+
MINUS  = -
RESERVEDL = (select|where|from|as|inner|cross|left|right|outer|join|on|group|by|order|asc|desc|true|false|not|distinct|limit|offset|all)
RESERVEDU = (SELECT|WHERE|FROM|AS|INNER|CROSS|LEFT|RIGHT|OUTER|JOIN|ON|GROUP|BY|ORDER|ASC|DESC|TRUE|FALSE|NOT|DISTINCT|LIMIT|OFFSET|ALL)
ID     = [_a-zA-Z][_a-zA-Z0-9]*
COMMA  = ,
DOT    = \.
OP1     = (and|AND)
OP2     = (or|OR)
OP3     = (<|>|<=|>=|==|!=|<>|<|>|=|\|\|)
OP4     = (-|\+)
OP5     = (\*|/)
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
{OP1}     : {token, {op1, TokenLine, TokenChars}}.
{OP2}     : {token, {op2, TokenLine, TokenChars}}.
{OP3}     : {token, {op3, TokenLine, TokenChars}}.
{OP4}     : {token, {op4, TokenLine, TokenChars}}.
{OP5}     : {token, {op5, TokenLine, TokenChars}}.
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
