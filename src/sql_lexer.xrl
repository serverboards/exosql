Definitions.

INT    = [0-9]+
ID     = [_a-zA-Z][_a-zA-Z0-9]*
COMMA  = ,
DOT    = \.
OP     = (<=|>=|==|!=|<>|-|\+|<|>|=|and|or|AND|OR|\|\||\*)
SPACE  = [\n\t\s]+
OPEN_PAR = \(
CLOSE_PAR = \)
QUOTED_STRING = ("(.|[^"])*"|'(.|[^"])*')
%% "'


Rules.

{SPACE}  : skip_token.
{OP}     : {token, {op, TokenLine, TokenChars}}.
{ID}     : {token, {id, TokenLine, TokenChars}}.
{COMMA}  : {token, {comma, TokenLine, TokenChars}}.
{DOT}    : {token, {dot, TokenLine, TokenChars}}.
{OPEN_PAR}      : {token, {open_par, TokenLine, TokenChars}}.
{CLOSE_PAR}     : {token, {close_par, TokenLine, TokenChars}}.
{INT}{DOT}{INT} : {token, {lit, TokenLine, TokenChars}}.
{INT}           : {token, {lit, TokenLine, TokenChars}}.
{QUOTED_STRING} : {token, {lit, TokenLine, string:slice(TokenChars, 1, string:length(TokenChars)-2)}}.

Erlang code.
