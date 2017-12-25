Definitions.

INT    = [0-9]+
ID     = [a-zA-Z][a-zA-Z0-9]*
COMMA  = ,
DOT    = \.
OP     = (<=|>=|==|!=|<>|-|\+|<|>|=|and|or|&&|\|\|)
SPACE  = [\n\t\s]+
OPEN_PAR = \(
CLOSE_PAR = \)

Rules.

{SPACE}  : skip_token.
{OP}     : {token, {op, TokenLine, TokenChars}}.
{ID}     : {token, {id, TokenLine, TokenChars}}.
{COMMA}  : {token, {comma, TokenLine, TokenChars}}.
{DOT}    : {token, {dot, TokenLine, TokenChars}}.
{OPEN_PAR}  : {token, {open_par, TokenLine, TokenChars}}.
{CLOSE_PAR} : {token, {close_par, TokenLine, TokenChars}}.
{INT}    : {token, {lit, TokenLine, TokenChars}}.

Erlang code.
