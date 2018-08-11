# rexosql - Rust ExoSQL VM

This is an experimental virtual machine interpreter to speed up exosql.

It is specialized in SQL and parallel execution of data.

The program is first built using the `ProgramBuilder::new()` or
`ProgramBuilder::compile(&str)`, and the run with some specific context.

Here the VM bytecode is described. It can be feed over the VM, which will
use some callbacks for necessary points: functions, extractors and schemas.

Ideally a query will be compiled, and then executed many times. And
even in the future it could be compiled just in time (JIT), and used from
several programming languages as Erlang/Elixir, Python, Rust, and more.

The data in stored in a stack, and can be aliased with STORE / LOAD. At the end
of the program top of the stack is returned.

# Program code ASM

```asm
:expression_where
# only show when col2 in spain and less than 1000 at col1
LOAD COL2 $1 = COL2
$2 = "spain"
$1 = $1 != $2
IF $1 .then .else
.then
$1 = COL1
$2 = 1000
$1 = $1 < $2
RET 1
.else
$1 = false
RET 1

.expression_select
# Used to select columns 0 and 1, and depending on the value of col0, the string "cheap" or "expensive"
$1 = COL1
$2 = COL2
$3 = 100
$4 = $2 < $3
IF $4 .then .else
.then
$3 = "cheap"
JUMP .end
.else
$3 = "expensive"
.end
RET 3

.code
# comment
EXTRACT databasevar tablevar qualsvar columnsvar
WHERE expression_where
SELECT expressions_select
# more code...
```

# Instructions

There are two types of programs: Main SQL program, and expressions

For literals, any JSON valid structure can be used.

## SQL code

* EXTRACT database table quals columns
* SELECT expressions
* WHERE expression
* DUP
* LOAD id
* STORE id
* ORDER BY
* CROSS_JOIN expression
* INNER_JOIN expression
* LEFT_JOIN expression
* RIGHT_JOIN expression
* LATERAL_JOIN expression
* GROUP_BY expressions
* OFFSET expression
* LIMIT expression

## Expressions

* LOADCOL ncol
* DUP
* SUM
* SUB
* MUL
* DIV
* CALL function

* EQ
* NEQ
* GT
* LT
* GTE
* LTE

* JUMP_FALSE label
* JUMP label

* RET
