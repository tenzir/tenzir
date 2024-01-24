# Tenzir Query Language

- **Status**: WIP
- **Created**: TBD
- **ETA**: TBD
- **Authors**:
  - [First Last](https://github.com/username)
- **Contributors**:
  - [First Last](https://github.com/username)
- **Discussion**: [PR #XYZ](https://github.com/tenzir/tenzir/pull/XYZ)

## Overview

A short summary of the proposal contents.

## Problem Statement

A description of the problem this proposal solves.

## Syntax

### Lexical Elements

> [_Keyword_] := `this` | `meta` | `let` | `if` | `else` | `match` | ... \
> [_Identifier_] := *AlphaUnder* *AlphaNumUnder*<sup>\*</sup> \
> [_Punctuation_] := ... \
> [_Literal_] := `true` | `false` | `"..."` | `r"..."` | `b"..."` | ... (+
> durations, etc) \
> [_LineComment_] := `//` ... `\n` \
> [_BlockComment_] := `/*` ... `*/` \
>
> *AlphaUnder* := [`a`-`z` `A`-`Z` `_`] \
> *AlphaNumUnder* := *AlphaUnder* | [`0`-`9`]

The literals for durations will be more limited than we currently have. For
example: `now()` instead of `now`, and `now() - 1h` instead of `1h ago`.


## [_File_]

> (`#!` ... `\n`)<sup>?</sup> [_Body_]<sup>?</sup>

### [_Body_]

A sequence of statements. Statements are separated by newlines or the `|`
character.

> [_Statement_] (*StmtSep* [_Statement_])<sup>\*</sup>
>
> *StmtSep* := `\n` | `|`

### ­­­[_Statement_]

> [_DeclarationStmt_] | [_OperatorStmt_] | [_AssignmentStmt_] | [_ControlStmt_]
>
> [_DeclarationStmt_] := \
> &nbsp; &nbsp; `let` [_Identifier_] `=` [_Expression_] \
> [_OperatorStmt_] := \
> &nbsp; &nbsp; [_Entity_] *OperatorArgs*<sup>?</sup> \
> [_AssignmentStmt_] := \
> &nbsp; &nbsp; [_Path_] `=` [_Expression_] \
> [_ControlStmt_] := \
> &nbsp; &nbsp; `if` [_Expression_] `{` [_Body_]<sup>?</sup> `}` (`else` `{`
> [_Body_]<sup>?</sup> `}`)<sup>?</sup> \
> &nbsp; &nbsp; `match` [_Expression_] `{` ([_Expression_] `=>` `{`
> [_Body_]<sup>?</sup> `}`)<sup>\*</sup> `}`
>
> *OperatorArgs* := \
> &nbsp; &nbsp; [_Arguments_] [_PipelineExpr_]<sup>?</sup> \
> &nbsp; &nbsp; `(` [_Arguments_] `,`<sup>?</sup>  `)`
> [_PipelineExpr_]<sup>?</sup> \
> &nbsp; &nbsp; `(` `)` [_PipelineExpr_]<sup>?</sup>


### [_Arguments_]

> [_Arguments_] := [_Argument_] (`,` [_Argument_])<sup>\*</sup> \
> [_Argument_] := [_Expression_] | [_Path_] `=` [_Expression_]


### [_Expression_]

Fun fact: TQL expressions are a superset of JSON.

#### [_UnaryExpr_]

> `not` [_Expression_] \
> `-` [_Expression_] \
> `$` [_Expression_]

We should decide whether we want to keep `$` for expanding non-entity constants.
The need for disambiguation arises when accessing `let` bindings in a pipeline,
`group` constants, outer values when processing a list with a pipeline, or
constants that are provided by operators such as `window` (e.g., window start
and end time). Except for list processing with pipelines, the available
constants can statically be determined (although this might change in the
future). But in all cases, when a pipeline is instantiated, all constants are
known, and we could make decisions depending on that. However, we can't know
whether there exists a field with this name.

Also, are constants always lexical?

#### [_BinaryExpr_]

> [_Expression_] `+` [_Expression_] \
> [_Expression_] `-` [_Expression_] \
> [_Expression_] `*` [_Expression_] \
> [_Expression_] `/` [_Expression_] \
> [_Expression_] `==` [_Expression_] \
> [_Expression_] `!=` [_Expression_] \
> [_Expression_] `>` [_Expression_] \
> [_Expression_] `<` [_Expression_] \
> [_Expression_] `>=` [_Expression_] \
> [_Expression_] `<=` [_Expression_] \
> [_Expression_] `&&` [_Expression_] \
> [_Expression_] `||` [_Expression_] \
> [_Expression_] `in` [_Expression_] \
> [_Expression_] `not` `in` [_Expression_]

#### [_Path_]

> `this` \
> `meta` \
> [_Identifier_] \
> [_Expression_] `[` [_Expression_] `]` \
> [_Expression_] `.` [_Identifier_]

We could also consider using `@` instead of `meta` for metadata.

#### ControlFlowExpr

> `if` [_Expression_] `{` [_Expression_] `}` (`else` `{` [_Expression_]
> `}`)<sup>?</sup> \
> `match` [_Expression_] `{` ([_Expression_] `=>` `{` [_Expression_]<sup>?</sup>
> `}`)<sup>\*</sup> `}`


#### [_ArrayExpr_]

> `[` *ArrayItems*<sup>?</sup> `]`
>
> *ArrayItems* := [_Expression_] (`,` [_Expression_])<sup>\*</sup>
> `,`<sup>?</sup>


#### [_RecordExpr_]
> `{` *RecordItems*<sup>?</sup> `}`
>
> *RecordItems* := *RecordItem* (`,` *RecordItem*)<sup>\*</sup> `,`<sup>?</sup>
> \
> *RecordItem* := *RecordKey* `:` [_Expression_] \
> *RecordKey* := [_IdentPath_] | [_String_] | `[` [_Expression_] `]`


#### [_PipelineExpr_]

> `{` [_Body_]<sup>?</sup> `}`

It's ambiguous whether `{` `}` denotes the empty record or the empty pipeline.
We can either use a rule like "pipeline expressions must have at least one
operator", which means that `{ pass }` would be needed for the empty pipeline.
Or we can try to resolve the ambiguity later when we have additional context.
Are there any places where both a pipeline and a record would be valid?


#### [_FunctionCall_]

> [_Entity_] `(` *FunctionArgs*<sup>?</sup> `)` \
> [_Expression_] `.` [_Identifier_] `(` *FunctionArgs*<sup>?</sup> `)`
>
> *FunctionArgs* := [_Arguments_] `,`<sup>?</sup>


#### [_Entity_]

> [_Identifier_] (`::` [_Identifier_])<sup>\*</sup>

There is a syntactic ambiguity between non-qualified constants and identifier.
This is related to [the discussion](#unaryexpr) about `$`.

## Alternatives

What alternatives may exist and why are they not an option?

[_Keyword_]: #lexical-elements
[_Punctuation_]: #lexical-elements
[_Identifier_]: #lexical-elements
[_LineComment_]: #lexical-elements
[_BlockComment_]: #lexical-elements
[_Literal_]: #lexical-elements
[_Float_]: #lexical-elements
[_Blob_]: #lexical-elements
[_Integer_]: #lexical-elements
[_String_]: #lexical-elements
[_Bool_]: #lexical-elements

[_File_]: #file
[_Body_]: #body

[_Statement_]: #statement
[_DeclarationStmt_]: #statement
[_OperatorStmt_]: #statement
[_AssignmentStmt_]: #statement
[_ControlStmt_]: #statement
[_OperatorArg_]: #statement
[_Entity_]: #entity

[_Arguments_]: #arguments
[_Argument_]: #arguments

[_Expression_]: #expression
[_AtomicExpr_]: #atomic-expr
[_PipelineExpr_]: #pipeline-expr
[_RecordExpr_]: #record-expr
[_ArrayExpr_]: #array-expr
[_UnaryExpr_]: #unary-expr
[_BinaryExpr_]: #binary-expr
[_FunctionCall_]: #function-call
[_Path_]: #path
[_IdentPath_]: #path
