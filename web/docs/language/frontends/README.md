# Frontends

A query language *frontend* translates the textual representation of a query to
Tenzir's internal representation.

Specifically, the frontend parses the string representation that you enter in a
user interface and translates it into an [abstract syntax tree
(AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) that
operates on the semantic building blocks of the language, such as literals,
predicates, extractors, and so on.

The frontend is customizable through the [query language
plugin][language-plugin]. For example, the [Sigma](frontends/sigma)
frontend translates Sigma rules written in YAML to Tenzir queries. The
[Tenzir](frontends/tenzir) plugin is the default frontend that implements the
language we designed for Tenzir.

[language-plugin]: ../../architecture/plugins.md#language

Tenzir ships with the following frontends:

import DocCardList from '@theme/DocCardList';

<DocCardList />
