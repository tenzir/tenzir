---
sidebar_position: 1
---

# Syntax

Tenzir comes with its own language to define pipelines, dubbed **Tenzir Query
Language (TQL)**.[^1]

[^1]: We pronounce it *teaquel*.

:::info Why yet another query language? Why not SQL?
We often get asked "why did you create yet another query language?" or "why are
you not using SQL?". This is a valid question and we answer it in detail in our
[FAQs](../faqs.md).
:::

Even though TQL is a transformation language, we call it a "query language" to
allude to the outcome of getting the data in the desired form. The language
is geared towards working with richly typed, structured data across multiple
schemas.

We put emphasis on the following principles when designing the language:

1. Use natural language keywords where possible, e.g., verbs to convey actions.
2. Lean on operator names that are familiar to Unix and Powershell users
3. Avoid gratuitous syntax elements like brackets, braces, quotes, or
   punctuations.
4. Exploit symmetries for an intuitive learning experience, e.g., `from` and
   `to` have their duals `read` and `write`.

Let's dive into an example:

![Pipeline Example](pipeline-example.excalidraw.svg)

Here is how you write this pipeline in the Tenzir language, with inline comments
describing what each operator does:

```cpp
/* 1. Start pipeline to data at a Tenzir node */
export
/* 2. Filter out a subset of events (predicate gets pushed down) */
| where #schema == "zeek.weird" && note == "SSL::Invalid_Server_Cert"
/* 3. Aggregate them by destination IP */
| summarize count(num) by id.resp_h
/* 4. Sort by frequency */
| sort
/* 5. Take the top-20 items */
| head 20
/* 6. Write the output as JSON to standard output */
| write json to stdout
```

If you are curious why we created our own language as opposed to using SQL,
please consult our [FAQs](../faqs).
