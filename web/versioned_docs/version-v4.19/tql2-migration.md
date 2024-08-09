# TQL2 Migration

This page answers the most frequently asked questions about TQL2.

### What is TQL2?

TQL2 is the next generation of the Tenzir Query Language for writing pipelines.

### How do I use TQL2?

Start your pipeline with a `// tql2` comment to opt into using the new language.
This is supported for Tenzir v4.19 or newer.

### Why create an all-new language?

TQL1 has grown historically. Over time, we identified the following shortcomings
that TQL2 fixes:
- Lack of composable expressions, in particular functions and arithmetic
  operations.
- Absence of nested pipelines, which was partially worked around with
  operator modifiers.
- Context-dependent grammar for operators that was unintuitive for users, and
  close to impossible to parse and syntax-highlight.
- Inability to define constants with `let`, and also soon types, functions, and
  custom operators within the pipeline definition directly.
- Lack of control flow within a pipeline like `if { … } else { … }` made it
  unnecessarily hard to work with your data.

### Can I mix-and-match TQL1 and TQL2?

Yes! The `legacy` operator takes a string as an argument that is parsed as a
TQL1 pipeline and then replaces itself with it. For example:

```
// tql2
export
where @name == "zeek.notice"
// The `top` operator is not yet available in TQL2, so we fall back to TQL1.
legacy "top msg"
```

### Where can I find the TQL2 reference?

Hang tight! We're in the process of writing a TQL2 reference, and aim to make it
available soon. Until then, you can find all available operators and functions
in TQL2 via `show plugins`:

```
show plugins
| unroll types
| put name, type=types
| where "tql2" in type
```

### When should I upgrade?

We do not recommend upgrading production deployments yet, but encourage playing
with TQL2 to get a feel for it and to report any missing things that you may
need.

### Where can I give feedback about TQL2?

Head over to the `#developers` channel in our [community Discord](/discord).

### What features does TQL2 currently support?

| Status | Explanation |
| :-: | :- |
| ✅ | (mostly) complete |
| 🔧 | in progress |

| Feature | Example | Status | Comments
| -: | :- | :-: | :- |
| Simple assignment | `conn.test = "done"` | ✅ | Insert-or-replace semantics
| Statement separation | newline or <code>\|</code> | ✅
| Dynamic field names | `object[expr]` | 🔧
| Arbitrary field names | `this["@!?$#"]` | ✅
| Conditionals | `if foo == 42 {...} else {...}` | ✅
| Pattern matching | `match proto { "UDP" => ..., "TCP" => ... }` | 🔧
| High performance | | ✅ | Columnar computation engine
| Object expression | `{ foo: 1, bar.baz.qux: 2 }` | ✅ | Field names can also be strings
| List expression | `[1, 2, null, 4]` | ✅
| Functions/methods | `foo(bar) && baz.starts_with("bar")` | ✅
| Named arguments | `foo.bar(baz=1, qux=2)` | ✅
| Arithmetic operators | `a + b / c < d - e` | ✅ | Also works with durations, etc.
| Event metadata | `where @name == "something"` | ✅
| Constant bindings | <code>let $bad_ip = 1.2.3.4 \| search $bad_ip</code> | ✅
| Domain types | `src in 255.0.0.0/24 && duration > 2min` | ✅ | `ip`, `subnet`, `duration`, `time`, `blob`
| Nested pipelines | `group protocol {...}`, `every 1h {...}` | 🔧
| Spread operator | `{...foo, bar: [42, ...baz, qux]}` | ✅ | Can expand objects and lists
| Packages | `my_package::my_function(foo)` | 🔧
| OCSF functionality | `ocsf::validate`, `ocsf::class_uid` | 🔧
| Type definitions | `type flow = {src: ip, dst: ip}` | 🔧
| Escape hatches | <code>python "..." \| js "..."</code> | 🔧
| Secrets | `secret("MY_SECRET")` | 🔧
| Function definitions | `fn my_function(...) { ... }` | 🔧
| Multi-stage parsing |  `msg.parse_json().content.parse_cef()` | ✅
