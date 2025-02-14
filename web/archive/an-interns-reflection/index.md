---
title: An Intern's Reflection
authors: [balavinaithirthan]
date: 2024-06-14
tags: [internship]
comments: true
---

I spent the past twelve weeks interning at Tenzir and am excited to share my
experiences.

<!-- truncate -->

## Journey

My first days at Tenzir felt like the first time I surfed. The summer after my
first year, I spent the mornings at Half Moon Bay, trying desperately to stay on
my surfboard. Just as I would climb a giant wave, another would come behind and
crash against my chest. A never-ending cycle for the weeks of summer.

Now, a few years later, I sat in front of a monitor at Tenzir’s headquarters,
left-clicking on my mouse for what felt like hours. Each left click revealed yet
another function, namespace, or variable; an endless cycle of waves. As a
student who learned from the ground up and got a broad understanding, I found
myself trying to learn the meaning of every function and the intricate specifics
of the architecture.

Twelve weeks later, I look back at this first week with a smile—both at how much
I have grown with Dominik’s mentorship and how little I knew about software
engineering and the complexities of these large codebases. In my first project,
I tackled problems head-on without understanding their scope. It took many
frustrating sessions of left-clicking loops before I overcame my initial fear of
asking for help.

Two days into the internship, Dominik walked me through the setup for the `read`
and `write` operators, how to segment my tasks, and how to celebrate small wins.
Writing my first feature taught me to tackle issues systematically, just as one
must focus only on the top of the wave. I had done more in that one hour than
combined in the past two days. This began the journey of how I learned to ride
each wave as its own and respect the vastness of the codebase.

This journey of learning the basics continued as I spent the rest of the first
week in a Git rebase hell, struggled with linters, and looked up Bash commands I
hadn’t used in years. I also began to notice the differences between software
engineering and computer science. Soon, my love of runtime guarantees and
detailed architecture shifted to writing code that worked and passed CI/CD.

My time at Tenzir has been eye-opening for my software engineering journey. I’ve
learned about the industry standards of modern C++, the scale of open-source
projects, and practical problem-solving.

Beyond writing code, I have learned about effective remote collaboration and the
importance of community in tech, something I plan to carry forward in my career.
I am still growing, both in trying to find the balance between big-picture vs
individual requirements and figuring out when to ask for help. Ultimately,
Tenzir has prepared me for the real world and continues to fuel my passion for
low-level systems.

## Projects

Here are some of the projects I worked on at Tenzir.

### Parquet and Feather Parsers and Printers

Apache Parquet (and to a lesser extent its sibling Apache Feather) is a widely
used data formats for storing tabular data. In line with other Tenzir operators,
we designed a parser plugin and printer plugin to allow users to read and write
Feather and Parquet data. We also adapted the Feather streaming interface to
stream Feather data and allowed for buffering in the Parquet printer. This
enables the following example:

```text {0} title="Convert a Feather file to a Parquet file"
from /path/to/file.feather
| to /path/to/file.parquet
```

### Projection Pushdown Optimization

Projection pushdown is an optimization technique that reduces data movement in a
pipeline by pushing the projection operation closer to the data source. Tenzir
pipelines have several operators capable of projection pushdown optimization,
e.g., `select`, `summarize`, `enrich`, and `drop`. This project focused on three
areas:

1. Creating the framework to extend future projection pushdown optimizations.
2. Implementing projection pushdown for the `select` operator.
3. Modifying the Feather and Parquet data formats to accept the projection
   pushdown.

We successfully moved the `select` operator up through the pipeline:

```text {0} title="What the user writes"
from ./example.json
| …
| select col
```

```text {0} title="What Tenzir runs"
from ./example.json
| select col
| …
```

Projection pushdown smartly detects whether it is safe to move up a projection
within the pipeline, operator by operator, and makes it so that the projection
runs as early as possible.

Finally, we modified the Feather and Parquet parsers to accept projection
pushdown optimizations. For example, in `read parquet | select foo`, Tenzir now
eliminates the `select` operator entirely and only reads the column `foo` in the
Parquet parser. Before, it read everything in the Parquet parser, and then later
on dropped all columns but the projected ones.

### Print Operator

We added a `print` operator that allows users to convert records into strings,
providing an inverse to the parse operator. The following is now possible:

```json {0} title="Input"
{
  "flow_id": 852833247340038,
  "flow": {
    "pkts_toserver": 1,
    "pkts_toclient": 0,
    "bytes_toserver": 54,
    "bytes_toclient": 0
  }
}
```

```text {0} title="Render the field flow as CSV"
from input.json
| print flow csv --no-header
```

```json {0} title="Output"
{
  "flow_id": 852833247340038,
  "flow": "1,0,54,0"
}
```

### Miscellaneous

In between these larger projects, I worked on smaller features and bug fixes.

Specifically, we modified the GeoIP context to allow for users to create empty
contexts and load data later—from anywhere. The following is now possible:

```text {0} title="Create an empty GeoIP context"
context create countries geoip
```

```text {0} title="Load the GeoIP database from a remote location"
load s3://path/to/countries.mmdb
| context load countries
```

We also modified the `python` plugin to check for syntax errors before input
arrives. For example, the following will now error before input is read:

```text {0} title="Syntax error: did you mean 'else'?"
…
| python 'self.x = "foo" if self.y esle "bar"'
```

Finally, we added two timeout flags to `lookup-table` update that attaches a
timeout to each event. The following is now possible:

```text {0} title="Expire lookup table entries after 10 days, or if they're not read for 1 day"
from inventory.csv
| context update subnets --create-timeout=10d --update-timeout=1d
```

## Reflection

I am grateful for Dominik’s mentorship and Tenzir, which provided a structured
environment with collaborative coding and exciting projects. Dominik has
significantly influenced my approach to writing code, encouraging me to consider
user experience and maintainability.

Interning at Tenzir was an extraordinary experience. I am thankful to Matthias
for the opportunity and for his advice on blending passion with business. This
internship was a journey of firsts: my first time in Germany, my first role as a
C++ developer, and my first real-world application of CS education. I eagerly
anticipate the "nexts," both at Tenzir and in life.

:::note From the Team at Tenzir
We all loved working with Bala—it felt like he arrived just yesterday and became
part of the team immediately. This was the first time we've had an intern at
Tenzir, and it certainly won't be the last. His application came out of the
blue—there was no advertised role for interns, but after we got an idea of his
high skill ceiling in two quick interview rounds, we thought we'd give it a
shot. And it was so worth it.

Want to get in touch with Bala? Connect with him on
[LinkedIn](http://linkedin.com/in/balabv/) and make sure to follow him on
[GitHub](https://github.com/balavinaithirthan).
:::
