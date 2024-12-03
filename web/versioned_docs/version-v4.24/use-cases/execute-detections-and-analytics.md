---
sidebar_position: 1
---

# Execute Detections and Analytics

Security teams integrate a variety of different detection content and custom
analytics to drive turn visibility into actionable alerts. The spectrum of
analytics ranges across atomic of indicators of compromise (IoCs),
[YARA](https://yara.readthedocs.io/) and [Sigma](https://sigmahq.io) rules,
Python scripts, and machine learning models. All of these forms require
different execution methods when deploying them on the raw event data. Scaling
up in both diversity of analytics and volume of event data is daunting
engineering task.

![Operationalize Security Content](execute-detections.excalidraw.svg)

Tenzir's Security Data Pipelines provide an execution vehicle for security
content across the entire spectrum. This helps security teams to operationalize
their detections at ease without devoting precious cycles to in-house data
engineering.

:::tip Key Takeaways
- Tenzir has built-in operators for Sigma and YARA rule matching
- Tenzir supports matching of indicators of compromise via lookup tables and
  Bloom filters
- Tenzir has a generic contextualization framework for enriching data with any
  form of security content
- Tenzir's pipeline supporting running inline Python code at high speeds
- Using Tenzir's pipelines, security teams have a turnkey solution to bring
  their detections and analytics to the security data—no need for in-house data
  engineering resources.
:::

## Problem: Operationalizing detections is hard

Security teams typically deploy several dozens of tools, of which many ship with
various detections. Rarely these work out of the box, but rather require
substantial tuning to achieve the right right true positive and false positive
rate. Mature teams realize that they ultimately need to own the detection
content to produce actionable alerts and sensibly dispatch investigations.

Bringing the security content to the data so that it can act is a non-trivial
task. It requires substantial amount of data engineering and in-depth knowledge
of the execution engine—be it a SIEM, log management solution, a data lake, a
custom database, or a mere streaming engine. But why is that so?

### Limited data engineering skills

When it comes to translating detections to the execution engine, deeper data
engineering skills are required to maintain a scalable system. Security
engineers are domain experts and know how to describe threats and look for
malware patterns, but often lack the skills to tune their rules so that they run
efficiently.

Moreover, security content needs to be deployed not only forward-looking in
a live streaming fashion, but also be translated into searches to apply it
retroactively. The reason is that there is an inherent lag in the availability
of detection content: threat research publish reports with observables and
detections only after an attack is already ongoing. By the team the security
team receives the new insight, the initial attack activity has likely already
happened, and may not occur again. Therefore, security teams also need a
backward-looking retro-matching capability for new content.

Live and retro matching often have different mechanisms within the same system,
making it complicated to configure for the analysts.

### Too many representations of detections

Detections come in many shapes, and they all need to be operationalized
differently. This can eat up a substantial amount of time from security
analysts, who should rather spend their time with the investigation of the
results.

For example, observables or indicators of compromise (IoCs) are a atomic pieces
of knowledge, typically disseminated through reports created by threat analysts.
Security engineers consume these reports, either manually or in structured form
via a intelligence platform. While such a platform may help to organize the
knowledge and organize relevant threats, the relevant observables still needs to
be deployed for detection.

### Ever-increasing data volumes

The sheer volume of telemetry from endpoint, network, and cloud data sources
makes executing detections a non-trivial challenge. If you collect petabytes of
data, performing a full scan over the data is not just inefficient, but
cost-prohibitive when doing so for every detection. Understanding the workloads
that detections generate is a prerequisite to effective execution on large
datasets.

For example, an engine must handle the continuous churn of observables
that describe the ever-changing threat landscape, and use that dataset for
continuous live and retro detection. This "background radiation" alone puts a
growing pressure the historical and live data event feeds that require a
carefully designed data architecture. On top of that come threat hunting and
incident response workloads. Orchestrating these workloads Without deep data
engineering knowledge is next to infeasible.

### Diverse detection inputs and outputs

Building detections is often a bottom-up process that starts with a specific
data source and references a subset of fields. Generalizing it to a larger set
of inputs necessitates an upfront normalization step. At this point many
security teams build on top of proprietary event taxonomies (or schemas)
inherent in the log management or SIEM solution. The trade-off is a deep
coupling of a library of detections and rules to specific vendor ecosystem.

Similarly, when orchestrating a diverse set of detections that come from
different producers and marketplaces, they rarely adhere to a standardized alert
output shape. It's up to the security engineers to wrangle the alert data or
buy themselves out of the problem by deploying SOAR tool.

## Solution: SecDataOps and Tenzir Pipelines

SecDataOps puts data utility front and center by making it easy to manage
security event dataflows. Tenzir's powerful reshaping capabilities make it
effortless to normalize data into the right form so that detections can run on
them. In fact, pipelines themselves can run detection workloads and produce
streamlined findings for easy-button consumption.

### Dedicated detection operators

The *Sec* in SecDataOps refers to security data, and detection is fundamental
operation on it. Tenzir features dedicated operators for executing detections,
such as [`yara`](../operators/yara.md) or
[`sigma`](../operators/sigma.md). If Tenzir does not support a
specific rule engine or detection format, you can extend implement your own
operator in C++ that may depend on custom third party libraries.

There's also a generic [`python`](../operators/python.md)
operator for integrating arbitrary third-party analytics on structured data.
Given the dominance of Python in the data science community, turn-key
transition to security data enables a much more efficient detection engineering
process.

### Normalized detections

Normalizing the shape of input and output of a detection is a prerequisite to
building scalable detection and response pipelines. Standardizing the input is
central to achieve a wide applicability and reuse of a detection. Standardizing
the detection output is critical to scale up the number and diversity of
detections while keeping the subsequent alerting processes lean.

Tenzir's wide array of reshaping operators make it easy to perform ad-hoc
adjustments of the data shape of a particular detection input or output. More
generally, Tenzir supports building entire libraries to normalize data into
broadly used event taxonomies, such as the [Open Cybersecurity Schema Framework
(OCSF)](https://ocsf.io). This ultimately decouples not only data collection
from detection, but also detection from response.

### Generalized contextualization mechanism

The line between detection and contextualization can be blurry. For example,
a contextualization may be the basis for a detection when enriching events with
a "magic risk score" between 0 and 1, and then raising an alert when the score
is greater than 0.8.

Tenzir has built-in contextualization framework for high-speed in-band
enriching, offering a one-stop-shop solution for integrating third-party context
into the detection process.

## Conclusion

Tenzir's SecDataOps approach and Security Data Pipelines lay the foundation for
sustainable and scalable detection operations. Built-in detection operators
jump-start the operationalization of security content, normalization using OCSF
decouple data collection, detection, and response, and an extensible high-speed
contextualization framework makes integrating third-party security content a
breeze. These capabilities dramatically improve the productivity of security
engineers and analysts by shifting their attention from wrangling data to
hunting threats.
