# Target Audience

This section characterizes the primary audience of Tenzir in the security
operations center (SOC). From our perspective, Tenzir users fall into three user
categories that ideally collaborate to achieve the SOC's mission of defending
its constituency: the **SOC Analyst**, the **Detection Engineer**, and the
**Data Scientist**.

![Personae](about/personae.excalidraw.svg)

In practice, the lines are blurred and a single person often has to wear
multiple hats. We deem it still important to expose the key functional roles
that yield an optimal team for detection and response use cases.

## SOC Analyst

The SOC Analyst operationalizes the security content crafted by the Detection
Engineer. The optimal roll-out of security content yields actionable alerts,
ideally pre-contextualized and automatically triaged. SOC Analysts are domain
experts, which is why flanking them with engineering and analytics resources
yields a strong team.

Responding to the alerts is the *reactive* element of the SOC Analyst. Threat
hunting is the *proactive* element, which involves forming hypotheses derived
from domain knowledge, experience, and understanding of the behavior of the
local environment.

:::tip Threat Hunting Engine
With Tenzir, we want to enable threat hunters with an interactive data
exploration workbench so that they can *think and execute in the security
domain*, without having to context-switch and adapt to the lower-level analytics
primitives exposed by a generic database.
:::

## Detection Engineer

The Detection Engineer distills the work from SOC Analyst and Data Scientist
into security content. *Detection as Code (DaC)* is the guiding principle.
Codified detections typically range in terms of complexity from singular
indicators of compromise to complex state machines describing behavior.

Examples include [Sigma rules](https://github.com/SigmaHQ/sigma) within
[correlations](https://github.com/SigmaHQ/sigma/wiki/Specification:-Sigma-Correlations),
[Yara rules](https://yara.readthedocs.io/) rules for file scanning, [Zeek
scripts](https://zeek.org) or [Suricata rules](https://suricata.io/) for network
analytics, and [Kestrel hunt flows](https://kestrel.readthedocs.io/) for generic
multi-stage correlations.

Simplistic detections (e.g., point indicators of file hashes, domains, or IPs of
attacker infrastructure) shift the effort to downstream contextualization and
triaging, which is why "shifting left" is crucial to keep the alerting funnel
actionable. Point indicators are not "bad" per se and can be highly actionable,
but it's the Detection Engineer's responsibility to ensure that low-fidelity
alerts are properly contextualized and triaged before hitting the SOC Analyst.

:::tip Security Content Execution Engine
With Tenzir, we aim to build a universal security content execution engine that
is capable of processing detections in an open, standardized format, such as
STIX bundles or MISP events. Tenzir will operationalize security content by
deploying it live in-stream to catch future events, as well as retro-actively to
surface past occurrences of previously unknown attacks.
:::

## Data Scientist

Data Scientists flank SOC Analysts and Detection Engineers in advanced
environments to solve complex detection challenges that involve processing
massive amounts of data. Illuminating the data and its stable relationships
to craft actionable detections is one central goal. When the SOC Analysts acts
as threat hunter to follow a domain-specific hypothesis based on TTPs and IoCs,
the Data Scientist acts as *data hunter* using statistical and machine-learning
tools. Analysis results manifest not only as new indicators, but often as
trained models that the Detection Engineer can codify and push out to the edge
for realtime detection.

The Data Scientist extracts features, trains models, and sanity-checks domain
assumptions (e.g., attacker TTPs) with SOC Analyst to arrive at a robust
model of malicious activity. Notebooks are an effective vehicle of capturing the
often organic process at arriving at a working solution—both analytically and
visually.

:::tip Data Science Workbench
With Tenzir, we enable Data Scientists to access the raw underlying security
data at high bandwidth while bringing their own tools to run custom, advanced
analytics. Tenzir makes this possible by standardizing all internal data
representation and processing on [Apache Arrow](https://arrow.apache.org), which
offers high interoperability and native access from R, Python, Spark, and other
data science tools.
:::
