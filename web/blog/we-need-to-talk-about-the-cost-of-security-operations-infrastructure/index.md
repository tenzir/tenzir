---
authors: oliverrochford
date: 2023-09-21
tags: [tenzir, pipelines, siem, cost, dataops, secdataops, finops]
comments: true
---

# We Need to Talk About the Cost of Security Operations Infrastructure

In today's digital age, businesses are under immense pressure to bolster their
cybersecurity. Understanding the financial implications of security tools is
vital to ensure optimal ROI through risk reduction and breach resilience. This
is particularly true for consumption-based security solutions like Security
Information and Event Management (SIEM).

![capex-vs-opex](capex-vs-opex.excalidraw.svg)

<!--truncate-->

## The SIEM Cost Challenge

Having worked both as a SIEM consultant and an industry analyst, I've observed
that SIEM costs are a recurring point of contention. As a consequence, SIEM
solutions, the cornerstone of many security operations programs, have garnered a
reputation for being a financial black hole.

Accurately forecasting SIEM costs has remained elusive. Given that a typical
[SIEM takes over 6 months to deploy][panther], predicting its data consumption a
year in advance is speculative. Even vendors sometimes fall short in providing
realistic cost estimates.

[panther]: https://panther.com/wp-content/uploads/2023/01/State-of-SIEM-2021.pdf

Several factors contribute to this unpredictability: outdated benchmark data,
scope changes, the evolving threat landscape, and rapid digitalization. These
variables often lead to unforeseen price hikes post-deployment, catching many
buyers off guard.

## SIEM Pricing: Outdated and Ineffective

Traditional SIEM pricing models haven't evolved in tandem with the explosion in
data volumes. While security teams now handle data ranging from Megabytes to
Petabytes, SIEM licensing remains anchored in a Gigabyte-centric world.

Moreover, the true value of the vast amounts of security data remains ambiguous.
Until a breach or intrusion is investigated, it's challenging to determine the
significance of the collected data. While the probability that the data is still
required decreases over time, we can’t always be sure when it will reach 0.

## Quantifying the Value of Security Data

The amount of security data an organization generates doesn't always correlate
with its revenue. This discrepancy complicates the task of assessing the
business value of security data. Survey data further highlights a disconnect
between the perceived and actual value of SIEM systems, with many organizations
lamenting rising costs and underwhelming features

Security teams are grappling with [rising costs][techresearch], [underutilized
features][panther], and a [lack of comprehensive
coverage][cardinalops], with one study stating that [over 40% think they are
overpaying for their SIEM, and more than 50% unhappy with their current SIEM
providers][panther].

[techresearch]: https://techresearchonline.com/wp-content/uploads/2022/03/SIEM_Shift_How_the_Cloud_is_Transforming_Security_Operations_US_20211007.pdf
[cardinalops]: https://f.hubspotusercontent00.net/hubfs/7289101/CardinalOps%20Quantifying%20the%20Threat%20Coverage%20Gap.pdf

## A Shift in Buyer Behavior

Rather than just endlessly expanding security budgets to combat escalating
costs, organizations are adopting various strategies:

- **Limiting Coverage**: Some are narrowing their security monitoring scope,
  focusing primarily on compliance. However, this approach can compromise threat
  visibility and increase vulnerability.
- **Adopting XDR**: Others are transitioning to Extended Detection and Response
  (XDR) solutions, prioritizing in-depth analysis over breadth. But as XDR gains
  traction, it may inherit SIEM's cost challenges.
- **Building Security Data Lakes:** These are becoming increasingly popular due
  to their cost-effectiveness and advanced analytical capabilities. However,
  transitioning to a data lake doesn't guarantee reduced consumption. Many
  organizations will find they are swapping Capex for Opex. Moreover, while data
  lakes offer certain advantages, they can't fully replace enterprise SIEMs.

## Future-proofing Security Operations for Automation and AI

Even with the improved cost efficiencies and economies of scale achieved by
using security data lakes and cloud computing, we are beginning to hit
affordability limits again.

The integration of new data-intensive tools and technologies, including machine
learning and AI, like large language models, further intensifies this demand.
While these advances promise enhanced cybersecurity capabilities, they
simultaneously usher in a new set of financial challenges that the industry will
have to grapple with. Technological advancements have made it feasible to
process vast data troves, but the question remains: is it economical?

Finding the precarious balance between achieving cost efficiencies and
maintaining robust security resilience is the conundrum facing cybersecurity
leaders. What they need to be able to make informed decisions is a comprehensive
understanding of these costs and their implications, so that they can
strategically navigate these challenges.

## Security FinOps with Tenzir Security Data Pipelines

At Tenzir, we aim to redefine how organizations manage security operations
expenses. Our security data pipelines address core challenges associated with
optimizing SIEM, security data lake, and cloud costs.

Our pipelines enhance data flow and processing by normalizing data formats to
reduce complexity and redundancy, performing in-stream enrichments, and applying
powerful reshaping to optimally prepare the data for consumption. By optimizing
data preprocessing down to the collection point, we curtail unnecessary SIEM
ingestion and cloud compute costs. We transfer many workloads to the edge that
were previously cost-inefficiently executed centrally. By scaling vertically
across cores and pipelines, and horizontally across nodes, organizations can
adapt to variable environments and data loads, ensuring deployment flexibility
and cost-efficiency.

Furthermore, Tenzir ensures data quality, a vital component for effective
DataOps and automation. By filtering out redundant data and prioritizing based
on significance, you can ensure efficient resource allocation. Tenzir's
instrumented data flows provide clear insights into data usage, facilitating
transparent cost benchmarking.

Discover more about our features and benefits in our [solution
brief](https://tenzir.com/solution-brief.pdf) and free whitepaper on
[optimizing SIEM, Cloud and data costs using
Tenzir](https://tenzir.com/whitepaper.pdf).

Start using Tenzir right away at [app.tenzir.com](https://app.tenzir.com).
