# VAST and Tenzir

This page answers frequently asked questions about the relationship of VAST and
[Tenzir](https://tenzir.com).

## What is VAST?

VAST, an acronym for *Visibility Across Space and Time*, is an open-source
software project created by Matthias Vallentin originally during his Master's at
Technical University Munich in 2006, and materialized throughout his PhD from
2008 to 2016 at the University of California, Berkeley.

### Inception at UC Berkeley

VAST was initially focused on providing a fast search experience using
structured and typed log data coming from [Zeek](https://zeek.org), the network
security monitor written by Vern Paxon, Matthias' PhD advisor.

The [NSDI'16 publication][nsdi16] details the system design, architecture and
the implementation. VAST pioneered the idea of applying the [actor
model](https://en.wikipedia.org/wiki/Actor_model) to the native domain, with a
type-safe message-passing runtime called [C++ Actor Framework
(CAF)](https://actor-framework.org). This architecture enabled a programming
model with a single abstraction for concurrency and distribution, yielding a
highly scalable system capable of first exploiting all available resources (CPU
cores, RAM, disk) within a single machine before scaling out horizontally.

[nsdi16]: https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/vallentin

### Continuation to Tenzir

After VAST's academic inception, Matthias founded Tenzir to propel the
open-source project into a commercial offering. VAST has found a new home at
Tenzir, and is the foundation for our vision of becoming the preferred
open-source security data plane for threat detection, investigation, and
response.

## What is Tenzir?

[Tenzir](https://tenzir.com) is a security startup that builds commercial
solutions powered by the open-source project VAST.

Tenzir makes VAST easy to deploy, use, and integrate into enterprise
environments. In particular, Tenzir provides a web interface for managing a
fleet of VAST nodes and pipelines spanned across them. Tenzir also provides
commercial support and customer success services related to VAST.

## What is the relationship between VAST and Tenzir?

VAST is an open-source software and growing community; Tenzir is a security
startup building out VAST in a commercial context. To date, Tenzir is the
biggest contributor to VAST, but the goal is to nurture a diverse and active
ecosystem of contributors from numerous organizations.

Tenzir also has the role of protecting and maintaining the self-sufficient
viability of VAST by fostering a vibrant and diverse community around both the
open-source and commercial offerings. In particular, Tenzir is dedicated to
ensuring independent governance of VAST by putting trust and transparency at the
forefront. Examples of this commitment from Tenzir include maintaining a [public
roadmap](https://vast.io/roadmap), providing detailed [contribution
guidelines](https://vast.io/docs/contribute), actively engaging in the
[community](https://vast.io/discord)
[forums](https://github.com/tenzir/vast/discussions), and promoting VAST across
social media, blog posts, conference talks, and more.

## Who is the audience of VAST and Tenzir?

As an open-source project, VAST aims to build a community of passionate users
and developers that cultivate an active exchange, contribute enhancements, and
spread the word. The mission of the VAST community is to offer a diverse
exchange point at the intersection of security and data.

Tenzir focuses on customers. The *community edition* is a free edition for
consultants, small business, and researchers with a very small budget. The
*enterprise edition* focuses on security professionals for larger enterprise
deployments, OEM/SI embeddings, as well as MSSP and MDR platforms.

:::info Early Access
We are working at full steam towards the launch of our community and enterprise
editions. We are working with a select group of early adopters who enjoy giving
feedback and don't mind rough edges. If that's you, please join the waitlist
below.

<div align="center">
  <a class="button button--md button--primary margin-right--md" href="https://webforms.pipedrive.com/f/c6vwqvg8HuWVVyE4GOE3fTYE7gAfsA9ggVjjicdBWZa674pIQqh1BP2p7CbVxpbq95">Sign Up</a>
</div>
:::

The following diagram illustrates the scope and audiences of the three editions:

![Editions](/img/editions.excalidraw.svg)
