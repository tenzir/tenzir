# Components

VAST uses the [actor model](actor-model) to structure the application logic into
individual components. Each component maps to an actor, which has a strongly
typed messaging interface that the compiler enforces. All actor components
execute concurrently, but control flow within a component is sequential. It's
possible that a component uses other helper components internally to achieve
more concurrency.

In other words, a component is a microservice within the application. Most
components are multi-instance in that the runtime can spawn them multiple times
for horizontal scaling. Only a few components are singletons where at most one
instance can exist, e.g., because they guard access to an underlying resource.

The diagram below illsutrates VAST's key components in the dataflow between
them:

![Components](/img/components.light.png#gh-light-mode-only)
![Components](/img/components.dark.png#gh-dark-mode-only)

By convention, we use ALL-CAPS naming for actor components and represent them as
circles. Red circles are singletons and blue circles multi-instance actors.

## Singleton Components

Singleton components have a special restriction in that VAST can spawn at most
one instance of them. This restriction exists because such a component mutates
an underlying resource and therefore needs to enforce sequential access. (An
actor does this by definition, because control flow within actor is sequential.)

### CATALOG

import MissingDocumentation from '@site/presets/MissingDocumentation.md';

<MissingDocumentation/>

### SCHEDULER

<MissingDocumentation/>

## Multi-instance Components

Multi-instance components exist at various place in the path of the data. They
often operate stateless and implement pure (side-effect-free) functions. In case
they own state, there is no dependency to other state of the same instance. For
example, a component may operate on a single file, but the whole system operates
on many distributed files, each of which represented by a single instance.

### LOADER

<MissingDocumentation/>

### SOURCE

<MissingDocumentation/>

### SINK

<MissingDocumentation/>

### DUMPER

<MissingDocumentation/>
