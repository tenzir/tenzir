# Use VAST

After having completed the [VAST setup](/docs/setup-vast), you can interact with
the system. At a high level, VAST does 3 things:

1. **Manage Data**: VAST ingests and indexes events, executes queries to
   retrieve data, facilitates metadata management to describe data, and supports
   flexible aging data to meet capacity and compliance requirements.
2. **Contextualize Events**: VAST provides operations to make more sense of the
   data, such as pivoting to linked events, exploring a spatio-temporal
   neighborhood, and enriching events with third-party context.
3. **Execute Security Content**: VAST processes threat intelligence, matching it
   live during ingestion and applying it retrospectively by compiling it into queries.

👇 Click on any blue actions to get started.

```mermaid
flowchart TB
  classDef action fill:#00a4f1,stroke:none,color:#eee
  classDef future fill:#bdcfdb,stroke:none,color:#222
  %% Actions
  subgraph manage [Manage Data]
    direction TB
    import(Import):::action
    export(Export):::future
    transform(Transform):::future
  end
  subgraph contextualize [Contextualize Events]
    direction TB
    pivot(Pivot):::future
    explore(Explore):::future
    enrich(Enrich):::future
  end
  subgraph detect [Detect Threats]
    direction TB
    match(Match<br/>Threat Intel):::future
    sigma(Execute<br/>Sigma Rules):::future
  end
  %% Edges
  manage <--> contextualize <--> detect
  %% Links
  click import "/vast/docs/use-vast/manage-data/import" "Import Data"
  %%click export "/vast/docs/use-vast/manage-data/export" "Export Data"
  %%click transform "/vast/docs/use-vast/manage-data/transform" "Transform Data"
  %%click pivot "/vast/docs/use-vast/contextualize-events/pivot" "Pivot"
  %%click explore "/vast/docs/use-vast/contextualize-events/explore" "Explore"
  %%click enrich "/vast/docs/use-vast/contextualize-events/enrich" "Enrich"
  %%click match "/vast/docs/use-vast/detect-threats/match-threat-intel" "Match Threat Intel"
  %%click sigma "/vast/docs/use-vast/detect-threats/execute-sigma-rules" "Execute Sigma Rules"
```
