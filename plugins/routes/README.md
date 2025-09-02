# Routes Plugin

This plugin provides routing capabilities for Tenzir pipelines, allowing data to
flow through named inputs and outputs with dynamic routing via connections and
stateful route objects.

## Architecture Plan

### 1. Router State Setup

The router maintains centralized state for managing routes, connections, and
data flow:

- **State Management**: Routes and connections are managed as a unified state
- **State Replacement**: State is always replaced entirely through the
  `routes::sync` operator
- **No CRUD Operations**: Individual create/read/update/delete operations are
  not supported initially; the entire state must be provided at once
- **Startup/Shutdown Ordering**: Potential conflicts with the pipeline-manager
  during startup and shutdown are intentionally ignored and left as a problem
  for future resolution

### 2. State Inspection

- **`routes::list` Operator**: Provides a simple dump of the current router
  state
- Useful for debugging and monitoring active routes and connections

### 3. Input/Output Operations

The plugin provides input and output operators for data flow:

- **`routes::output`**: Sends data to a named output
- **`routes::input`**: Receives data from a named input
- **Optional Name Argument**: Both operators accept an optional name argument

#### Engine Requirements

- The engine must be modified to provide pipeline names as default arguments to
  input/output operators
- This enables automatic naming based on the executing pipeline

### 4. Connections

Connections link inputs and outputs together to establish data flow paths:

- **Input-to-Output Mapping**: Connections specify which inputs connect to which
  outputs
- **Many-to-Many Support**: A single input can connect to multiple outputs, and
  multiple inputs can connect to a single output
- **Dynamic Creation**: Connections can be created and modified through the
  router state
- **Connection Metadata**: Each connection may include metadata such as
  priority, filtering rules, or transformation specifications

### 5. Stateful Routes with Cascading Predicates

Routes are separate stateful objects that provide sophisticated data routing
logic:

- **Single Input**: Each route has exactly one input that receives data
- **Multiple Outputs**: Routes can send data to multiple outputs based on
  routing logic
- **Cascading Predicates**: Routes contain an ordered list of predicates that
  are evaluated in sequence
- **First Match Routing**: Data is routed to the output of the first predicate
  that evaluates to true
- **Default Output**: Routes can specify a default output for data that doesn't
  match any predicate
- **State Persistence**: Routes can maintain internal state between data
  processing operations
- **Predicate Types**: Predicates can include:
  - Field-based conditions (e.g., `src_ip == "192.168.1.1"`)
  - Pattern matching on event content
  - Statistical conditions based on accumulated state
  - Time-based routing rules

#### Route Configuration Example

```yaml
connections:
  - from: "raw_network_logs"
    to: "security_events"
  - from: "raw_application_logs"
    to: "security_events"
  - from: "critical_alerts"
    to: "incident_response_input"
  - from: "brute_force_detection"
    to: "automated_blocking_input"
  - from: "known_threats"
    to: "threat_hunting_input"
  - from: "general_security_log"
    to: "siem_storage_input"
  - from: "processed_metrics"
    to: "dashboard_input"
  - from: "performance_alerts"
    to: "ops_team_input"

routes:
  security_triage:
    input: "security_events"
    rules:
      - where: "severity == 'critical'"
        output: "critical_alerts"
      - where: "event_type == 'authentication_failure' and count > 5"
        output: "brute_force_detection"
      - where: "src_ip in threat_intelligence.ips"
        output: "known_threats"

  metrics_router:
    input: "system_metrics"
    rules:
      - where: "metric_type == 'performance' and value > threshold"
        output: "performance_alerts"
        final: true
      - where: "source == 'database' or source == 'web_server'"
        output: "processed_metrics"
        final: true
      - output: "other_metrics"
```
