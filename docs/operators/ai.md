---
title: ai
category: AI
example: 'ai "Explain this dataset"'
---

Takes a set of events and analyzes them together with a prompt with an LLM.

```tql
ai prompt:string, api_key:string, [model=string], [system=string]
```

## Description

The `ai` operator sends a batch of events along with a user-provided prompt to a
Large Language Model (LLM) for analysis. The operator collects all input events,
formats them as structured data, and sends them to an AI service for processing.
The AI's response is returned as a single output event of type `tenzir.ai` with
the response text in the `response` field.

This operator is particularly useful for:
- Analyzing patterns in log data or security events
- Summarizing large datasets with natural language
- Extracting insights from unstructured data
- Performing semantic analysis on event streams

:::note[API Requirements]
This operator requires access to an LLM provider service. You must provide valid
API credentials for the selected provider. The provider is determined automatically
from the model name.
:::

:::note[Batch Processing]
The `ai` operator processes all input events as a single batch before sending
them to the LLM provider. This means it will buffer the entire input stream in
memory before producing any output.
:::

### `prompt: string`

The natural language prompt that instructs the LLM on how to analyze the data.
This prompt is sent along with a structured representation of all input events.

### `api_key: string`

The API key for authenticating with the LLM provider. This parameter is required
and the specific format depends on the provider (determined by the model name).

### `model: string (optional)`

The specific model to use from the LLM provider. The model name also determines
which provider to use (e.g., "gpt-4" uses OpenAI, "claude-3-sonnet" uses Anthropic).
If not specified, a default model will be used.

### `system: string (optional)`

An optional system message that provides additional context or instructions to
the LLM about how to behave or what role to assume during the analysis.

## Examples

### Analyze security events for suspicious patterns

```tql
from logs.json
where event_type == "authentication"
ai "Analyze these authentication events for suspicious login patterns. 
    Look for unusual geographic locations, failed login attempts, or 
    off-hours access patterns.",
    api_key="sk-..."
```

```tql
{
  response: "Based on the authentication events, I found several suspicious patterns..."
}
```

### Summarize network traffic anomalies

```tql
from network_logs.json
where bytes_transferred > 1000000
ai "Summarize the high-volume network transfers in this data. 
    Identify the top sources and destinations, and flag any 
    unusual patterns or potential data exfiltration indicators.",
    api_key="sk-...",
    system="You are a network security analyst."
```

```tql
{
  response: "Network Traffic Analysis Summary:\n\n1. Top Sources: 192.168.1.100 (45% of traffic)..."
}
```

### Extract key insights from application logs

```tql
from app_logs.json
where level in ["ERROR", "WARN"]
ai "Review these application errors and warnings. Group similar 
    issues together and prioritize them by severity and frequency.",
    api_key="sk-...",
    model="gpt-4"
```

```tql
{
  response: "Application Log Analysis:\n\nHigh Priority Issues:\n1. Database connection timeouts (12 occurrences)..."
}
```

### Generate incident summary

```tql
from security_alerts.json
where severity == "high"
ai "Create a concise incident summary from these high-severity alerts. 
    Include timeline, affected systems, and recommended actions.",
    api_key="anthropic-key-...",
    model="claude-3-sonnet"
```

```tql
{
  response: "INCIDENT SUMMARY\n\nTimeline: 14:30-15:45 UTC\nAffected Systems: Web servers cluster-01..."
}
```
