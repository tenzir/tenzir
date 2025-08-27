---
title: ai
category: AI
example: 'ai "Explain these events"'
---

Analyzes a set of events with an LLM.

```tql
ai prompt:string, model:string, api_key:secret
```

## Description

The `ai` operator sends event with user-provided prompt to a
Large Language Model (LLM). The operator collects all input events,
formats them as structured data, and sends them to an AI service for processing.
The AI's response is returned as a single output event of type `tenzir.ai` with
the response text in the `response` field.

This operator is particularly useful for:

- Analyzing patterns in log data or security events
- Summarizing large datasets with natural language
- Extracting insights from unstructured data
- Performing semantic analysis on event streams

The operator requires access to an LLM provider service. You must provide valid
API credentials for the selected provider. The provider is determined automatically
from the model name.

The operator supports the following models:

| Model | Identifier |
|-------|------------|
| Claude 3.5 Sonnet | `claude-3-5-sonnet-20241022` |
| Claude 3.5 Haiku | `claude-3-5-haiku-20241022` |
| Claude 3 Opus | `claude-3-opus-20240229` |
| Claude 3 Sonnet | `claude-3-sonnet-20240229` |
| Claude 3 Haiku | `claude-3-haiku-20240307` |
| GPT-4o | `gpt-4o` |
| GPT-4o mini | `gpt-4o-mini` |
| GPT-4 Turbo | `gpt-4-turbo` |
| GPT-4 | `gpt-4` |
| GPT-3.5 Turbo | `gpt-3.5-turbo` |

:::note[Batch Processing]
The `ai` operator processes all input events as a single batch before sending
them to the LLM provider. This means it will buffer the entire input stream in
memory before producing any output.
:::

### `prompt: string`

The prompt that instructs the LLM on how to analyze the data.
This prompt is sent along with a structured representation of all input events.

### `model: string`

The specific model to use from the LLM provider. The model name determines
which provider to use (e.g., "gpt-4" uses OpenAI, "claude-3-sonnet" uses Anthropic).

See the above description for a list of supported models.

### `api_key: secret`

The API key for authenticating with the LLM provider. This parameter is required
and the specific format depends on the provider (determined by the model name).

## Examples

### Analyze security events for suspicious patterns

```tql
from logs.json
where event_type == "authentication"
ai r"Analyze these authentication events for suspicious login patterns.
    Look for unusual geographic locations, failed login attempts, or
    off-hours access patterns.",
    model="claude-3-5-sonnet-20241022",
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
ai r"Summarize the high-volume network transfers in this data.
    Identify the top sources and destinations, and flag any
    unusual patterns or potential data exfiltration indicators.",
    model="gpt-4",
    api_key="sk-..."
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
ai r"Review these application errors and warnings. Group similar
    issues together and prioritize them by severity and frequency.",
    model="gpt-4",
    api_key="sk-..."
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
ai r"Create a concise incident summary from these high-severity alerts.
    Include timeline, affected systems, and recommended actions.",
    model="claude-3-opus-20240229",
    api_key="sk-ant-..."
```

```tql
{
  response: "INCIDENT SUMMARY\n\nTimeline: 14:30-15:45 UTC\nAffected Systems: Web servers cluster-01..."
}
```
