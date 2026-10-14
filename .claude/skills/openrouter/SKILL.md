---
name: openrouter
description: Use this to write code to call a free open-source LLM using LiteLLM and OpenRouter
---

# Calling an LLM via OpenRouter

These instructions allow you to write code to call a free, open-weights LLM through OpenRouter.
This method uses LiteLLM.

## Setup

The OPENROUTER_API_KEY must be set in the .env file and loaded in as an environment variable.

The uv project must include litellm and pydantic.
`uv add litellm pydantic`

## Model choice

Use a free (`:free`) open-weights model that supports Structured Outputs. The default is
`openrouter/qwen/qwen3.8-27b:free`. A larger alternative is
`openrouter/nvidia/nemotron-3-super-120b-a12b:free`.

Make the model id configurable via an `OPENROUTER_MODEL` environment variable with the default
above, since the set of free models on OpenRouter changes over time.

Note: free models are rate limited by OpenRouter (per-minute and per-day caps on the free tier)
and can be slower or briefly unavailable. Handle request errors gracefully and surface a readable
message to the user rather than crashing the chat endpoint.

## Code snippets

### Imports and constants

```python
import os
from litellm import completion

MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/qwen/qwen3.8-27b:free")
```

### Code to call for a text response

```python
response = completion(model=MODEL, messages=messages, reasoning_effort="low")
result = response.choices[0].message.content
```

### Code to call for a Structured Outputs response

```python
response = completion(model=MODEL, messages=messages, response_format=MyBaseModelSubclass, reasoning_effort="low")
result = response.choices[0].message.content
result_as_object = MyBaseModelSubclass.model_validate_json(result)
```

### Code to handle request failures

```python
import litellm

try:
    response = completion(model=MODEL, messages=messages, response_format=MyBaseModelSubclass, reasoning_effort="low")
except litellm.RateLimitError:
    raise LLMUnavailable("The free model is rate limited right now. Please try again in a moment.")
except litellm.APIError as e:
    raise LLMUnavailable(f"The model could not be reached: {e}")
```

Catch `LLMUnavailable` in the chat route and return its message as the assistant `message` field, so the
user sees a readable note in the chat panel instead of a 500.
