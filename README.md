# opentelemetry_shigoto

OpenTelemetry instrumentation for the [Shigoto](https://github.com/Taure/shigoto) background job system.

## Installation

### Erlang (rebar3)

```erlang
{deps, [
    {opentelemetry_shigoto, {git, "https://github.com/Taure/opentelemetry_shigoto.git", {branch, "main"}}}
]}.
```

### Elixir (Mix)

```elixir
{:opentelemetry_shigoto, github: "Taure/opentelemetry_shigoto", branch: "main"}
```

## Usage

Call `setup/0` in your application's `start/2`:

```erlang
opentelemetry_shigoto:setup().
```

```elixir
:opentelemetry_shigoto.setup()
```

## Spans

| Event | Span Name | Kind |
|-------|-----------|------|
| Job completed | `shigoto.job.execute Worker` | CONSUMER |
| Job failed | `shigoto.job.execute Worker` | CONSUMER (ERROR) |
| Job inserted | `shigoto.job.insert Worker` | PRODUCER |
| Job snoozed | `shigoto.job.snooze Worker` | CONSUMER |
| Job discarded | `shigoto.job.discard Worker` | CONSUMER (ERROR) |
| Job cancelled | `shigoto.job.cancel Worker` | CONSUMER |
| Batch completed | `shigoto.batch.complete` | CONSUMER |

## Attributes

- `shigoto.job.id` — Job ID
- `shigoto.job.worker` — Worker module name
- `shigoto.job.queue` — Queue name
- `shigoto.job.attempt` — Current attempt number
- `shigoto.job.duration_ms` — Execution duration (completed/failed only)
