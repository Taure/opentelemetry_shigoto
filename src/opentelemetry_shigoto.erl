-module(opentelemetry_shigoto).
-moduledoc """
OpenTelemetry instrumentation for Shigoto.

Subscribes to Shigoto's telemetry events and creates OpenTelemetry spans
for job execution, insertion, and lifecycle events.

```erlang
%% In your application's start/2
opentelemetry_shigoto:setup().
```

## Spans

- `shigoto.job.execute` — Job execution (completed or failed). Includes
  execution duration, worker, queue, attempt, and error status on failure.
- `shigoto.job.insert` — Job insertion.
- `shigoto.job.snooze` — Job snoozed (rate limited, circuit open, etc.).
- `shigoto.job.discard` — Job permanently discarded after max attempts.
- `shigoto.job.cancel` — Job cancelled.
- `shigoto.batch.complete` — All jobs in a batch finished.

## Attributes

All spans include:
- `shigoto.job.id` — Job ID
- `shigoto.job.worker` — Worker module name
- `shigoto.job.queue` — Queue name
- `shigoto.job.attempt` — Current attempt number

Execution spans additionally include:
- `shigoto.job.duration_ms` — Execution duration in milliseconds

Failed spans set `OTEL_STATUS_ERROR` with the error reason.
""".

-export([setup/0, setup/1]).
-export([
    handle_completed/4,
    handle_failed/4,
    handle_inserted/4,
    handle_snoozed/4,
    handle_discarded/4,
    handle_cancelled/4,
    handle_batch_completed/4
]).
-export([format_worker/1, span_name/2]).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-doc "Attach all telemetry handlers with default options.".
-spec setup() -> ok.
setup() ->
    setup(#{}).

-doc """
Attach all telemetry handlers with options.

Options:
- `job_system` — Job system name (default: `<<"shigoto">>`)
""".
-spec setup(map()) -> ok.
setup(Opts) ->
    Handlers = [
        {<<"otel-shigoto-completed">>, [shigoto, job, completed], fun ?MODULE:handle_completed/4},
        {<<"otel-shigoto-failed">>, [shigoto, job, failed], fun ?MODULE:handle_failed/4},
        {<<"otel-shigoto-inserted">>, [shigoto, job, inserted], fun ?MODULE:handle_inserted/4},
        {<<"otel-shigoto-snoozed">>, [shigoto, job, snoozed], fun ?MODULE:handle_snoozed/4},
        {<<"otel-shigoto-discarded">>, [shigoto, job, discarded], fun ?MODULE:handle_discarded/4},
        {<<"otel-shigoto-cancelled">>, [shigoto, job, cancelled], fun ?MODULE:handle_cancelled/4},
        {<<"otel-shigoto-batch-completed">>, [shigoto, batch, completed],
            fun ?MODULE:handle_batch_completed/4}
    ],
    lists:foreach(
        fun({Id, Event, Handler}) ->
            telemetry:attach(Id, Event, Handler, Opts)
        end,
        Handlers
    ),
    ok.

%%----------------------------------------------------------------------
%% Handlers
%%----------------------------------------------------------------------

-doc false.
handle_completed(_Event, #{duration := Duration}, Metadata, Config) ->
    Attributes = job_attributes(Metadata, Config),
    DurationMs = erlang:convert_time_unit(Duration, native, millisecond),
    Attributes1 = Attributes#{'shigoto.job.duration_ms' => DurationMs},
    SpanName = span_name(<<"execute">>, Metadata),
    StartTime = opentelemetry:timestamp() - Duration,
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{
            start_time => StartTime,
            kind => ?SPAN_KIND_CONSUMER,
            attributes => Attributes1
        }
    ),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok;
handle_completed(_Event, _Measurements, _Metadata, _Config) ->
    ok.

-doc false.
handle_failed(_Event, #{duration := Duration}, Metadata, Config) ->
    Attributes = job_attributes(Metadata, Config),
    DurationMs = erlang:convert_time_unit(Duration, native, millisecond),
    Attributes1 = Attributes#{'shigoto.job.duration_ms' => DurationMs},
    Reason = maps:get(reason, Metadata, <<"unknown">>),
    SpanName = span_name(<<"execute">>, Metadata),
    StartTime = opentelemetry:timestamp() - Duration,
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{
            start_time => StartTime,
            kind => ?SPAN_KIND_CONSUMER,
            attributes => Attributes1
        }
    ),
    ErrorMsg = format_error(Reason),
    otel_span:set_status(SpanCtx, ?OTEL_STATUS_ERROR, ErrorMsg),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok;
handle_failed(_Event, _Measurements, _Metadata, _Config) ->
    ok.

-doc false.
handle_inserted(_Event, _Measurements, Metadata, Config) ->
    Attributes = job_attributes(Metadata, Config),
    SpanName = span_name(<<"insert">>, Metadata),
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{kind => ?SPAN_KIND_PRODUCER, attributes => Attributes}
    ),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok.

-doc false.
handle_snoozed(_Event, _Measurements, Metadata, Config) ->
    Attributes0 = job_attributes(Metadata, Config),
    SnoozeReason = maps:get(snooze_reason, Metadata, <<"unknown">>),
    Attributes = Attributes0#{'shigoto.job.snooze_reason' => SnoozeReason},
    SpanName = span_name(<<"snooze">>, Metadata),
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{kind => ?SPAN_KIND_CONSUMER, attributes => Attributes}
    ),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok.

-doc false.
handle_discarded(_Event, _Measurements, Metadata, Config) ->
    Attributes = job_attributes(Metadata, Config),
    SpanName = span_name(<<"discard">>, Metadata),
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{kind => ?SPAN_KIND_CONSUMER, attributes => Attributes}
    ),
    otel_span:set_status(SpanCtx, ?OTEL_STATUS_ERROR, <<"job discarded">>),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok.

-doc false.
handle_cancelled(_Event, _Measurements, Metadata, Config) ->
    Attributes = job_attributes(Metadata, Config),
    SpanName = span_name(<<"cancel">>, Metadata),
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        SpanName,
        #{kind => ?SPAN_KIND_CONSUMER, attributes => Attributes}
    ),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok.

-doc false.
handle_batch_completed(_Event, _Measurements, Metadata, Config) ->
    JobSystem = maps:get(job_system, Config, <<"shigoto">>),
    Attributes = #{
        'shigoto.system' => JobSystem,
        'shigoto.batch.id' => maps:get(batch_id, Metadata, undefined),
        'shigoto.batch.total_jobs' => maps:get(total_jobs, Metadata, 0),
        'shigoto.batch.completed_jobs' => maps:get(completed_jobs, Metadata, 0),
        'shigoto.batch.discarded_jobs' => maps:get(discarded_jobs, Metadata, 0)
    },
    SpanCtx = otel_tracer:start_span(
        opentelemetry:get_application_tracer(?MODULE),
        <<"shigoto.batch.complete">>,
        #{kind => ?SPAN_KIND_CONSUMER, attributes => Attributes}
    ),
    otel_span:end_span(SpanCtx, opentelemetry:timestamp()),
    ok.

%%----------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------

-doc "Build span name from operation and worker metadata.".
-spec span_name(binary(), map()) -> binary().
span_name(Operation, #{worker := Worker}) ->
    WorkerBin = format_worker(Worker),
    <<"shigoto.job.", Operation/binary, " ", WorkerBin/binary>>;
span_name(Operation, _) ->
    <<"shigoto.job.", Operation/binary>>.

-doc "Format a worker name to binary.".
-spec format_worker(atom() | binary()) -> binary().
format_worker(Worker) when is_atom(Worker) ->
    atom_to_binary(Worker, utf8);
format_worker(Worker) when is_binary(Worker) ->
    Worker;
format_worker(_) ->
    <<"unknown">>.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

job_attributes(Metadata, Config) ->
    JobSystem = maps:get(job_system, Config, <<"shigoto">>),
    #{
        'shigoto.system' => JobSystem,
        'shigoto.job.id' => maps:get(job_id, Metadata, undefined),
        'shigoto.job.worker' => format_worker(maps:get(worker, Metadata, undefined)),
        'shigoto.job.queue' => maps:get(queue, Metadata, undefined),
        'shigoto.job.attempt' => maps:get(attempt, Metadata, 0)
    }.

format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).
