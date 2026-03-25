-module(opentelemetry_shigoto).
-moduledoc """
OpenTelemetry instrumentation for Shigoto background jobs.

Subscribes to Shigoto's telemetry events and creates OTel spans
and metrics for job execution, queue operations, and resilience.

## Setup

```erlang
opentelemetry_shigoto:setup().
```

Or with options:

```erlang
opentelemetry_shigoto:setup(#{
    span_prefix => <<"shigoto">>
}).
```
""".

-export([setup/0, setup/1]).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(HANDLER_ID, opentelemetry_shigoto).

-doc "Set up OpenTelemetry instrumentation with default options.".
-spec setup() -> ok.
setup() ->
    setup(#{}).

-doc "Set up OpenTelemetry instrumentation with options.".
-spec setup(map()) -> ok.
setup(Opts) ->
    Events = [
        [shigoto, job, completed],
        [shigoto, job, failed],
        [shigoto, job, inserted],
        [shigoto, job, claimed],
        [shigoto, job, snoozed],
        [shigoto, job, discarded],
        [shigoto, job, cancelled],
        [shigoto, queue, poll],
        [shigoto, queue, paused],
        [shigoto, queue, resumed],
        [shigoto, resilience, rate_limited],
        [shigoto, resilience, circuit_open],
        [shigoto, resilience, bulkhead_full],
        [shigoto, resilience, load_shed],
        [shigoto, batch, completed],
        [shigoto, cron, scheduled]
    ],
    telemetry:attach_many(?HANDLER_ID, Events, fun handle_event/4, Opts),
    ok.

%%----------------------------------------------------------------------
%% Telemetry handlers
%%----------------------------------------------------------------------

handle_event([shigoto, job, completed], Measurements, Metadata, Opts) ->
    Tracer = tracer(),
    SpanName = span_name(<<"job.execute">>, Metadata, Opts),
    DurationMs = maps:get(duration_ms, Measurements, 0),
    Attributes = job_attributes(Metadata, Measurements),
    Ctx = otel_tracer:start_span(Tracer, SpanName, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => Attributes
    }),
    otel_span:set_status(Ctx, ?OTEL_STATUS_OK, <<>>),
    EndTime = opentelemetry:timestamp(),
    StartTime = EndTime - erlang:convert_time_unit(DurationMs, millisecond, nanosecond),
    otel_span:set_attribute(Ctx, ~"shigoto.start_time", StartTime),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, failed], Measurements, Metadata, Opts) ->
    Tracer = tracer(),
    SpanName = span_name(<<"job.execute">>, Metadata, Opts),
    Reason = maps:get(reason, Metadata, <<"unknown">>),
    Attributes = job_attributes(Metadata, Measurements),
    Ctx = otel_tracer:start_span(Tracer, SpanName, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => Attributes#{~"error.message" => Reason}
    }),
    otel_span:set_status(Ctx, ?OTEL_STATUS_ERROR, Reason),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, inserted], _Measurements, Metadata, Opts) ->
    Tracer = tracer(),
    SpanName = span_name(<<"job.insert">>, Metadata, Opts),
    Attributes = #{
        ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
        ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
        ~"shigoto.queue" => maps:get(queue, Metadata, <<>>),
        ~"shigoto.priority" => maps:get(priority, Metadata, 0)
    },
    Ctx = otel_tracer:start_span(Tracer, SpanName, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => Attributes
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, claimed], Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    QueueWait = maps:get(queue_wait_ms, Measurements, 0),
    Attributes = #{
        ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
        ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
        ~"shigoto.queue" => maps:get(queue, Metadata, <<>>),
        ~"shigoto.queue_wait_ms" => QueueWait
    },
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.job.claim">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => Attributes
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, snoozed], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Reason = maps:get(snooze_reason, Metadata, <<>>),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.job.snooze">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
            ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
            ~"shigoto.snooze_reason" => Reason
        }
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, discarded], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.job.discard">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
            ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
            ~"shigoto.attempt" => maps:get(attempt, Metadata, 0)
        }
    }),
    otel_span:set_status(Ctx, ?OTEL_STATUS_ERROR, <<"discarded">>),
    otel_span:end_span(Ctx);
handle_event([shigoto, job, cancelled], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.job.cancel">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
            ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>))
        }
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, queue, poll], Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.queue.poll">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.queue" => maps:get(queue, Metadata, <<>>),
            ~"shigoto.claimed" => maps:get(claimed, Measurements, 0)
        }
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, queue, paused], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.queue.pause">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{~"shigoto.queue" => maps:get(queue, Metadata, <<>>)}
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, queue, resumed], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.queue.resume">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{~"shigoto.queue" => maps:get(queue, Metadata, <<>>)}
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, resilience, rate_limited], Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.resilience.rate_limited">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
            ~"shigoto.retry_after_ms" => maps:get(retry_after_ms, Measurements, 0)
        }
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, resilience, circuit_open], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.resilience.circuit_open">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>))}
    }),
    otel_span:set_status(Ctx, ?OTEL_STATUS_ERROR, <<"circuit_open">>),
    otel_span:end_span(Ctx);
handle_event([shigoto, resilience, bulkhead_full], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.resilience.bulkhead_full">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>))}
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, resilience, load_shed], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.resilience.load_shed">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{~"shigoto.priority" => maps:get(priority, Metadata, 0)}
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, batch, completed], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.batch.complete">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.batch_id" => maps:get(batch_id, Metadata, 0),
            ~"shigoto.total_jobs" => maps:get(total_jobs, Metadata, 0),
            ~"shigoto.completed_jobs" => maps:get(completed_jobs, Metadata, 0),
            ~"shigoto.discarded_jobs" => maps:get(discarded_jobs, Metadata, 0)
        }
    }),
    otel_span:end_span(Ctx);
handle_event([shigoto, cron, scheduled], _Measurements, Metadata, _Opts) ->
    Tracer = tracer(),
    Ctx = otel_tracer:start_span(Tracer, <<"shigoto.cron.schedule">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{
            ~"shigoto.cron.name" => to_binary(maps:get(name, Metadata, <<>>)),
            ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
            ~"shigoto.cron.schedule" => maps:get(schedule, Metadata, <<>>)
        }
    }),
    otel_span:end_span(Ctx);
handle_event(_Event, _Measurements, _Metadata, _Opts) ->
    ok.

%%----------------------------------------------------------------------
%% Internal
%%----------------------------------------------------------------------

tracer() ->
    opentelemetry:get_application_tracer(?MODULE).

span_name(Base, Metadata, Opts) ->
    Prefix = maps:get(span_prefix, Opts, <<"shigoto">>),
    Worker = to_binary(maps:get(worker, Metadata, <<>>)),
    case Worker of
        <<>> -> <<Prefix/binary, ".", Base/binary>>;
        _ -> <<Prefix/binary, ".", Base/binary, " ", Worker/binary>>
    end.

job_attributes(Metadata, Measurements) ->
    #{
        ~"shigoto.job_id" => maps:get(job_id, Metadata, 0),
        ~"shigoto.worker" => to_binary(maps:get(worker, Metadata, <<>>)),
        ~"shigoto.queue" => maps:get(queue, Metadata, <<>>),
        ~"shigoto.attempt" => maps:get(attempt, Metadata, 0),
        ~"shigoto.priority" => maps:get(priority, Metadata, 0),
        ~"shigoto.duration_ms" => maps:get(duration_ms, Measurements, 0),
        ~"shigoto.queue_wait_ms" => maps:get(queue_wait_ms, Measurements, 0)
    }.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_atom(V) -> atom_to_binary(V);
to_binary(V) when is_list(V) -> list_to_binary(V);
to_binary(V) -> iolist_to_binary(io_lib:format("~p", [V])).
