-module(opentelemetry_shigoto_tests).
-include_lib("eunit/include/eunit.hrl").

format_worker_atom_test() ->
    ?assertEqual(<<"my_worker">>, opentelemetry_shigoto:format_worker(my_worker)).

format_worker_binary_test() ->
    ?assertEqual(<<"my_worker">>, opentelemetry_shigoto:format_worker(<<"my_worker">>)).

format_worker_unknown_test() ->
    ?assertEqual(<<"unknown">>, opentelemetry_shigoto:format_worker(123)).

span_name_with_worker_test() ->
    ?assertEqual(
        <<"shigoto.job.execute my_worker">>,
        opentelemetry_shigoto:span_name(<<"execute">>, #{worker => my_worker})
    ).

span_name_with_binary_worker_test() ->
    ?assertEqual(
        <<"shigoto.job.insert my_worker">>,
        opentelemetry_shigoto:span_name(<<"insert">>, #{worker => <<"my_worker">>})
    ).

span_name_without_worker_test() ->
    ?assertEqual(
        <<"shigoto.job.execute">>,
        opentelemetry_shigoto:span_name(<<"execute">>, #{})
    ).

span_name_snooze_test() ->
    ?assertEqual(
        <<"shigoto.job.snooze rate_limited_worker">>,
        opentelemetry_shigoto:span_name(<<"snooze">>, #{worker => rate_limited_worker})
    ).

span_name_discard_test() ->
    ?assertEqual(
        <<"shigoto.job.discard failing_worker">>,
        opentelemetry_shigoto:span_name(<<"discard">>, #{worker => failing_worker})
    ).

span_name_cancel_test() ->
    ?assertEqual(
        <<"shigoto.job.cancel some_worker">>,
        opentelemetry_shigoto:span_name(<<"cancel">>, #{worker => some_worker})
    ).
