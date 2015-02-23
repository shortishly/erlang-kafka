%% Copyright (c) 2013-2015 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(cs_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).



all() ->
    common:all().

groups() ->
    common:groups(?MODULE).


init_per_suite(Config) ->
    ok = transform(Config, "zookeeper.properties"),
    ok = transform(Config, "kafka0.properties"),
    ok = transform(Config, "kafka1.properties"),
    ok = transform(Config, "kafka2.properties"),
    {ok, _} = run(Config, "zookeeper-server-start.sh", "zookeeper.properties"),
    {ok, _} = run(Config, "kafka-server-start.sh", "kafka0.properties"),
    {ok, C0} = ekc:start([{port, 9092}]),
    {ok, _} = run(Config, "kafka-server-start.sh", "kafka1.properties"),
    {ok, C1} = ekc:start([{port, 9093}]),
    {ok, _} = run(Config, "kafka-server-start.sh", "kafka2.properties"),
    {ok, C2} = ekc:start([{port, 9094}]),
    ok = producer(Config, "pg100", "pg100.txt"),
    ok = producer(Config, "simple", "simple.txt"),
    [{{connection, 0}, C0}, {{connection, 1}, C1}, {{connection, 2}, C2} | Config].



end_per_suite(Config) ->
    ok = ekc:stop(connection(Config, 2)),
    ok = run(Config, "kafka-server-stop.sh", "kafka2.properties"),
    ok = ekc:stop(connection(Config, 1)),
    ok = run(Config, "kafka-server-stop.sh", "kafka1.properties"),
    ok = ekc:stop(connection(Config, 0)),
    ok = run(Config, "kafka-server-stop.sh", "kafka0.properties"),
    ok = run(Config, "zookeeper-server-stop.sh", "zookeeper.properties").


transform(Config, Name) ->
    {ok, Original} = file:read_file(filename:join(common:data_dir(Config), Name)),
    ok = file:write_file(filename:join(common:priv_dir(Config), Name), binary:replace(Original, <<"PRIV_DIR">>, list_to_binary(common:priv_dir(Config)))).


metadata_test(Config) ->
    {ok, Metadata} = ekc:metadata(connection(Config, 0)),
    3 = length(ekc_metadata:brokers(Metadata)),
    2 = length(ekc_metadata:topics(Metadata)).

metadata_missing_topic_test(Config) ->
    {ok, Metadata} = ekc:metadata(connection(Config, 0), [<<"missing">>]),
    3 = length(ekc_metadata:brokers(Metadata)),
    [Topic] = ekc_metadata:topics(Metadata),
    leader_not_available = ekc_topic:error_code(Topic),
    <<"missing">> = ekc_topic:name(Topic),
    [] = ekc_topic:partitions(Topic).

pg100_partitions_test(Config) ->
    {ok, Metadata} = ekc:metadata(connection(Config, 0), [<<"pg100">>]),
    3 = length(ekc_metadata:brokers(Metadata)),
    [Topic] = ekc_metadata:topics(Metadata),
    no_error = ekc_topic:error_code(Topic),
    <<"pg100">> = ekc_topic:name(Topic).

simple_test(Config) ->
    Name = <<"pg100">>,
    {ok, Metadata} = ekc:metadata(connection(Config, 0), [Name]),
    3 = length(ekc_metadata:brokers(Metadata)),
    [Topic] = ekc_metadata:topics(Metadata),
    no_error = ekc_topic:error_code(Topic),
    Name = ekc_topic:name(Topic),
    lists:foldl(fun
		    (Partition, A) ->
			Connection = connection(Config, ekc_partition:leader(Partition)),

			{ok, [Earliest]} = ekc:offset(Connection, -1, [earliest(Topic, Partition)]),
			{ok, [Latest]} = ekc:offset(Connection, -1, [latest(Topic, Partition)]),

			case {offsets(Earliest), offsets(Latest)} of
			    {[0], [0]} ->
				A;
			    {[Low], [_High]} ->
				[ekc:fetch(Connection, -1, 1000, 0, [{Name, [{ekc_partition:id(Partition), Low, 2048}]}]) | A]
			end
		end,
		[],
		ekc_topic:partitions(Topic)).

offsets(Topic) ->
    lists:foldl(fun
		    (Partition, A) ->
			A ++ ekc_partition:offsets(Partition)
		end,
		[],
		ekc_topic:partitions(Topic)).



latest(Topic, Partition) ->    
    {ekc_topic:name(Topic), [{ekc_partition:id(Partition), -1, 1}]}.

earliest(Topic, Partition) ->
    {ekc_topic:name(Topic), [{ekc_partition:id(Partition), -2, 1}]}.
    
    

run(Config, Command, Properties) ->
    Parent = self(),
    Child = spawn(fun
		      () ->
			  Port = erlang:open_port({spawn, Command ++ " " ++ filename:join(common:priv_dir(Config), Properties)}, [exit_status, in, binary, {line, 8192}]),
			  run_loop(Parent, Port)
		  end),
    receive
	ready ->
	    {ok, Child};

	{exit_status, 0} = Exit ->
	    ct:log("~p", [[{command, Command}, {exit, Exit}]]),
	    ok;

	{exit_status, Code} = Exit ->
	    ct:pal("~p", [[{command, Command}, {exit, Exit}]]),
	    {error, Code};
	
	Otherwise ->
	    ct:pal("~p", [[{otherwise, Otherwise}, {module, ?MODULE}, {line, ?LINE}]]),
	    otherwise
    end.

run_loop(Parent, Port) ->
    receive
	{Port, {data, {eol, <<_:25/bytes, " INFO binding to port ", _/binary>> = Data}}} ->
	    %% zookeeper
	    ct:log("~p", [binary_to_list(Data)]),
	    Parent ! ready,
	    run_loop(Parent, Port);

	{Port, {data, {eol, <<_:25/bytes, " INFO [Kafka Server ", _:1/bytes, "], started ", _/binary>> = Data}}} ->
	    %% kafka
	    ct:log("~p", [binary_to_list(Data)]),
	    Parent ! ready,
	    run_loop(Parent, Port);

	{Port, {data, {eol, Data}}} ->
	    ct:log("~p", [binary_to_list(Data)]),
	    run_loop(Parent, Port);
	
	{Port, {exit_status, _} = Exit} ->
	    Parent ! Exit;

	Otherwise ->
	    ct:pal("~p", [[{port, Port}, {otherwise, Otherwise}, {module, ?MODULE}, {line, ?LINE}]])
    end.

producer(Config, Topic, Messages) ->
    os:cmd("kafka-console-producer.sh --sync --broker-list localhost:9092,localhost:9093,localhost:9094 --topic " ++ Topic ++ " <" ++ filename:join(common:data_dir(Config), Messages)),
    ok.


connection(Config, N) ->
    ?config({connection, N}, Config).
