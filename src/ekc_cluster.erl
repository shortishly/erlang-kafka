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

-module(ekc_cluster).
-behaviour(gen_server).

-export([
	 start/2,
	 start_link/2,
	 stream/4,
	 topics/1,
	 stop/1
	]).

-export([
	 init/1,
	 code_change/3,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2
	]).


-include("ekc.hrl").


start(Host, Port) ->
    gen_server:start(?MODULE, [{host, Host}, {port, Port}], []).

start_link(Host, Port) ->
    gen_server:start_link(?MODULE, [{host, Host}, {port, Port}], []).

-spec stream(pid(), binary(), binary(), fun((ekc:message_set()) -> ok)) -> ok.
stream(Cluster, ConsumerGroup, Topic, F) ->
    gen_server:cast(Cluster, {stream, ConsumerGroup, Topic, F}).

topics(Cluster) ->
    gen_server:call(Cluster, topics).

-spec stop(pid()) -> ok.		     
stop(Consumer) ->
    gen_server:cast(Consumer, stop).


-record(state, {
	  host,
	  port,
	  servers :: pid(),
	  brokers = #{},
	  monitors = #{},
	  max_bytes = 1024
	 }).

init(Parameters) ->
    process_flag(trap_exit, true),
    self() ! supervisor,
    [self() ! Parameter || Parameter <- Parameters],
    self() ! connect,
    {ok, #state{}}.


handle_call(topics, _, #state{brokers = Brokers} = S) ->
    #{0 := Broker} = Brokers,
    {ok, Metadata} = ekc_protocol_server:metadata(Broker),
    {reply, [ekc_topic:name(Topic) || Topic <- ekc_metadata:topics(Metadata)], connect_to_brokers_in_metadata(Metadata, S)}.

handle_cast({stream, ConsumerGroup, TopicName, F}, S) ->
    {noreply, topic(ConsumerGroup, TopicName, F, S)};

handle_cast(stop, S) ->
    {stop, normal, S}.


handle_info(supervisor, S) ->
    {ok, Servers} = ekc_protocol_server_supervisor:start_link(),
    {noreply, S#state{servers = Servers}};

handle_info({host, Host}, S) ->
    {noreply, S#state{host = Host}};

handle_info({port, Port}, S) ->
    {noreply, S#state{port = Port}};

handle_info({max_bytes, MaxBytes}, S) ->
    {noreply, S#state{max_bytes = MaxBytes}};

handle_info(connect, #state{host = Host, port = Port} = S) ->
    {ok, C} = ekc_protocol_server:start(Host, Port),
    {ok, Metadata} = ekc_protocol_server:metadata(C),
    ok = ekc_protocol_server:stop(C),
    {noreply, connect_to_brokers_in_metadata(Metadata, S)};

handle_info({message_set, ConsumerGroup, ConsumerBroker, Partition, TopicName, #message_set{offset = Offset} = MessageSet, F}, S) ->
    ok = F(MessageSet),
    ekc_protocol_server:offset_commit(ConsumerBroker, ConsumerGroup, [{TopicName, [{Partition, Offset, -1, <<"">>}]}]),
    {noreply, S};

handle_info({fetch, ConsumerGroup, ConsumerMetadata, #metadata{} = Metadata, [#topic{name = Name, partitions = [#partition{id = Partition, offsets = [Offset]}]}], F}, #state{max_bytes = MaxBytes} = S) ->
    {ok, [#topic{partitions = [#partition{message_sets = MessageSets}]}]} = ekc_protocol_server:fetch(isr(Partition, Metadata, S), -1, 0, 0, [{Name, [{Partition, offset(Offset), MaxBytes}]}]),
    [self() ! {message_set, ConsumerGroup, broker(ConsumerMetadata, S), Partition, Name, MessageSet, F} || MessageSet <- MessageSets],
    {noreply, topic(ConsumerGroup, Name, F, S)}.


code_change(_, State, _) ->
    {ok, State}.

terminate(_, #state{brokers = Brokers}) ->
    [ekc_protocol_server:stop(Broker) || Broker <- maps:values(Brokers)],
    ok.


broker(#consumer_metadata{id = Replica}, #state{brokers = Brokers}) ->
    maps:get(Replica, Brokers).


isr(Partition, #metadata{topics = [#topic{partitions = [#partition{id = Partition, isr = [ISR]} | _]} | _]}, #state{brokers = Brokers}) ->
    maps:get(ISR, Brokers);

isr(Partition, #metadata{topics = [#topic{partitions = [_ | Partitions]} = Topic | Topics]} = Metadata, S) ->
    isr(Partition, Metadata#metadata{topics = [Topic#topic{partitions = Partitions} | Topics]}, S).


offset(-1) ->
    0;
offset(Offset) ->
    Offset.

-spec connect_to_brokers_in_metadata(ekc:metadata(), #state{}) -> #state{}.
connect_to_brokers_in_metadata(Metadata, S) ->
    connect_to_brokers(ekc_metadata:brokers(Metadata), S).


-spec connect_to_brokers(list(ekc:broker()), #state{}) -> #state{}.
connect_to_brokers(Brokers, S) ->
    lists:foldl(fun connect_to_broker/2, S, Brokers).


-spec connect_to_broker(ekc:broker(), #state{}) -> #state{}.
connect_to_broker(Broker, #state{brokers = Brokers} = S) ->
    case maps:find(ekc_broker:id(Broker), Brokers) of
	{ok, _} ->
	    S;

	error ->
	    start_child(Broker, S)
    end.

-spec start_child(ekc:broker(), #state{}) -> {ok, pid()}.
start_child(Broker, #state{brokers = Brokers, servers = Servers, monitors = Monitors} = S) ->
    {ok, C} = supervisor:start_child(Servers, [binary_to_list(ekc_broker:host(Broker)), ekc_broker:port(Broker)]),
    S#state{brokers = maps:put(ekc_broker:id(Broker), C, Brokers), monitors = maps:put(C, monitor(process, C), Monitors)}.
    


-spec topic(binary(), binary(), fun((ekc:message_set()) -> ok), #state{}) -> #state{}.
topic(ConsumerGroup, TopicName, F, #state{brokers = Brokers} = S) ->
    #{0 := Broker} = Brokers,
    {ok, ConsumerMetadata} = ekc_protocol_server:consumer_metadata(Broker, ConsumerGroup),
    {ok, Metadata} = ekc_protocol_server:metadata(Broker, [TopicName]),
    {ok, Offsets} = offset_fetch(ConsumerGroup, ConsumerMetadata, Metadata, S),
    self() ! {fetch, ConsumerGroup, ConsumerMetadata, Metadata, Offsets, F},
    S.


-spec offset_fetch(binary(), ekc:consumer_metadata(), ekc:metadata(), #state{}) -> integer().
offset_fetch(ConsumerGroup, #consumer_metadata{id = Id}, #metadata{topics = [#topic{name = Name, partitions = Partitions}]}, #state{brokers = Brokers}) ->
    ekc_protocol_server:offset_fetch(maps:get(Id, Brokers), ConsumerGroup, [{Name, [Partition || #partition{id = Partition} <- Partitions]}]).


    

    
