%% Copyright (c) 2014-2015 Peter Morgan <peter.james.morgan@gmail.com>
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

-module(ekc).
-behaviour(gen_server).

-export([
	 start/0,
	 start/1,
	 start_link/0,
	 start_link/1, 
	 metadata/1,
	 metadata/2,
	 consumer_metadata/2,
	 fetch/5,
	 offset/3,
	 stop/1,
	 test/0
	]).

-export([
	 init/1,
	 code_change/3,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2
	]).

-export([
	 make/0
	]).


-export_type([
	      message_set/0,
	      broker/0,
	      partition/0,
	      topic/0,
	      metadata/0,
	      error_code/0,
	      consumer_metadata/0
	     ]).

-include("ekc.hrl").
-include("ekc_protocol.hrl").


-type error_code() :: no_error |
		      unknown |
		      offset_out_of_range |
		      invalid_message |
		      unknown_topic_or_partition |
		      invalid_message_size |
		      leader_not_available |
		      not_leader_for_partition |
		      request_timed_out |
		      broker_not_available |
		      replica_not_available |
		      message_size_too_large |
		      stale_controller_epoch |
		      offset_metadata_too_large |
		      offsets_load_in_progress |
		      consumer_coordinator_not_available |
		      not_coordinator_for_consumer.

-opaque message_set() :: #message_set{}.
-opaque broker() :: #broker{}.
-opaque partition() :: #partition{}.
-opaque topic() :: #topic{}.
-opaque metadata() :: #metadata{}.
-opaque consumer_metadata() :: #consumer_metadata{}.



start() ->
    start([]).

start(Parameters) ->
    gen_server:start(?MODULE, Parameters, []).

start_link() ->
    start_link([]).

start_link(Parameters) ->
    gen_server:start_link(?MODULE, Parameters, []).


-spec metadata(pid()) -> {ok, metadata()}.
metadata(Server) ->
    gen_server:call(Server, metadata).

-spec metadata(pid(), list(binary())) -> {ok, metadata()}.
metadata(Server, TopicNames) ->
    gen_server:call(Server, {metadata, TopicNames}).


-spec fetch(pid(),
	    integer(), 
	    integer(), 
	    integer(), 
	    list(binary() | {binary(), 
			     {non_neg_integer(), 
			      non_neg_integer(), 
			      pos_integer()}})) -> {ok, list(topic())}.
fetch(Server, ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    gen_server:call(Server, {fetch, ReplicaId, MaxWaitTime, MinBytes, Topics}).


-spec offset(pid(),
	     integer(), 
	     list(binary() | {binary(), 
			      {non_neg_integer(), 
			       non_neg_integer(), 
			       pos_integer()}})) -> ok.
offset(Server, ReplicaId, Topics) ->
    gen_server:call(Server, {offset, ReplicaId, Topics}).

consumer_metadata(Server, ConsumerGroup) ->
    gen_server:call(Server, {consumer_metadata, ConsumerGroup}).


stop(Server) ->
    gen_server:cast(Server, stop).
    
    
    



init(Parameters) ->
    [self() ! Parameter || Parameter <- Parameters],
    self() ! connect,
    {ok, #protocol_state{}}.

handle_call(metadata, From, S) ->
    make_request(ekc_protocol_metadata:request(S), 
		 From, 
		 fun ekc_protocol_metadata:response/1, 
		 S);

handle_call({metadata, TopicNames}, From, S) ->
    make_request(ekc_protocol_metadata:request(TopicNames, S), 
		 From, 
		 fun ekc_protocol_metadata:response/1, 
		 S);

handle_call({fetch, ReplicaId, MaxWaitTime, MinBytes, Topics}, From, S) ->
    make_request(ekc_protocol_fetch:request(ReplicaId, MaxWaitTime, MinBytes, Topics, S), 
		 From, 
		 fun ekc_protocol_fetch:response/1, 
		 S);

handle_call({offset, ReplicaId, Topics}, From, S) ->
    make_request(ekc_protocol_offset:request(ReplicaId, Topics, S), 
		 From, 
		 fun ekc_protocol_offset:response/1, 
		 S);

handle_call({consumer_metadata, ConsumerGroup}, From, S) ->
    make_request(ekc_protocol_consumer_metadata:request(ConsumerGroup, S), 
		 From, 
		 fun ekc_protocol_consumer_metadata:response/1, 
		 S).


handle_cast(stop, S) ->
    {stop, normal, S}.



handle_info({port, Port}, S) when is_integer(Port) ->
    {noreply, S#protocol_state{port = Port}};

handle_info({host, Host}, S) -> 
    {noreply, S#protocol_state{host = Host}};

handle_info({correlation_id, CorrelationId}, S) when is_integer(CorrelationId) ->
    {noreply, S#protocol_state{correlation_id = CorrelationId}};

handle_info({client_id, ClientId}, S) when is_binary(ClientId) ->
    {noreply, S#protocol_state{client_id = ClientId}};

handle_info(connect, #protocol_state{host = Host, port = Port} = S) ->
    case gen_tcp:connect(Host, Port, [{mode, binary}, {active, once}]) of
	{ok, Socket} ->
	    {noreply, S#protocol_state{socket = Socket}};

	{error, Reason} ->
	    {stop, Reason}
    end;

handle_info({tcp, _, 
	     <<
	       Size:32/signed, 
	       Remainder/binary
	     >> = Packet},
	    #protocol_state{parts = <<>>} = S) when Size == byte_size(Remainder) ->
    process_packet(Packet, S);

handle_info({tcp, _, Part}, 
	    #protocol_state{
	       parts = 
		   <<
		     Size:32/signed, 
		     _/binary
		   >> = Parts} = S) when byte_size(
					   <<
					     Parts/binary, 
					     Part/binary
					   >>) >= Size ->
    <<
      Size:32/signed, 
      Packet:Size/bytes, 
      Remainder/binary
    >> = <<Parts/binary, Part/binary>>,
    process_packet(
      <<
	Size:32, 
	Packet/binary
      >>, S#protocol_state{parts = Remainder});


handle_info({tcp, _, Part}, 
	    #protocol_state{
	       parts = Parts, 
	       socket = Socket} = S) ->
    case active(Socket, once) of
	ok ->
	    {noreply, S#protocol_state{
			parts = 
			    <<
			      Parts/binary, 
			      Part/binary
			    >>}};

	{error, _} = Reason ->
	    {stop, Reason, S}
    end;

handle_info({tcp_closed, _}, S) ->
    {stop, normal, S}.




code_change(_, State, _) ->
    {ok, State}.

terminate(_, _) ->
    ok.



-spec make_request(#request{}, 
		   {pid(), term()}, 
		   ekc_protocol:handler(), 
		   #protocol_state{}) -> {noreply, #protocol_state{}} | {stop, _, _, #protocol_state{}}.

make_request(#request{
		packet = Packet, 
		state = S2}, 
	     From, 
	     Handler, 
	     #protocol_state{
		correlation_id = CorrelationId, 
		requests = Requests, 
		socket = Socket} = S1) ->
    case gen_tcp:send(Socket, Packet) of
	ok ->
	    case active(Socket, once) of
		ok ->
		    {noreply, 
		     S2#protocol_state{
		       requests = orddict:store(CorrelationId, 
						#response{
						   from = From, 
						   handler = Handler}, 
						Requests)}};

		{error, _} = Error ->
		    {stop, abnormal, Error, S1}
	    end;

	{error, _} = Error ->
	    {stop, abnormal, Error, S1}
    end.
    

process_packet(
  <<
    Size:32/signed, 
    Packet:Size/bytes
  >>,
  #protocol_state{
     requests = Requests, 
     socket = Socket} = S) ->

    <<
      CorrelationId:32/signed, 
      Remainder/bytes>> = Packet,

    case orddict:find(CorrelationId, Requests) of

	{ok, #response{from = From, handler = ResponseHandler}} ->
	    gen_server:reply(From, ResponseHandler(Remainder)),
	    case active(Socket, once) of
		ok ->
		    {noreply, S#protocol_state{requests = orddict:erase(CorrelationId, Requests)}};

		{error, _} = Reason ->
		    {stop, Reason, S}
	    end;

	error ->
	    case active(Socket, once) of
		ok ->
		    {noreply, S};

		{error, _} = Reason ->
		    {stop, Reason, S}
	    end
    end.


active(Socket, HowMuch) ->
    inet:setopts(Socket, [{active, HowMuch}]).



make() ->
    make:all([load]).
    


stream() ->
    fun(MessageSet) ->
	    error_logger:info_report([{message_set, ekc_message_set:value(MessageSet)}]),
	    ok
    end.


test() ->
    {ok, C} = ekc_cluster:start("dev001.local", 9092),
    sys:trace(C, true),
    ekc_cluster:topics(C),
    ekc_cluster:stream(C, <<"test">>, <<"pg100">>, stream()),
    timer:sleep(60000),
    ekc_cluster:stop(C).
