%% Copyright (c) 2014 Peter Morgan <peter.james.morgan@gmail.com>
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
	 stop/1
	]).

-export([
	 error_code/1
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
	      error_code/0
	     ]).

-include("ekc.hrl").



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
    gen_server:call(Server, stop).
    
    
    



init(Parameters) ->
    init(Parameters, #state{}).

init([{port, Port} | T], S) when is_integer(Port) ->
    init(T, S#state{port = Port});

init([{host, Host} | T], S) -> 
    init(T, S#state{host = Host});

init([{correlation_id, CorrelationId} | T], S) when is_integer(CorrelationId) ->
    init(T, S#state{correlation_id = CorrelationId});

init([{client_id, ClientId} | T], S) when is_binary(ClientId) ->
    init(T, S#state{client_id = ClientId});

init([], #state{host = Host, port = Port} = S) ->
    case gen_tcp:connect(Host, Port, [{mode, binary}, {active, once}]) of
	{ok, Socket} ->
	    {ok, S#state{socket = Socket}};

	{error, Reason} ->
	    {stop, Reason}
    end.




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
		 S);

handle_call(stop, _, S) ->
    {stop, normal, ok, S}.


handle_cast(_, S) ->
    {stop, abnormal, S}.


handle_info({tcp, _, 
	     <<
	       Size:32/signed, 
	       Remainder/binary
	     >> = Packet},
	    #state{parts = <<>>} = S) when Size == byte_size(Remainder) ->
    process_packet(Packet, S);

handle_info({tcp, _, Part}, 
	    #state{
	       parts = 
		   <<
		     Size:32/signed, 
		     Data/binary
		   >> = Parts} = S) when byte_size(
					   <<
					     Data/binary, 
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
      >>, S#state{parts = Remainder});


handle_info({tcp, _, Part}, 
	    #state{
	       parts = Parts, 
	       socket = Socket} = S) ->
    case active(Socket, once) of
	ok ->
	    {noreply, S#state{
			parts = 
			    <<
			      Parts/binary, 
			      Part/binary
			    >>}};

	{error, _} = Reason ->
	    {stop, Reason, S}
    end;

handle_info({tcp_closed, _}, S) ->
    {stop, abnormal, S}.




code_change(_, State, _) ->
    {ok, State}.

terminate(_, _) ->
    ok.



-spec make_request(#request{}, 
		   {pid(), term()}, 
		   ekc_protocol:handler(), 
		   #state{}) -> {noreply, #state{}} | {stop, _, _, #state{}}.

make_request(#request{
		packet = Packet, 
		state = S2}, 
	     From, 
	     Handler, 
	     #state{
		correlation_id = CorrelationId, 
		requests = Requests, 
		socket = Socket} = S1) ->
    case gen_tcp:send(Socket, Packet) of
	ok ->
	    case active(Socket, once) of
		ok ->
		    {noreply, 
		     S2#state{
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
  #state{
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
		    {noreply, S#state{requests = orddict:erase(CorrelationId, Requests)}};

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





error_code(0) ->
    no_error;
error_code(-1) ->
    unknown;
error_code(1) ->
    offset_out_of_range;
error_code(2) ->
    invalid_message;
error_code(3) ->
    unknown_topic_or_partition;
error_code(4) ->
    invalid_message_size;
error_code(5) ->
    leader_not_available;
error_code(6) ->
    not_leader_for_partition;
error_code(7) ->
    request_timed_out;
error_code(8) ->
    broker_not_available;
error_code(9) ->
    replica_not_available;
error_code(10) ->
    message_size_too_large;
error_code(11) ->
    stale_controller_epoch;
error_code(12) ->
    offset_metadata_too_large;
error_code(14) ->
    offsets_load_in_progress;
error_code(15) ->
    consumer_coordinator_not_available;
error_code(16) ->
    not_coordinator_for_consumer.


make() ->
    make:all([load]).
    
