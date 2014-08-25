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

-module(ekc_protocol_metadata).
-include("ekc.hrl").

-export([request/1,
	 request/2,
	 response/1]).


request(#state{} = S) ->
    request([], S).

request(TopicNames, #state{} = S) ->
    Topics = list_to_binary([
			     <<
			       (byte_size(Name)):16/signed, 
			       Name/bytes
			     >> || Name <- TopicNames]),
    ekc_protocol:request(?METADATA_REQUEST, 
			 <<
			   (length(TopicNames)):32/signed, 
			   Topics/bytes
			 >>,
			 S).



-spec response(<<_:32,_:_*8>>) -> {ok, ekc:metadata()}.

response(
  <<
    NumberOfBrokers:32/signed, 
    BrokerRemainder/binary
  >>) ->
    
    {Brokers, 
     <<
       NumberOfTopics:32/signed, 
       TopicMetadataRemainder/binary
     >>} = brokers(NumberOfBrokers, BrokerRemainder, []),

    {Topics, _} = topics(NumberOfTopics, TopicMetadataRemainder, []),
    {ok, #metadata{brokers = Brokers, topics = Topics}}.


topics(0, <<Remainder/binary>>, A) ->
    {A, Remainder};

topics(N, 
       <<
	 ErrorCode:16/signed, 
	 TopicNameLen:16/signed, 
	 TopicName:TopicNameLen/bytes, 
	 PartitionLength:32/signed, 
	 PartitionsRemainder/binary
       >>,
       A) ->

    {Partitions, Remainder} = partitions(PartitionLength, PartitionsRemainder, []),
    topics(N-1, 
	   Remainder, 
	   [#topic{error_code = ekc:error_code(ErrorCode), 
		   name = TopicName, 
		   partitions = Partitions} | A]).



-spec partitions(non_neg_integer(), binary(), list(ekc:partition())) -> {list(ekc:partition()), binary()}.

partitions(0, <<Remainder/binary>>, A) ->
    {A, Remainder};


partitions(N, 
	   <<
	     ErrorCode:16/signed, 
	     Id:32/signed, 
	     Leader:32/signed, 
	     NumberOfReplicas:32/signed, 
	     ReplicasRemainder/binary
	   >>, A) ->

    {Replicas, <<
		 NumberOfISR:32/signed, 
		 ISRRemainder/binary
	       >>} = replicas(NumberOfReplicas, ReplicasRemainder, []),
    {ISR, Remainder} = isrs(NumberOfISR, ISRRemainder, []),
    partitions(N-1, 
	       Remainder, 
	       [#partition{error_code = ekc:error_code(ErrorCode), 
			   id = Id, 
			   leader = Leader, 
			   replicas = Replicas, 
			   isr = ISR} | A]).


isrs(0, Remainder, A) ->
    {A, Remainder};
isrs(N, <<InSyncReplica:32/signed, Remainder/binary>>, A) ->
    isrs(N-1, Remainder, [InSyncReplica | A]).
    
    

-spec replicas(non_neg_integer(), binary(), list(integer())) -> {list(integer()), binary()}.

replicas(0, Remainder, A) ->
    {A, Remainder};
replicas(N, <<Replica:32/signed, Remainder/binary>>, A) ->
    replicas(N-1, Remainder, [Replica | A]).



-spec brokers(non_neg_integer(), binary(), list(ekc:broker())) -> {list(ekc:broker()), binary()}.

brokers(0, Remainder, A) ->
    {A, Remainder};

brokers(N, 
	<<
	  NodeId:32/signed, 
	  HostLength:16/signed, 
	  Host:HostLength/bytes, 
	  Port:32/signed, 
	  Remainder/binary
	>>, A) ->
    brokers(N-1, Remainder, [#broker{id = NodeId, host = Host, port = Port} | A]).


