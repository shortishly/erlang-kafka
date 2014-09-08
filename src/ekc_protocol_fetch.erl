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

-module(ekc_protocol_fetch).
-include("ekc.hrl").

-export([request/5,
	 response/1]).


request(ReplicaId, MaxWaitTime, MinBytes, Topics, #state{} = S) ->
    ekc_protocol:request(?FETCH_REQUEST, 
			 <<
			   ReplicaId:32/signed, 
			   MaxWaitTime:32/signed, 
			   MinBytes:32/signed, 
			   (ekc_protocol:topics(Topics))/binary
			 >>, 
			 S).

-spec response(<<_:32,_:_*8>>) -> {ok, list(ekc:topic())}.

response(
  <<
    NumberOfTopics:32/signed, 
    Remainder/binary
  >>) ->
    {ok, response(NumberOfTopics, Remainder)}.


response(0, <<>>) ->
    [];

response(N, 
	 <<
	   TopicNameLength:16/signed, 
	   TopicName:TopicNameLength/bytes, 
	   NumberOfPartitions:32/signed, 
	   PartitionsRemainder/binary
	 >>) ->
    {Partitions, Remainder} = partitions(NumberOfPartitions, PartitionsRemainder, []),
    [#topic{name = TopicName, partitions = Partitions} | response(N-1, Remainder)].


partitions(0, Remainder, A) ->
    {A, Remainder};

partitions(N, 
	   <<
	     Partition:32/signed, 
	     ErrorCode:16/signed, 
	     HighWaterMark:64/signed, 
	     MessageSetSize:32/signed, 
	     MessageSet:MessageSetSize/bytes, 
	     Remainder/binary
	   >>, A) ->
    partitions(N-1, 
	       Remainder, 
	       [#partition{
		   id = Partition, 
		   error_code = ekc:error_code(ErrorCode), 
		   high_water_mark = HighWaterMark, 
		   message_sets = message_set(MessageSet)} | A]).




-spec message_set(binary()) -> list(ekc:message_set()).

message_set(<<>>) ->
    [];


message_set(
  <<
    Offset:64/signed, 
    MessageSize:32/signed, 
    Message:MessageSize/bytes, 
    Remainder/bytes
  >>) ->
    <<_CRC:32, MessageBody/binary>> = Message,
    message_set(Message, Offset, erlang:crc32(MessageBody), Remainder);

message_set(
  <<
    _Partial/bytes
  >>)  -> [].


message_set(<<
	      CRC:32, 
	      Magic:8/signed, 
	      Attributes:8/signed,
	      -1:32/signed, 
	      ValueSize:32/signed, 
	      Value:ValueSize/bytes
	    >>, Offset, CRC, Remainder) ->
    [#message_set{
	offset = Offset, 
	crc = CRC, 
	magic = Magic, 
	attributes = Attributes, 
	value = Value} | message_set(Remainder)];
message_set(_, _, _, <<>>) -> [].

    
