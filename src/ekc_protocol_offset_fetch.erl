%% Copyright (c) 2015 Peter Morgan <peter.james.morgan@gmail.com>
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

-module(ekc_protocol_offset_fetch).
-include("ekc.hrl").
-include("ekc_protocol.hrl").

-export([
	 request/3,
	 response/1
	]).


request(ConsumerGroup, Topics, #protocol_state{} = S) ->
    ekc_protocol:request(?OFFSET_FETCH_REQUEST,
			 <<
			   (ekc_protocol:encode(string, ConsumerGroup))/binary,
			   (ekc_protocol:encode(topics, Topics))/binary
			 >>,
			 S).

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
	    Offset:64/signed,
	    MetadataLength:16/signed,
	    Metadata:MetadataLength/bytes,
	    ErrorCode:16/signed,
	    Remainder/binary
	  >>,
	  A) ->
    partitions(N-1,
	       Remainder,
	       [#partition {
		   id = Partition,
		   offsets = [Offset],
		   metadata = Metadata,
		   error_code = ekc_protocol:error_code(ErrorCode)
		  } | A]).


