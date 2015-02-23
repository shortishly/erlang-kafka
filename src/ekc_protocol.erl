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

-module(ekc_protocol).
-include("ekc.hrl").
-include("ekc_protocol.hrl").

-export([
	 request/3,
	 error_code/1,
	 encode/3,
	 encode/2
	]).

-export_type([
	      handler/0
	     ]).


-type handler() :: fun((binary()) -> {ok, term()} | {error, term()}).


-spec request(?FETCH_REQUEST | 
	      ?OFFSET_REQUEST | 
	      ?METADATA_REQUEST | 
	      ?CONSUMER_METADATA_REQUEST, <<_:8,_:_*8>>, #protocol_state{}) -> #request{}.

request(ApiKey, 
	RequestMessage, 
	#protocol_state{
	   api_version = ApiVersion, 
	   correlation_id = CorrelationId, 
	   client_id = ClientId} = S) ->

    #request{packet = frame(<<
			      (encode(integer, ApiKey, 16))/binary,
			      (encode(integer, ApiVersion, 16))/binary,
			      (encode(integer, CorrelationId, 32))/binary,
			      (encode(string, ClientId))/binary,
			      RequestMessage/binary
			    >>),
	     state = S#protocol_state{correlation_id = CorrelationId+1}}.




-spec frame(<<_:8,_:_*8>>) -> <<_:32,_:_*8>>.
frame(Message) ->
    <<
      (byte_size(Message)):32/signed, 
      Message/binary
    >>.

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


encode(integer, Value, Precision) when Precision == 8 orelse Precision == 16 orelse Precision == 32 orelse Precision == 64 ->
    <<Value:Precision/signed>>.

encode(string, <<>>) ->
    encode(integer, -1, 16);

encode(string, Content) ->
    <<
      (encode(integer, byte_size(Content), 16))/binary,
      Content/bytes
    >>;

encode(bytes, <<>>) ->
    encode(integer, -1, 32);
encode(bytes, Content) ->
    <<
      (encode(integer, byte_size(Content), 32))/binary,
      Content/bytes
    >>;

encode(array, Array) ->
    <<
      (encode(integer, length(Array), 32))/binary,
      (list_to_binary(Array))/binary
    >>;

encode(topics, Topics) ->
    encode(array, [encode(topic, {Topic, Partitions}) || {Topic, Partitions} <- Topics]);

encode(topic, {Topic, Partitions}) ->
    <<
      (encode(string, Topic))/binary,
      (encode(partitions, Partitions))/binary
    >>;

encode(partitions, Partitions) ->
    encode(array, [encode(partition, Partition) || Partition <- Partitions]);

encode(partition, {Partition, FetchOffset, MaxBytes}) ->
    <<
      (encode(integer, Partition, 32))/binary,
      (encode(integer, FetchOffset, 64))/binary,
      (encode(integer, MaxBytes, 32))/binary
    >>;
encode(partition, {Partition, Offset, TimeStamp, Metadata}) ->
    <<
      (encode(integer, Partition, 32))/binary,
      (encode(integer, Offset, 64))/binary,
      (encode(integer, TimeStamp, 64))/binary,
      (encode(string, Metadata))/binary
    >>;
encode(partition, Partition) when is_integer(Partition) ->
    encode(integer, Partition, 32).





