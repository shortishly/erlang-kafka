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

-module(ekc_protocol).
-include("ekc.hrl").

-export([request/3,
	topics/1]).

-export_type([handler/0]).


-type handler() :: fun((binary()) -> {ok, term()} | {error, term()}).



-spec request(?FETCH_REQUEST | 
	      ?OFFSET_REQUEST | 
	      ?METADATA_REQUEST | 
	      ?CONSUMER_METADATA_REQUEST, <<_:8,_:_*8>>, #state{}) -> #request{}.

request(ApiKey, 
	RequestMessage, 
	#state{
	   api_version = ApiVersion, 
	   correlation_id = CorrelationId, 
	   client_id = ClientId} = S) ->

    #request{packet = frame(<<
			      ApiKey:16/signed, 
			      ApiVersion:16/signed, 
			      CorrelationId:32/signed, 
			      (byte_size(ClientId)):16/signed,
			      ClientId/bytes, 
			      RequestMessage/binary
			    >>),
	     state = S#state{correlation_id = CorrelationId+1}}.




-spec frame(<<_:8,_:_*8>>) -> <<_:32,_:_*8>>.
frame(Message) ->
    <<
      (byte_size(Message)):32/signed, 
      Message/binary
    >>.


-spec topics(list(binary() | {binary(), {non_neg_integer(), non_neg_integer(), pos_integer()}})) -> binary().


topics(Topics) ->
    topics(Topics, 
	   <<
	     (length(Topics)):32/signed
	   >>).



topics([], A) ->
    A;

topics([{TopicName, Partitions} | T], A) ->
    topics(T,
	   <<
	     A/binary, 
	     (byte_size(TopicName)):16/signed, 
	     TopicName/bytes, 
	     (length(Partitions)):32/signed, 
	     (list_to_binary([
			      <<
				Partition:32/signed, 
				FetchOffset:64/signed, 
				MaxBytes:32/signed
			      >> || {Partition, FetchOffset, MaxBytes} <- Partitions]))/binary
	   >>);

topics([TopicName | T], A) ->
   topics([{TopicName, []} | T], A).
    
