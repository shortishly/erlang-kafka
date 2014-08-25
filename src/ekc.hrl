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

-define(PRODUCE_REQUEST, 0).
-define(FETCH_REQUEST, 1).
-define(OFFSET_REQUEST, 2).
-define(METADATA_REQUEST, 3).
-define(OFFSET_COMMIT_REQUEST, 8).
-define(OFFSET_FETCH_REQUEST, 9).
-define(CONSUMER_METADATA_REQUEST, 10).




-record(message_set, {offset :: integer(),
		      crc :: integer(),
		      magic :: integer(),
		      attributes :: integer(),
		      key :: binary(), 
		      value :: binary()}).

-record(broker, {id :: integer(), 
		 host :: binary(), 
		 port :: non_neg_integer()}).

-record(partition, {error_code, 
		    id :: integer(),
		    leader :: integer(), 
		    high_water_mark :: integer(), 
		    replicas = [] :: list(integer()), 
		    isr = [] :: list(integer()), 
		    offsets = [] :: list(integer()), 
		    message_sets = [] :: list(#message_set{})}).

-record(topic, {error_code,
		name :: binary(), 
		partitions = [] :: list(#partition{})}).

-record(metadata, {brokers = [] :: list(#broker{}),
		   topics = [] :: list(#topic{})}).



-record(state, {api_version = 0 :: integer(), 
		client_id = <<"ekc">> :: binary(), 
		correlation_id = 0 :: non_neg_integer(), 
		parts = <<>> :: binary(),
		host = localhost :: inet:hostname(), 
		port = 9092 :: inet:port_number(), 
		requests = orddict:new() :: orddict:orddict(), 
		socket :: inet:socket()}).

-record(request, {packet :: binary(),
		  state :: #state{}}).

-record(response, {from :: {pid(), term()},
		   handler}).

