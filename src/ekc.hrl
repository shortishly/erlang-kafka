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

-record(message_set, {
	  offset :: integer(),
	  crc :: integer(),
	  magic :: integer(),
	  attributes :: integer(),
	  key :: binary(), 
	  value :: binary()
	 }).

-record(broker, {
	  id :: integer(), 
	  host :: binary(), 
	  port :: non_neg_integer()
	 }).

-record(partition, {
	  error_code :: ekc:error_code(), 
	  id :: integer(),
	  leader :: integer(), 
	  high_water_mark :: integer(), 
	  replicas = [] :: list(integer()), 
	  isr = [] :: list(integer()), 
	  offsets = [] :: list(integer()), 
	  message_sets = [] :: list(ekc:message_set()),
	  metadata :: binary()
	 }).

-record(topic, {
	  error_code :: ekc:error_code(),
	  name :: binary(), 
	  partitions = [] :: list(ekc:partition())
	 }).

-record(metadata, {
	  brokers = [] :: list(ekc:broker()),
	  topics = [] :: list(ekc:topic())
	 }).

-record(consumer_metadata, {
	  error_code :: ekc:error_code(),
	  id :: pos_integer(),
	  host :: binary(),
	  port :: pos_integer()
	 }).
	  
