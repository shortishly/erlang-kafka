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

-module(ekc_protocol_consumer_metadata).
-include("ekc.hrl").

-export([request/2,
	 response/1]).

request(ConsumerGroup, S) ->
    ekc:request(?CONSUMER_METADATA_REQUEST, 
		<<
		  (byte_size(ConsumerGroup)):16/signed, 
		  ConsumerGroup/binary
		>>, S).

response(
  <<
    ErrorCode:16/signed, 
    Id:32/signed, 
    HostLength:16/signed, 
    Host:HostLength/bytes, 
    Port:32/signed
  >>) ->
    {ok, {ekc:error_code(ErrorCode), Id, Host, Port}}.

