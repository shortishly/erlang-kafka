%% Copyright (c) 2013-2014 Peter Morgan <peter.james.morgan@gmail.com>
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

-module(metadata_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).


all() ->
    common:all().

groups() ->
    common:groups(?MODULE).

init_per_suite(Config) ->
    common:init_per_suite(Config).

protocol(Config) ->
    common:protocol(Config).


response_test(Config) ->
    {ok, Metadata} = ekc_protocol_metadata:response(protocol(Config)),
    [Broker] = ekc_metadata:brokers(Metadata),
    0 = ekc_broker:id(Broker),
    <<"dev001.localdomain">> = ekc_broker:host(Broker),
    9092 = ekc_broker:port(Broker).

