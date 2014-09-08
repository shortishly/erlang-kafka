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

-module(fetch_SUITE).
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
    {ok, [Topic]} = ekc_protocol_fetch:response(protocol(Config)),
    <<"test">> = ekc_topic:name(Topic),
    [Partition] = ekc_topic:partitions(Topic),
    no_error = ekc_partition:error_code(Partition),
    0 = ekc_partition:id(Partition),
    124787 = ekc_partition:high_water_mark(Partition),
    38 = length(ekc_partition:message_sets(Partition)),
    [M0, M1, M2, M3 | _] = ekc_partition:message_sets(Partition),
    <<_:3/bytes, "The Project Gutenberg EBook of The Complete Works of William Shakespeare, by">> = ekc_message_set:value(M0),
    <<"William Shakespeare">> = ekc_message_set:value(M1),
    <<"">> = ekc_message_set:value(M2),
    <<"This eBook is for the use of anyone anywhere at no cost and with">> = ekc_message_set:value(M3).
    
