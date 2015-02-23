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

-module(ekc_partition).
-export([
	 error_code/1,
	 id/1,
	 leader/1,
	 high_water_mark/1,
	 replicas/1,
	 isr/1,
	 offsets/1,
	 message_sets/1
	]).

-include("ekc.hrl").


-spec error_code(ekc:partition()) -> atom().
error_code(#partition{error_code = ErrorCode}) ->
    ErrorCode.


-spec id(ekc:partition()) -> integer().
id(#partition{id = Id}) ->
    Id.


-spec leader(ekc:partition()) -> integer().
leader(#partition{leader = Leader}) ->
    Leader.


-spec high_water_mark(ekc:partition()) -> integer().
high_water_mark(#partition{high_water_mark = HighWaterMark}) ->
    HighWaterMark.


-spec replicas(ekc:partition()) -> list(integer()).
replicas(#partition{replicas = Replicas}) ->
    Replicas.

-spec isr(ekc:partition()) -> list(integer()).
isr(#partition{isr = ISR}) ->
    ISR.

-spec offsets(ekc:partition()) -> list(integer()).
offsets(#partition{offsets = Offsets}) ->
    Offsets.

-spec message_sets(ekc:partition()) -> list(ekc:message_set()).
message_sets(#partition{message_sets = MessageSets}) ->
    MessageSets.


