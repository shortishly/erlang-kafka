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

-module(ekc_message_set).
-export([
	 offset/1,
	 crc/1,
	 magic/1,
	 attributes/1,
	 key/1,
	 value/1
	]).

-include("ekc.hrl").


-spec offset(ekc:message_set()) -> integer().
offset(#message_set{offset = Offset}) ->
    Offset.

-spec crc(ekc:message_set()) -> integer().
crc(#message_set{crc = CRC}) ->
    CRC.

-spec magic(ekc:message_set()) -> integer().
magic(#message_set{magic = Magic}) ->
    Magic.

-spec attributes(ekc:message_set()) -> integer().
attributes(#message_set{attributes = Attributes}) ->
    Attributes.

-spec key(ekc:message_set()) -> binary().
key(#message_set{key = Key}) ->
    Key.

-spec value(ekc:message_set()) -> binary().
value(#message_set{value = Value}) ->
    Value.
