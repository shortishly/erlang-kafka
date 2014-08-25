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

-module(ekc_broker).

-export([id/1,
	 host/1,
	 port/1]).

-include("ekc.hrl").

-spec id(ekc:broker()) -> integer().
id(#broker{id = Id}) ->
    Id.


-spec host(ekc:broker()) -> binary().
host(#broker{host = Host}) ->
    Host.


-spec port(ekc:broker()) -> non_neg_integer().
port(#broker{port = Port}) ->
    Port.

