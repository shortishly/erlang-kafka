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

-module(ekc_topic).
-export([
	 error_code/1,
	 name/1,
	 partitions/1
	]).

-include("ekc.hrl").


-spec error_code(ekc:topic()) -> atom().
error_code(#topic{error_code = ErrorCode}) ->
    ErrorCode.


-spec name(ekc:topic()) -> binary().
name(#topic{name = Name}) ->
    Name.

-spec partitions(ekc:topic()) -> list(ekc:partition()).
partitions(#topic{partitions = Partitions}) ->
    Partitions.
