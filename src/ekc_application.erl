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

-module(ekc_application).
-behaviour(application).

-export([start/0,
	 start/2,
	 stop/1]).



start(_Type, _Args) ->
	ekc_supervisor:start_link().

stop(_State) ->
	ok.


start() ->
    ok = ensure(ekc).

ensure(Application) ->    
    ensure_started(application:start(Application)).

ensure_started({error, {already_started, _}}) ->    
    ok;
ensure_started(ok) -> 
    ok.
