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

-module(ekc_supervisor).
-behaviour(supervisor).

-export([
	 start_link/0,
	 event_manager/1,
	 event_manager/2,
	 supervisor/1,
	 supervisor/2,
	 worker/1,
	 worker/2,
	 worker/3,
	 worker/4
	]).

-export([
	 init/1
	]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).



event_manager(Name) ->
    event_manager(Name, permanent).

event_manager(Name, Restart) ->
    {Name, {gen_event, start_link, [{local, Name}]}, Restart, 5000, worker, [gen_event]}.

supervisor(Module) ->
    supervisor(Module, permanent).

supervisor(Module, Restart) ->
    {Module, {Module, start_link, []}, Restart, 5000, supervisor, [Module]}.

worker(Module) ->
    worker(Module, permanent).

worker(Module, Restart) ->
    worker(Module, Restart, []).

worker(Module, Restart, Parameters) ->
    worker(Module, Module, Restart, Parameters).    

worker(Id, Module, Restart, Parameters) ->
    {Id, {Module, start_link, Parameters}, Restart, 5000, worker, [Module]}.



init([]) ->
	Procs = [supervisor(ekc_protocol_server_supervisor)],
	{ok, {{one_for_one, 1, 5}, Procs}}.
