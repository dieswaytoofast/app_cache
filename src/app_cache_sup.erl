%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc The cache processor 
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache_sup).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = supervisor,

    Mods = [app_cache_processor_sup,
            app_cache_scavenger_sup,
            app_cache_refresher_sup,
            app_cache_sequence_cache_sup],

    %% Initialize the tables used for the Mnesia cache.
    %% TODO: Some kind of mnesia timing thing
    Supervisors =
        [{Mod, {Mod, start_link, []}, Restart, Shutdown, Type, [Mod]} || Mod <- Mods],

    {ok, {SupFlags, Supervisors}}.
