%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc mnesia based cache application.
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache_app).

-behaviour(application).


%% Application callbacks
-export([start/2, stop/1]).

-include("defaults.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

-spec start(application:start_type(), [term()]) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
    app_cache_sup:start_link().

stop(_State) ->
    ok.
