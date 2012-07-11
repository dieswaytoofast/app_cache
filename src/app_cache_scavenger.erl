%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Scavenger process that periodically removes stale entries from the
%%%      app_cache
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache_scavenger).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').


-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, scavenge/1, reset_timer/1, expired_entries/1,
         get_timers/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% ------------------------------------------------------------------
%% Includes & Defines
%% ------------------------------------------------------------------

-include("app_cache.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          timers = []
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%%
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-spec scavenge(table()) -> ok.
scavenge(Table) ->
    gen_server:cast(?SERVER, {scavenge, Table}).

get_timers() ->
    gen_server:call(?SERVER, {get_timers}).

-spec reset_timer(table()) -> ok | error().
reset_timer(Table) ->
    gen_server:call(?SERVER, {reset_timer, Table}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @private
%% @doc Initializes the server registering timers to scavenge the cache Mnesia
%%      tables according to each table's expiration time.
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Retrieve the list of cache tables and create a timer for each of them
    %% using the table's time-to-live as the timer interval.
    {atomic, Metatables} = mnesia:transaction(fun() -> mnesia:match_object( #app_metatable{_ = '_'}) end ),
    Timers = lists:foldl(fun(#app_metatable{table = Table, time_to_live = TimeToLive}, Acc) ->
                    get_new_timer(Table, TimeToLive) ++ Acc
            end, [], Metatables),
    {ok, #state{timers = Timers}}.


handle_call({reset_timer, Table}, _From, #state{timers = Timers} = _State) ->
    {Response, FinalTimers} = case mnesia:transaction(fun() -> mnesia:read(?METATABLE, Table) end) of
        {atomic, [#app_metatable{time_to_live = TimeToLive}]} ->
            {ok, update_timers(Table, TimeToLive, Timers)};
        [] ->
            {{error, {invalid_table, Table}}, Timers}
    end, 
    lager:debug("Timers:~p~n", [FinalTimers]),
    {reply, Response, #state{timers = FinalTimers}};

handle_call({get_timers}, _From, #state{timers = Timers} = State) ->
    {reply, Timers, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


handle_cast({scavenge, Table}, State) ->
    lists:foreach(fun (Entry) -> mnesia:dirty_delete_object(Entry) end, expired_entries(Table)),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec expired_entries(table()) -> [term()].
expired_entries(Table) ->
    Now = app_cache:current_time_in_gregorian_seconds(),
    {TimeToLive, TimestampPosition} = app_cache:get_ttl_and_field_index(Table),
    MatchHead = '$1',
    Guard = [{'<', {element, TimestampPosition + 1, '$1'}, Now - TimeToLive}],
    Result = ['$1'],
    % Dirty is ok, since this is just garbage-collecting stale data
    mnesia:dirty_select(Table, [{MatchHead, Guard, Result}]).

%% @doc Deletes and recreates the timer for a given table in the timers list
-spec update_timers(table(), time_to_live(), list()) -> list().
update_timers(Table, TimeToLive, Timers) ->
    Timers1 = cancel_old_timer(Table, Timers),
    NewTimers =  get_new_timer(Table, TimeToLive),
    NewTimers ++ Timers1.

%% @doc Removes a timer entry from the list of timers
-spec cancel_old_timer(table(), list()) -> list().
cancel_old_timer(Table, Timers) ->
    case lists:keytake(Table, 1, Timers) of
        {value, {Table, _TimeToLive, TimerRef}, NewTimers} ->
            timer:cancel(TimerRef),
            NewTimers;
        false ->
            Timers
    end.

%% @doc Create timers to activate the scavenger for the table
-spec get_new_timer(Table::table(), TimeToLive::time_to_live()) -> any().
get_new_timer(Table, TimeToLive) ->
    case TimeToLive of
        ?INFINITY ->
            [];
        _ ->
            {ok, TimerRef} = timer:apply_interval(TimeToLive * ?SCAVENGE_FACTOR, ?SERVER, scavenge, [Table]),
            [{Table, TimeToLive, TimerRef}]
    end.

