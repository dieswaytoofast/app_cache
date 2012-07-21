%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Cache sequences in the gen_server
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache_sequence_cache).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').


-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% used to initialize the sequence cache w/ a Dict
-export([reset_cache/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% ------------------------------------------------------------------
%% Includes & Defines
%% ------------------------------------------------------------------

-include("defaults.hrl").

-define(SERVER, ?MODULE).

-record(state, {
            sequences = dict:new()                                     :: any()
            }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%%
start_link() ->
    gen_server:start_link({local, ?SEQUENCE_CACHE}, ?MODULE, [], []).

%% @doc Reset the cache from the sequence table
-spec reset_cache() -> ok.
reset_cache() ->
    gen_server:cast(?SERVER, {reset_cache}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @private
%% @doc Initializes the server registering timers to scavenge the cache Mnesia
%%      tables according to each table's expiration time.
-spec init([]) -> {ok, #state{}}.
init([]) ->
    app_cache:cache_init([?SEQUENCE_TABLE_DEF]),
    reset_cache(),
    {ok, #state{}}.


handle_call({create, Key, Start, UpperBoundIncrement}, _From, State) ->
    NewDict = initialize_cache(Key, Start, Start, UpperBoundIncrement, State),
    {reply, ok, State#state{sequences = NewDict}};

handle_call({next_value, Key, Increment}, _From, State) ->
    Dict = State#state.sequences,
    Sequence = get_sequence(Key, Dict),
    UpperBound = Sequence#sequence_cache.upper_bound, 
    NewCachedValue = Sequence#sequence_cache.cached_value + Increment, 
    Sequence1 = Sequence#sequence_cache{cached_value = NewCachedValue},
    Sequence2 = if NewCachedValue >= UpperBound ->
            update_upper_bound(Sequence1);
        true ->
            Sequence1
    end,
    NewDict = dict:store(Key, Sequence2, Dict),
    {reply, NewCachedValue,State#state{sequences = NewDict}};

handle_call({current_value, Key}, _From, State) ->
    Dict = State#state.sequences,
    {CachedValue, NewDict} = 
    case dict:find(Key, Dict) of
        {ok, Sequence} ->
            {Sequence#sequence_cache.cached_value, Dict};
        error ->
            Dict1 = initialize_cache(Key, ?DEFAULT_CACHE_START, State),
            {?DEFAULT_CACHE_START, Dict1}
    end,
    {reply, CachedValue, State#state{sequences = NewDict}};

handle_call({set_value, Key, Value}, _From, State) ->
    NewDict = initialize_cache(Key, Value, State),
    {reply, ok, State#state{sequences = NewDict}};


handle_call({delete, Key}, _From, State) ->
    Dict = State#state.sequences,
    app_cache:remove_data(?SEQUENCE_TABLE, Key),
    NewDict = dict:erase(Key, Dict),
    {reply, ok, State#state{sequences = NewDict}};

handle_call({all_sequences}, _From, State) ->
    Dict = State#state.sequences,
    {reply, dict:to_list(Dict), State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({reset_cache, Dict}, _State) ->
    reset_cache_internal(),
    {noreply, #state{sequences = Dict}};

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

update_upper_bound(Sequence) ->
    Key = Sequence#sequence_cache.key, 
    CachedValue = Sequence#sequence_cache.cached_value,
    UpperBoundIncrement = Sequence#sequence_cache.upper_bound_increment, 
    NewUpperBound = app_cache:sequence_next_value(Key, UpperBoundIncrement),
    %% Validate against ext. manip of the sequence
    % sequence was incremeneted past the old value
    if NewUpperBound =< CachedValue ->
            FinalUpperBound = CachedValue + UpperBoundIncrement,
            app_cache:sequence_init(Key, FinalUpperBound),
            Sequence#sequence_cache{cached_value = CachedValue,
                                  upper_bound = FinalUpperBound};
        true ->
            Sequence#sequence_cache{cached_value = CachedValue,
                                  upper_bound = NewUpperBound}
    end.
    

get_sequence(Key, Dict) ->
    case dict:find(Key, Dict) of
        {ok, Sequence} ->
            Sequence;
        error ->
            #sequence_cache{key = Key}
    end.

-spec initialize_cache(sequence_key(), sequence_value(), #state{}) -> any().
initialize_cache(Key, Value, State) ->
    Dict = State#state.sequences,
    Sequence = get_sequence(Key, Dict),
    Start = Sequence#sequence_cache.start,
    UpperBoundIncrement = Sequence#sequence_cache.upper_bound_increment,
    initialize_cache(Key, Value, Start, UpperBoundIncrement, State). 

-spec initialize_cache(sequence_key(), sequence_value(), sequence_value(), sequence_value(), #state{}) -> any().
initialize_cache(Key, Value, Start, UpperBoundIncrement, State) ->
    Dict = State#state.sequences,
    app_cache:set_data(#sequence_table{key = Key,
                                 value = Value}),
    app_cache:sequence_next_value(Key, UpperBoundIncrement),
    dict:store(Key, #sequence_cache{key = Key,
                                    start = Start,
                                    upper_bound_increment = UpperBoundIncrement,
                                    cached_value = Value,
                                    upper_bound = Value}, Dict).
        
%% @doc return a Dict with all the data in the sequence table
-spec reset_cache_internal() -> any().
reset_cache_internal() ->
    lists:foldl(fun(#sequence_table{key = Key, value = Value}, _Acc) ->
                    UpperBoundIncrement = ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT,
                    app_cache:sequence_next_value(Key, UpperBoundIncrement),
                    dict:store(Key, #sequence_cache{key = Key,
                                                    cached_value = Value,
                                                    upper_bound = Value}, dict:new())
            end, dict:new(), app_cache:get_all_data(?SEQUENCE_TABLE)).
