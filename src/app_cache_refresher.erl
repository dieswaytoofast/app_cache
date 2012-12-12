%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Refreshes individual entries in tables
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache_refresher).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').


-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         refresh_data/3,
         remove_function/1,
         reset_function/1]).

-export([apply_refresh_function/3]).

-export([remove_key/2,
         clear_table/1]).

%% used to initialize the refresher table
-export([reset_cache/0]).

%% testing
-export([double_value/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% ------------------------------------------------------------------
%% Includes & Defines
%% ------------------------------------------------------------------

-include_lib("stdlib/include/qlc.hrl").
-include("defaults.hrl").

-define(SERVER, ?MODULE).

-record(state, {
            functions = dict:new()
         }).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%%
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec refresh_data(async | sync, table(), [table_key()]) -> ok | error().
refresh_data(async, Table, KeyList) ->
    gen_server:cast(?SERVER, {refresh_data, Table, KeyList});
refresh_data(sync, Table, KeyList) ->
    gen_server:call(?SERVER, {refresh_data, Table, KeyList}).

-spec remove_function(table()) -> ok.
remove_function(Table) ->
    gen_server:cast(?SERVER, {remove_function, Table}).

-spec reset_function(table()) -> ok.
reset_function(Table) ->
    gen_server:cast(?SERVER, {reset_function, Table}).


%% @doc remove the entry for this key
-spec remove_key(table(), table_key()) -> ok | error().
remove_key(Table, Key) ->
    gen_server:call(?SERVER, {remove_key, Table, Key}).

%% @doc remove all the entries for this table
-spec clear_table(table()) -> ok.
clear_table(Table) ->
    gen_server:cast(?SERVER, {clear_table, Table}).



%% @doc Reset the cache
-spec reset_cache() -> ok.
reset_cache() ->
    gen_server:cast(?SERVER, {reset_cache}).


%% @doc Testing
double_value(Key) ->
    case mnesia:dirty_read(test_table_1, Key) of
        [#test_table_1{value = X} = Entry] ->
            Entry#test_table_1{value = 2*X};
        _ ->
            #test_table_1{key = Key, value = 1}
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @private
%% @doc Initializes the refresh functions for all the tables
-spec init([]) -> {ok, #state{}}.
init([]) ->
    reset_cache(),
    {ok, #state{}}.

handle_call({refresh_data, Table, KeyList}, _From, State) ->
    case dict:find(Table, State#state.functions) of
        {ok, RefreshData} ->
            ok = update_refresh_table(sync, RefreshData, Table, KeyList);
        error ->
            Error = {{error, {invalid_table, Table}}, State#state.functions},
            lager:error("Error:~p~n", [Error])
    end,
    {reply, ok, State};

handle_call({remove_key, Table, Key}, _From, #state{functions = Functions} = State) ->
    Response =
    case dict:is_key(Table, Functions) of
        true ->
            ok = remove_entry_from_table(Table, Key);
        false ->
            ok
    end,
    {reply, Response, State}.

handle_cast({clear_table, Table}, #state{functions = Functions} = State) ->
    case dict:is_key(Table, Functions) of
        true ->
            remove_all_table_entries(Table);
        false ->
            ok
    end,
    {noreply, State};

handle_cast({remove_function, Table}, #state{functions = Functions} = State) ->
    NewFunctions =
    case dict:find(Table, Functions) of
        {ok, _RefreshData} ->
            dict:erase(Table, Functions);
        error ->
            Functions
    end,
    remove_all_table_entries(Table),
    {noreply, State#state{functions = NewFunctions}};

handle_cast({refresh_data, Table, KeyList}, State) ->
    case dict:find(Table, State#state.functions) of
        {ok, RefreshData} ->
            ok = update_refresh_table(async, RefreshData, Table, KeyList);
        error ->
            Error = {{error, {invalid_table, Table}}, State#state.functions},
            lager:error("Error:~p~n", [Error])
    end,
    {noreply, State};

handle_cast({reset_cache}, _State) ->
    Functions = reset_cache_internal(),
    ok = reset_table_entries(Functions),
    {noreply, #state{functions = Functions}};

handle_cast({reset_function, Table}, #state{functions = Functions} = State) ->
    {_Response, Functions1} = case mnesia:transaction(fun() -> mnesia:read(?METATABLE, Table) end) of
        {atomic, [#app_metatable{refresh_function = RefreshData}]} ->
            update_function_dict(Table, RefreshData, Functions);
        _ ->
            remove_all_table_entries(Table),
            {{error, {invalid_table, Table}}, Functions}
    end,
    {noreply, State#state{functions = Functions1}}.

%% @doc We use this to capture refresh_functon requests when the parameter is an
%%      anonymous fun
handle_info({apply_refresh_function, FunctionIdentifier, Table, Key}, State) ->
    apply_refresh_function(FunctionIdentifier, Table, Key),
    {noreply, State};

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================
%% @doc return a List of all the applicable functions
-spec reset_cache_internal() -> any().
reset_cache_internal() ->
    app_cache:cache_init([?REFRESH_TABLE_DEF]),
    %% Retrieve the list of cache tables and create a timer for each of them
    %% using the table's time-to-live as the timer interval.
    {atomic, Metatables} = mnesia:transaction(fun() -> mnesia:match_object( #app_metatable{_ = '_'}) end ),
    lists:foldl(fun
            (#app_metatable{table = _Table, refresh_function = #refresh_data{function_identifier = undefined}}, Acc) ->
                Acc;
            (#app_metatable{table = Table, refresh_function = RefreshData}, Acc) ->
                dict:store(Table, RefreshData, Acc)
        end, dict:new(), Metatables).


%% @doc Add the refresh_function to the known set (if necessary)
-spec update_function_dict(table(), #refresh_data{}, any()) -> {ok, any()}.
update_function_dict(Table, #refresh_data{function_identifier = undefined}, Functions) ->
    remove_all_table_entries(Table),
    {ok, Functions};
update_function_dict(Table, RefreshData, Functions) ->
    remove_all_table_entries(Table),
    store_function_dict(Table, RefreshData, Functions).

store_function_dict(Table, RefreshData, Functions) ->
    case dict:is_key(Table, Functions) of
        true ->
            update_if_different(Table, RefreshData, Functions);
        false ->
            {ok, dict:store(Table, RefreshData, Functions)}
    end.

update_if_different(Table, #refresh_data{refresh_interval = RefreshInterval} = RefreshData, Functions) ->
    #refresh_data{refresh_interval = OldRefreshInterval} = OldRefreshData = dict:fetch(Table, Functions),
    if OldRefreshData =:= RefreshData ->
            {ok, Functions};
        OldRefreshInterval =:= RefreshInterval ->
            NewFunctions = dict:store(Table, RefreshData, Functions),
            {ok, NewFunctions};
        true ->
            NewFunctions = dict:store(Table, RefreshData, Functions),
            % TODO should update the timer functions on a case by case basis
            remove_all_table_entries(Table),
            {ok, NewFunctions}
    end.

%% @doc delete all entries that are not in the known list of functions
remove_all_table_entries(Table) ->
    MatchHead = #refresh_table{key = {Table, '$1'}, timer = '$2', _ = '_'},
    Guard =  [],
    Result = [{{'$1', '$2'}}],
    DeleteFun = fun() ->
            KeyList = mnesia:select(?REFRESH_TABLE, [{MatchHead, Guard, Result}]),
            lists:foreach(fun({Key, Timer}) ->
                        timer2:cancel(Timer),
                        % Remove the refresh_table entry
                        mnesia:delete({?REFRESH_TABLE, {Table, Key}})
                end, KeyList)
    end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            ok;
        Error ->
            {error, Error}
    end.

%% @doc remove all the entries that do not have refresh functions
-spec reset_table_entries(function_dict()) ->  ok | error().
reset_table_entries(Functions) ->
    case get_unique_tables() of
        {error, _} = Error ->
            Error;
        TableList ->
            InvalidTables = get_invalid_tables(TableList, Functions),
            [remove_all_table_entries(Table) || Table <- InvalidTables],
            ok
    end.

%% @doc Get the tables that are do not have refresh functions
-spec get_invalid_tables([table()], function_dict()) -> [table()].
get_invalid_tables(TableList, Functions) ->
    lists:filter(fun(Table) ->
                dict:is_key(Table, Functions) end, TableList).

%% @doc Get all the unique tables in the set of keys that need to be refreshed
-spec get_unique_tables() -> [table()] | error().
get_unique_tables() ->
    QH = qlc:q([Table || #refresh_table{key = {Table, _}} <-
                mnesia:table(?REFRESH_TABLE)], {unique, true}),
    TableFun = fun() -> qlc:eval(QH) end,
    case mnesia:transaction(TableFun) of
        {atomic, TableList} ->
            TableList;
        Error ->
            {error, Error}
    end.

-spec update_refresh_table(sync | async, #refresh_data{} | undefined, table(), [table_key()]) -> ok.
update_refresh_table(_, undefined, _Table, _KeyList) ->
    void;
update_refresh_table(Type, RefreshData, Table, KeyList) ->
    lists:foreach(fun(Key) ->
                update_key_in_refresh_table(RefreshData, Table, Key),
                if Type =:= sync ->
                        apply_refresh_function(RefreshData, Table, Key);
                    true ->
                        ok
                end
        end, KeyList).

%% @doc If the key doesnt exist in the refresh table, add it
-spec update_key_in_refresh_table(#refresh_data{} | undefined, table(), table_key()) -> ok | void.
update_key_in_refresh_table(undefined, _Table, _Key) ->
    void;
update_key_in_refresh_table(RefreshData, Table, Key) ->
    % seconds!
    TTL = get_ttl_in_seconds(RefreshData#refresh_data.refresh_interval),
    FunctionIdentifier = RefreshData#refresh_data.function_identifier,
    UpdateFun = fun() ->
            case mnesia:read(?REFRESH_TABLE, {Table, Key}) of
                [] ->
                    {ok, TimerRef} = get_timer_for_data(TTL, FunctionIdentifier, Table, Key),
                    mnesia:write(#refresh_table{
                            key = {Table, Key},
                            time_to_live = TTL,
                            last_update = app_cache:current_time_in_gregorian_seconds(),
                            timer = TimerRef});
                _ ->
                    void
            end
    end,
    {atomic, Res} = mnesia:transaction(UpdateFun),
    Res.

%% @doc Return the TTL in seconds (or infinity!)
-spec get_ttl_in_seconds(time_to_live()) -> time_to_live().
get_ttl_in_seconds(?INFINITY) ->
    ?INFINITY;
get_ttl_in_seconds(TTL) ->
    TTL*1000.

%% @doc Return a timer that'll run the MF every TTL milliseconds
-spec get_timer_for_data(time_to_live(), function_identifier(), table(), table_key()) -> {ok, any()}.
get_timer_for_data(?INFINITY, _FunctionIdentifier, _Table, _Key) ->
    {ok, undefined};
get_timer_for_data(TTL, {module_and_function, {_Module, _Function}} = FunctionIdentifier, Table, Key) ->
    timer2:apply_interval(TTL, ?SERVER, apply_refresh_function, [FunctionIdentifier, Table, Key]);
get_timer_for_data(TTL, {function, _Function} = FunctionIdentifier, Table, Key) ->
    timer2:send_interval(TTL, {apply_refresh_function, FunctionIdentifier, Table, Key});
get_timer_for_data(_TTL, _Other, _Table, _Key) ->
    {ok, undefined}.

%% @doc Run the refresh function on the key in the table
-spec apply_refresh_function(#refresh_data{} | function_identifier() | undefined,
                             table(), table_key()) -> ok | error().
apply_refresh_function(undefined, _Table, _Key) ->
    void;
apply_refresh_function(RefreshData, _Table, Key) when is_record(RefreshData, refresh_data) ->
    FunctionIdentifier = RefreshData#refresh_data.function_identifier,
    apply_refresh_function(FunctionIdentifier, _Table, Key);
apply_refresh_function({module_and_function, {Module, Function}}, _Table, Key) ->
    Data = erlang:apply(Module, Function, [Key]),
    app_cache:set_data(Data);
apply_refresh_function({function, Function}, _Table, Key) ->
    Data = Function(Key),
    app_cache:set_data(Data);
apply_refresh_function(_Other, _Table, _Key) ->
    void.

%% @doc cancel the timer and remove the key
remove_entry_from_table(Table, Key) ->
    DeleteFun = fun() ->case mnesia:read(?REFRESH_TABLE, {Table, Key}) of
                [#refresh_table{timer = TRef}] ->
                    timer2:cancel(TRef),
                    mnesia:delete({?REFRESH_TABLE, {Table, Key}}),
                    % Necessary to make sure that we didn't repopulate the table
                    %       because a timer fired
                    mnesia:delete({Table, Key});
                _ ->
                    ok
            end
    end,
    case mnesia:transaction(DeleteFun) of
        {atomic, _} ->
            ok;
        {aborted, _} = Error ->
            lager:debug("error:~p~n", [Error]),
            {error, Error}
    end.
