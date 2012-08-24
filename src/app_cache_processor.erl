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
-module(app_cache_processor).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').

%-compile([{parse_transform, dynarec}]).
-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([get_ttl_and_field_index/1]).

%% Mnesia utility APIs
-export([create_metatable/1]).
-export([upgrade_metatable/0, upgrade_table/1, upgrade_table/2, upgrade_table/4]).
-export([check_key_exists/3]).
-export([read_data/3]).
-export([read_data_from_index/4]).
-export([read_data_by_last_key/2]).
-export([read_after/3]).
-export([read_records/2]).
-export([read_all_data/2]).
-export([read_last_n_entries/3]).
-export([read_first_n_entries/3]).
-export([write_data/2]).
-export([write_data_overwriting_timestamp/2]).
-export([delete_data/3]).
-export([delete_all_data/2]).
-export([delete_record/3]).
-export([increment_data/3, increment_data/4]).

-export([table_fields/1]).
-export([table_info/1]).


-export([get_functions/1]).

% For testing
-export([double_test_table_1_value/1]).


%% gen_server APIs
-export([start_link/0, start_link/1]).

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
            tables = []         :: [#app_metatable{}]
            }).



%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link() ->
    Nodes = app_cache:get_env(cache_nodes, [node()]),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Nodes], []).
    
start_link(Nodes) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Nodes], []).

-spec get_ttl_and_field_index(#app_metatable{}) -> {timestamp(), table_key_position()}.
get_ttl_and_field_index(TableInfo) ->
    get_ttl_and_field_index_internal(TableInfo).

% TODO make this dependant on the node
-spec upgrade_metatable() -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_metatable() ->
    upgrade_table(?METATABLE, app_cache:get_record_fields(?METATABLE)).

-spec upgrade_table(table()) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table) ->
    Fields = app_cache:table_fields(Table),
    upgrade_table(Table, Fields).


-spec upgrade_table(table(), [app_field()]) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table, Fields) ->
    %% Replace 'ignore' with a function that performs the schema upgrade once the schema changes.
    mnesia:transform_table(Table, ignore, Fields, Table).

-spec upgrade_table(table(), OldVersion :: non_neg_integer(), NewVersion :: non_neg_integer(), [app_field()]) ->
                           {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table, _OldVersion, _NewVersion, Fields) ->
    %% Replace 'ignore' with a function that performs the schema upgrade once the schema changes.
    mnesia:transform_table(Table, ignore, Fields, Table).

-spec get_functions(table()) -> #data_functions{} | undefined.
get_functions(Table) ->
    gen_server:call(?SERVER, {get_functiosn, Table}).

-spec table_fields(table()) -> table_fields().
table_fields(Table) ->
    gen_server:call(?SERVER, {table_fields, Table}).

-spec table_info(table()) -> #app_metatable{} | undefined.
table_info(Table) ->
    gen_server:call(?SERVER, {table_info, Table}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes]) ->
    process_flag(trap_exit, true),
    try
        init_metatable_internal(Nodes),
        Tables = load_metatable_internal(),
        {ok, #state{tables = Tables}}
    catch
        _:Error ->
            lager:error("Error ~p initializing mnesia.  Did you forget to run ~p:setup()?~n", [Error, ?SERVER]),
            {stop, Error}
    end.

handle_call({cache_init, Nodes, InTables}, _From, State) ->
    Response = cache_init_internal(Nodes, InTables),
    reset_helpers(InTables),
    Tables = load_metatable_internal(),
    {reply, Response, State#state{tables = Tables}};

handle_call({init_metatable, Nodes}, _From, State) ->
    Response = init_metatable_internal(Nodes),
    {reply, Response, State};

handle_call({get_metatable}, _From, State) ->
    Response = State#state.tables,
    {reply, Response, State};

handle_call({init_table, Table, Nodes}, _From, State) ->
    Response = init_table_internal(Table, Nodes, State#state.tables),
    Tables = load_metatable_internal(),
    reset_helpers(table_info(Table, Tables)),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_tables, Nodes}, _From, State) ->
    Response = create_tables_internal(Nodes, State#state.tables),
    Tables = load_metatable_internal(),
    reset_helpers(Tables),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_table, TableInfo, Nodes}, _From, State) ->
    Table = TableInfo#app_metatable.table,
    Tables = State#state.tables,
    NewTables = update_tables_with_table_info(TableInfo, Tables),
    Response = create_table_internal(Table, Nodes, NewTables),
    reset_helpers(TableInfo),
    FinalTables = load_metatable_internal(),
    {reply, Response, State#state{tables = FinalTables}};

handle_call({get_functions, Table}, _From, State) ->
    Tables = State#state.tables,
    TableInfo = table_info(Table, Tables),
    Response = get_functions_internal(TableInfo),
    {reply, Response, State};

handle_call({set_read_transform_function, Table, Function}, _From, State) ->
    Response = case validate_function_identifier(Function) of
        true -> 
            set_read_transform_function(Table, Function, State#state.tables);
        false ->
            {error, {?INVALID_FUNCTION_IDENTIFIER, Function}}
    end,
    FinalTables = load_metatable_internal(),
    {reply, Response, State#state{tables = FinalTables}};

handle_call({set_write_transform_function, Table, Function}, _From, State) ->
    Response = case validate_function_identifier(Function) of
        true -> 
            set_write_transform_function(Table, Function, State#state.tables);
        false ->
            {error, {?INVALID_FUNCTION_IDENTIFIER, Function}}
    end,
    FinalTables = load_metatable_internal(),
    {reply, Response, State#state{tables = FinalTables}};

handle_call({set_refresh_function, Table, #refresh_data{
                    function_identifier = FunctionIdentifier} = RefreshData}, _From, State) ->
    Response = case validate_function_identifier(FunctionIdentifier) of
        true -> 
            set_refresh_function(Table, RefreshData, State#state.tables);
        false ->
            {error, {?INVALID_FUNCTION_IDENTIFIER, FunctionIdentifier}}
    end,
    app_cache_refresher:reset_function(Table),
    FinalTables = load_metatable_internal(),
    {reply, Response, State#state{tables = FinalTables}};

handle_call({set_persist_function, Table, #persist_data{
                    function_identifier = FunctionIdentifier} = PersistData}, _From, State) ->
    Response = case validate_function_identifier(FunctionIdentifier) of
        true -> 
            set_persist_function(Table, PersistData, State#state.tables);
        false ->
            {error, {?INVALID_FUNCTION_IDENTIFIER, FunctionIdentifier}}
    end,
    FinalTables = load_metatable_internal(),
    {reply, Response, State#state{tables = FinalTables}};

handle_call({table_info, Table}, _From, State) ->
    Response = table_info(Table, State#state.tables),
    {reply, Response, State};

handle_call({table_version, Table}, _From, State) ->
    Response = table_version(Table, State#state.tables),
    {reply, Response, State};

handle_call({table_time_to_live, Table}, _From, State) ->
    Response = table_time_to_live(Table, State#state.tables),
    {reply, Response, State};

handle_call({table_fields, Table}, _From, State) ->
    Response = table_fields(Table, State#state.tables),
    {reply, Response, State};

handle_call({update_table_time_to_live, Table, TTL}, _From, State) ->
    update_table_time_to_live_internal(Table, TTL),
    Tables = load_metatable_internal(),
    {reply, Tables, State#state{tables = Tables}};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.


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

-spec create_metatable([node()]) -> ok | {aborted, Reason :: any()}.
create_metatable(Nodes) ->
    case mnesia:create_table(?METATABLE, [{access_mode, read_write},
                                               {record_name, ?METATABLE},
                                               {attributes,
                                               app_cache:get_record_fields(?METATABLE)},
                                               {disc_copies, Nodes},
                                               {type, set}]) of
        {atomic, ok} ->
            ok;
        Error ->
            throw(Error)
    end.

-spec init_metatable_internal([node()]) -> ok | {aborted, Reason :: any()}.
init_metatable_internal(Nodes) ->
    Fields = app_cache:get_record_fields(?METATABLE),
    %% Make sure that the schema of the Mnesia table is up-to-date.
    try ((length(Fields) + 1 =:= mnesia:table_info(?METATABLE, arity)) andalso
         (Fields -- mnesia:table_info(?METATABLE, attributes)) =:= []) of
        true ->
            ok;
        false ->
            upgrade_metatable()
    catch
        _ : _ ->
            create_metatable(Nodes)
    end.

-spec load_metatable_internal() -> ok | {aborted, Reason :: any()}.
load_metatable_internal() ->
    {atomic, Data} = mnesia:transaction(fun() -> 
                    mnesia:match_object(#app_metatable{_ = '_'}) end),
    Data.

-spec cache_init_internal([node()], [#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
cache_init_internal(Nodes, Tables) ->
    try
        lists:foreach(fun(#app_metatable{table = Table}) -> 
                           init_table_internal(Table, Nodes, Tables)
                  end, Tables),
        Tables
    catch
        _:Error ->
            lager:error("Error ~p initializing mnesia.  Did you forget to run ~p:setup()?~n", [Error, ?SERVER]),
            {error, Error}
    end.

-spec init_table_internal(table(), [node()], [#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
init_table_internal(Table, Nodes, Tables) ->
    Version = table_version(Table, Tables),
    
    Fields = table_fields(Table, Tables),
    OldVersion = 
    case mnesia:transaction(fun() -> mnesia:read(?METATABLE, Table) end) of
        {atomic, [#app_metatable{version = Number}]} ->
            Number;
        {atomic, []} ->
            Version
    end,
    %% Make sure that the schema of the Mnesia table is up-to-date.
    try 
        case ((OldVersion =:= Version) andalso
              (length(Fields) + 1 =:= mnesia:table_info(Table, arity)) andalso
              (Fields -- mnesia:table_info(Table, attributes)) =:= []) of
            true ->
                ok;
            false ->
                upgrade_table(Table, OldVersion, Version, Fields)
        end
    catch
        _ : _ ->
            create_table_internal(Table, Nodes, Tables)
    end.

-spec create_tables_internal([node()], [#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
create_tables_internal(Nodes, Tables) ->
    create_metatable(Nodes),
    lists:foreach(fun(#app_metatable{table = Table}) ->
                          create_table_internal(Table, Nodes, Tables)
                  end, Tables).



-spec create_table_internal(table(), [node()], [#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
create_table_internal(Table, Nodes, Tables) ->
    #app_metatable{version = Version, 
                   time_to_live = TimeToLive, 
                   type = Type, 
                   fields = Fields, 
                   secondary_index_fields = IndexFields} = table_info(Table, Tables),
    {atomic, ok} = mnesia:create_table(Table, 
                                       [{access_mode, read_write},
                                        {record_name, Table},
                                        {attributes,
                                         table_fields(Table, Tables)},
                                        {disc_copies, Nodes},
                                        {type, Type},
                                        {local_content, true}]),
    % Add secondary indexes
    lists:map(fun(Field) ->
                {atomic, ok} = mnesia:add_table_index(Table, Field)
        end, get_index_fields(TimeToLive, IndexFields)),

    WriteFun = fun() -> 
            mnesia:write(#app_metatable{
                    table = Table,
                    version = Version,
                    time_to_live = TimeToLive,
                    type = Type,
                    fields = Fields,
                    secondary_index_fields = IndexFields,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = create_table
                    }) end,
    {atomic, ok} = mnesia:transaction(WriteFun).

%% @doc Update the TTL for the given table in the metatable
update_table_time_to_live_internal(Table, TimeToLive) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    time_to_live = TimeToLive,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_ttl
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun),
    ok.

%% @doc Update the read_transform_fun for the given table in the metatable
update_table_read_transform_function(Table, FunctionIdentifier) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    read_transform_function = FunctionIdentifier,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_read_transform_function
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun),
    ok.

%% @doc Update the write_transform_fun for the given table in the metatable
update_table_write_transform_function(Table, FunctionIdentifier) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    write_transform_function = FunctionIdentifier,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_write_transform_function
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun),
    ok.

update_table_refresh_function(Table, Function) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    refresh_function = Function,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_refresh_function
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun),
    ok.

update_table_persist_function(Table, PersistData) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    persist_function = PersistData,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_persist_function
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun),
    ok.

%%
%%  These don't use  mnesia
%%
-spec get_functions_internal(#app_metatable{}) -> function() | undefined.
get_functions_internal(undefined) ->
    #data_functions{};
get_functions_internal(TableInfo) ->
    #app_metatable{read_transform_function = ReadFunction,
                   write_transform_function = WriteFunction,
                   refresh_function = RefreshFunction,
                   persist_function = PersistFunction} = TableInfo,
    #data_functions{read_transform_function = ReadFunction,
                    write_transform_function = WriteFunction,
                    refresh_function = RefreshFunction,
                    persist_function = PersistFunction}.

-spec set_read_transform_function(table(), function(), [#app_metatable{}]) -> {ok | error(), [#app_metatable{}]}.
set_read_transform_function(Table, Function, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        Metatable when is_record(Metatable, app_metatable) ->
            update_table_read_transform_function(Table,Function);
        false ->
            {{error, {?INVALID_TABLE, Table}}, Tables}
    end.

-spec set_write_transform_function(table(), function(), [#app_metatable{}]) -> {ok | error(), [#app_metatable{}]}.
set_write_transform_function(Table, Function, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        Metatable when is_record(Metatable, app_metatable) ->
            update_table_write_transform_function(Table,Function);
        false ->
            {{error, {?INVALID_TABLE, Table}}, Tables}
    end.


-spec set_refresh_function(table(), function(), [#app_metatable{}]) -> {ok | error(), [#app_metatable{}]}.
set_refresh_function(Table, Function, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        Metatable when is_record(Metatable, app_metatable) ->
            update_table_refresh_function(Table,Function);
        false ->
            {{error, {?INVALID_TABLE, Table}}, Tables}
    end.


-spec set_persist_function(table(), function(), [#app_metatable{}]) -> {ok | error(), [#app_metatable{}]}.
set_persist_function(Table, PersistData, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        Metatable when is_record(Metatable, app_metatable) ->
            update_table_persist_function(Table,PersistData);
        false ->
            {{error, {?INVALID_TABLE, Table}}, Tables}
    end.


-spec table_info(table(), [#app_metatable{}]) -> {table_version(), time_to_live(), table_type()} | undefined.
table_info(Table, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        #app_metatable{table = Table} = TableInfo ->
            TableInfo;
        false ->
            undefined
    end.

-spec table_version(table(), [#app_metatable{}]) -> table_version().
table_version(Table, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        #app_metatable{version = Version} ->
            Version;
        false ->
            undefined
    end.

-spec table_time_to_live(table(), [#app_metatable{}]) -> time_to_live().
table_time_to_live(Table, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        #app_metatable{time_to_live = TimeToLive} ->
            TimeToLive;
        false ->
            app_cache:get_env(cache_time_to_live)
    end.

-spec table_fields(table(), [#app_metatable{}]) -> table_fields().
table_fields(Table, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        #app_metatable{fields = Fields} ->
            Fields;
        false ->
            []
    end.

%%
%% Table accessors
%% 

-spec check_key_exists(transaction_type(), table(), table_key()) -> boolean().
check_key_exists(TransactionType, Table, Key) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            refresh_if_necessary(TableInfo, [Key]),
            CachedData = cache_entry(TransactionType, Table, Key),
            case filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData) of
                [_Data] ->
                    true;
                _ ->
                    false
            end
    end.

-spec read_data(transaction_type(), table(), table_key()) -> list().
read_data(TransactionType, Table, Key) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            refresh_if_necessary(TableInfo, [Key]),
            CachedData = cache_entry(TransactionType, Table, Key),
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_data_from_index(transaction_type(), table(), table_key(), table_key()) -> list().
read_data_from_index(TransactionType, Table, Key, IndexField) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Fields = table_fields(Table),
            IndexPosition = get_index(IndexField, Fields),
            Data1 = cache_entry_from_index(TransactionType, Table, Key, IndexPosition),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_entry_from_index(TransactionType, Table, Key, IndexPosition);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

%% @doc It returns the largest key in the table
%%      This assumes that the table is of type ordered_set. 
-spec read_data_by_last_key(transaction_type(), table()) -> list().
read_data_by_last_key(TransactionType, Table) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Data1 = cache_last_key_entry(TransactionType, Table),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_last_key_entry(TransactionType, Table);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_after(transaction_type(), table(), table_key()) -> list().
read_after(TransactionType, Table, After) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Data1 = cache_select(TransactionType, Table, After),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_select(TransactionType, Table, After);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_records(transaction_type(), tuple()) -> list().
read_records(TransactionType, Record) ->
    Table = element(1, Record),
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Record1 = clear_timestamp_if_unset(TTLFieldIndex, Record),
            Data1 = cache_select_records(TransactionType, Record1),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_select_records(TransactionType, Record1);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_all_data(transaction_type(), table()) -> list().
read_all_data(TransactionType, Table) ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Data1 = cache_select_all(TransactionType, Table),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_select_all(TransactionType, Table);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_last_n_entries(transaction_type(), table(), pos_integer()) -> list().
read_last_n_entries(TransactionType, Table, N) when N > 0 ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_select_last_n_entries(TransactionType, Table, N),
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_first_n_entries(transaction_type(), table(), pos_integer()) -> list().
read_first_n_entries(TransactionType, Table, N) when N > 0 ->
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Data1 = cache_select_first_n_entries(TransactionType, Table, N),
            CachedData = case refresh_if_necessary(TableInfo, get_data_keys(Data1)) of
                true ->
                    cache_select_first_n_entries(TransactionType, Table, N);
                false ->
                    Data1
            end,
            filter_and_update_data(get_functions_internal(TableInfo), TableTTL, TTLFieldIndex, CachedData)
    end.

%% @doc If necessary, have the cache update itself w/ current data
%%      Returns true if the update needs to happen *before* the read
-spec refresh_if_necessary(#app_metatable{}, [table_key()]) -> boolean().
refresh_if_necessary(#app_metatable{
                refresh_function = undefined}, _Keys) -> 
    false;
refresh_if_necessary(#app_metatable{
                refresh_function = #refresh_data{
                    function_identifier = undefined}}, _Keys) -> 
    false;
refresh_if_necessary(#app_metatable{
                refresh_function = #refresh_data{
                    before_each_read = false,
                    after_each_read = false}} = TableInfo, Keys) -> 
    app_cache_refresher:refresh_data(async, TableInfo#app_metatable.table, Keys),
    false;
refresh_if_necessary(#app_metatable{
                refresh_function = #refresh_data{
                    before_each_read = true,
                    after_each_read = false}} = TableInfo, Keys) -> 
    app_cache_refresher:refresh_data(sync, TableInfo#app_metatable.table, Keys),
    true;
% WARNING: HERE BE RACE CONDITIONS
%           You could, theoretically, have the "after_read" happen before the
%           return
refresh_if_necessary(#app_metatable{
                refresh_function = #refresh_data{
                    before_each_read = false,
                    after_each_read = true}} = TableInfo, Keys) -> 
    proc_lib:spawn_link(fun() -> app_cache_refresher:refresh_data(sync, TableInfo#app_metatable.table, Keys) end),
    false;
% WARNING: SAME RACE AS ABOVE
refresh_if_necessary(#app_metatable{
                refresh_function = #refresh_data{
                    before_each_read = true,
                    after_each_read = true}} = TableInfo, Keys) -> 
    app_cache_refresher:refresh_data(sync, TableInfo#app_metatable.table, Keys),
    proc_lib:spawn_link(fun() -> app_cache_refresher:refresh_data(sync, TableInfo#app_metatable.table, Keys) end),
    true.


-spec write_data(transaction_type(), any()) -> ok | error().
write_data(TransactionType, Data) ->
    Table = element(1, Data),
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {_, TTLFieldIndex} -> 
            TimestampedData = get_timestamped_data(TTLFieldIndex, Data),
            TransformedData = write_transform_data(get_functions_internal(TableInfo), TimestampedData),
            persist_data(get_functions_internal(TableInfo), 
                         TransactionType, _OverwriteTimestamp = false, 
                         TransformedData, _ClearedTimestampData = undefined)
    end.

-spec write_data_overwriting_timestamp(transaction_type(), any()) -> ok | error().
write_data_overwriting_timestamp(TransactionType, Data) ->
    Table = element(1, Data),
    TableInfo = table_info(Table),
    % return only the valid values
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} = Error ->
            Error;
        {_, TTLFieldIndex} -> 
            TimestampedData = get_timestamped_data(TTLFieldIndex, Data),
            TransformedData = write_transform_data(get_functions_internal(TableInfo), TimestampedData),
            ClearedTimestampData = clear_timestamp_if_exists(TTLFieldIndex, TransformedData),
            persist_data(get_functions_internal(TableInfo), 
                         TransactionType, _OverwriteTimestamp = true, 
                         TransformedData, ClearedTimestampData)
    end.

%% @doc persist the data to mnesia (and maybe somewhere else?)
%%      with OverwriteTimestamp =:= true, this will delete any existing records w/ the same
%%      key
-spec persist_data(#data_functions{}, transaction_type(), boolean(), any(), any()) -> ok | error().
persist_data(#data_functions{persist_function = undefined}, 
             TransactionType, OverwriteTimestamp, Data, ClearedTimestampData) ->
    write_data_to_cache(TransactionType, OverwriteTimestamp, Data,
                        ClearedTimestampData);
persist_data(#data_functions{persist_function = 
                             #persist_data{function_identifier = undefined}}, 
             TransactionType, OverwriteTimestamp, Data, ClearedTimestampData) ->
    write_data_to_cache(TransactionType, OverwriteTimestamp, Data, ClearedTimestampData);
persist_data(#data_functions{persist_function = 
                             #persist_data{synchronous = true, 
                                           function_identifier = FunctionIdentifier}}, 
             TransactionType, OverwriteTimestamp, Data, ClearedTimestampData) ->
    try
        ok = write_data_to_cache(TransactionType, OverwriteTimestamp, Data, ClearedTimestampData),
        transform_data(FunctionIdentifier, Data)
    catch
        _:_ ->
            roll_back_write(TransactionType, OverwriteTimestamp, Data),
            {error, {?PERSIST_FAILURE, Data}}
    end;
persist_data(#data_functions{persist_function = #persist_data{synchronous = false, 
                                                              function_identifier
                                                              = FunctionIdentifier}}, TransactionType, OverwriteTimestamp, Data, ClearedTimestampData) ->
    write_data_to_cache(TransactionType, OverwriteTimestamp, Data, ClearedTimestampData),
    proc_lib:spawn_link(fun() -> transform_data(FunctionIdentifier, Data) end);
persist_data(_, _TransactionType, _OverwriteTimestamp, _Data, _ClearedTimestampData) ->
    {error, ?INVALID_PERSIST_FUNCTION}.

% This one presumes that there is only one record w/ the given key (i.e., not a
% bag)
-spec roll_back_write(transaction_type(), boolean(), any()) -> ok.
roll_back_write(TransactionType, _OverwriteTimestamp = false, Record) ->
    Table = element(1, Record),
    Key = element(2, Record),
    delete_data(TransactionType, Table, Key);
% This one presumes that there are many records with the given key, i.e., a
% bag)
roll_back_write(TransactionType, _OverwriteTimestamp = true, Record) ->
    delete_record(TransactionType, _IgnoreTimestamp = true, Record).
    

%%%
%%% Actual Mnesia functions for CRUD activities
%%%

%% Increments
-spec increment_data(transaction_type(), Table::table(), Key::table_key(), Value::sequence_value) -> ok | error().
increment_data(_TransactionType, Table, Key, Value) ->
    increment_data(Table, Key, Value).

-spec increment_data(Table::table(), Key::table_key(), Value::sequence_value) -> ok | error().
increment_data(Table, Key, Value) ->
    mnesia:dirty_update_counter(Table, Key, Value).


%% Writes
-spec write_data_to_cache(transaction_type(), boolean(), any(), any()) -> ok | error().
write_data_to_cache(?TRANSACTION_TYPE_SAFE, _OverwriteTimestamp = false, Data, _ClearedTimestampData) -> 
    WriteFun = fun () -> mnesia:write(Data) end,
    case mnesia:transaction(WriteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
write_data_to_cache(?TRANSACTION_TYPE_SAFE, _OverwriteTimestamp = true, Data, ClearedTimestampData) -> 
    DataFun = fun () -> 
            % Get all the records that match when we ignore the timestamp
            Table = element(1, ClearedTimestampData),
            Records = case mnesia:select(Table, [{ClearedTimestampData, [], ['$_']}]) of
                Result when is_list(Result) ->
                    Result;
                _ ->
                    []
            end,
            % Delete these records
            [mnesia:delete_object(Record) || Record <- Records],
            % And write the new data
            mnesia:write(Data) end,
    case mnesia:transaction(DataFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
write_data_to_cache(?TRANSACTION_TYPE_DIRTY, _OverwriteTimestamp = true, Data, ClearedTimestampData) -> 
    Table = element(1, ClearedTimestampData),
    Records = case mnesia:dirty_select(Table, [{ClearedTimestampData, [], ['$_']}]) of
        Result when is_list(Result) ->
            Result;
        _ ->
            []
    end,
    % Delete these records
    [mnesia:dirty_delete_object(Record) || Record <- Records],
    % And write the new data
    mnesia:dirty_write(Data);
write_data_to_cache(?TRANSACTION_TYPE_DIRTY, _OverwriteTimestamp = false, Data, _ClearedTimestampData) -> 
    mnesia:dirty_write(Data).


%% Deletes
-spec delete_data(transaction_type(), Table::table(), Key::table_key()) -> ok | error().
delete_data(?TRANSACTION_TYPE_SAFE, Table, Key) ->
    DeleteFun = fun () -> mnesia:delete({Table, Key}) end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            app_cache_refresher:remove_key(Table, Key),
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
delete_data(?TRANSACTION_TYPE_DIRTY, Table, Key) ->
    mnesia:dirty_delete({Table, Key}),
    app_cache_refresher:remove_key(Table, Key).

%% Deletes
-spec delete_all_data(transaction_type(), Table::table()) -> ok | error().
delete_all_data(_, Table) ->
    case mnesia:clear_table(Table) of
        {atomic, ok} ->
            app_cache_refresher:clear_table(Table),
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end.

-spec delete_record(transaction_type(), boolean(), any()) -> ok | error().
delete_record(?TRANSACTION_TYPE_SAFE = TransactionType, _IgnoreTimestamp = false, Record) ->
    DeleteFun = fun () -> mnesia:delete_object(Record) end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            Table = element(1, Record),
            Key = element(2, Record),
            delete_refresher_if_necessary(TransactionType, Table, Key);
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
delete_record(?TRANSACTION_TYPE_SAFE = TransactionType, _IgnoreTimestamp = true, Record) ->
    ClearedRecord = clear_timestamp_if_exists(Record),
    Table = element(1, ClearedRecord),
    DeleteFun = fun() ->
            Records = case mnesia:select(Table, [{ClearedRecord, [], ['$_']}]) of
                Result when is_list(Result) ->
                    Result;
                _ ->
                    []
            end,
            % Delete these records
            [mnesia:delete_object(InRecord) || InRecord <- Records]
    end,
    case mnesia:transaction(DeleteFun) of
        {atomic, _} ->
            Table = element(1, Record),
            Key = element(2, Record),
            delete_refresher_if_necessary(TransactionType, Table, Key);
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
delete_record(?TRANSACTION_TYPE_DIRTY = TransactionType, _IgnoreTimestamp = false, Record) ->
    Table = element(1, Record),
    Key = element(2, Record),
    mnesia:dirty_delete_object(Record),
    delete_refresher_if_necessary(TransactionType, Table, Key);
delete_record(?TRANSACTION_TYPE_DIRTY = TransactionType, _IgnoreTimestamp = true, Record) ->
    ClearedRecord = clear_timestamp_if_exists(Record),
    Table = element(1, ClearedRecord),
    Records = case mnesia:dirty_select(Table, [{ClearedRecord, [], ['$_']}]) of
        Result when is_list(Result) ->
            Result;
        _ ->
            []
    end,
    % Delete these records
   [mnesia:dirty_delete_object(InRecord) || InRecord <- Records],
    Table = element(1, Record),
    Key = element(2, Record),
    delete_refresher_if_necessary(TransactionType, Table, Key).

%% Reads
-spec cache_entry(transaction_type(), table(), table_key()) -> [any()].
cache_entry(?TRANSACTION_TYPE_SAFE, Table, Key) ->
    ReadFun = fun () -> mnesia:read(Table, Key) end,
    case mnesia:transaction(ReadFun) of
        {atomic, Data} ->
            Data;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
cache_entry(?TRANSACTION_TYPE_DIRTY, Table, Key) ->
    mnesia:dirty_read(Table, Key).

-spec cache_entry_from_index(transaction_type(), table(), table_key(), table_key_position()) -> [any()].
cache_entry_from_index(?TRANSACTION_TYPE_SAFE, Table, Key, IndexPosition) ->
    ReadFun = fun () -> mnesia:index_read(Table, Key, IndexPosition + 1) end,
    case mnesia:transaction(ReadFun) of
        {atomic, Data} ->
            Data;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
cache_entry_from_index(?TRANSACTION_TYPE_DIRTY, Table, Key, IndexPosition) ->
    mnesia:dirty_index_read(Table, Key, IndexPosition + 1).


-spec cache_last_key_entry(transaction_type(), table()) -> [any()].
cache_last_key_entry(TransactionType = ?TRANSACTION_TYPE_SAFE, Table) ->
    ReadFun = fun () -> mnesia:last(Table) end,
    case mnesia:transaction(ReadFun) of
        {atomic,'$end_of_table'} ->
            [];
        {atomic, Key} ->
            cache_entry(TransactionType, Table, Key);
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
cache_last_key_entry(TransactionType = ?TRANSACTION_TYPE_DIRTY, Table) ->
    case mnesia:dirty_last(Table) of
        '$end_of_table' ->
            [];
        Key ->
            cache_entry(TransactionType, Table, Key)
    end.


-spec cache_select_last_n_entries(transaction_type(), table(), pos_integer()) -> [any()].
cache_select_last_n_entries(TransactionType = ?TRANSACTION_TYPE_SAFE, Table, N) ->
    ReadFun = fun () -> 
            case mnesia:last(Table) of
                '$end_of_table' ->
                    [];
                Key ->
                    get_lifo_data(TransactionType, Table, Key, mnesia:read(Table, Key), N-1)
            end
    end,
    case mnesia:transaction(ReadFun) of
        {atomic,[]} ->
            [];
        {atomic, Data} ->
            Data;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;


cache_select_last_n_entries(TransactionType = ?TRANSACTION_TYPE_DIRTY, Table, N) ->
    case mnesia:dirty_last(Table) of
        '$end_of_table' ->
            [];
        Key ->
            get_lifo_data(TransactionType, Table, Key, mnesia:dirty_read(Table, Key), N-1)
    end.

-spec cache_select_first_n_entries(transaction_type(), table(), pos_integer()) -> [any()].
cache_select_first_n_entries(TransactionType, Table, N) ->
    MatchHead = '$1',
    Guard =  [],
    Result = ['$_'],
    cache_select(TransactionType, Table, undefined, [{MatchHead, Guard, Result}], N).


-spec cache_select_all(transaction_type(), table()) -> [any()].
cache_select_all(TransactionType, Table) ->
    MatchHead = '$1',
    Guard =  [],
    Result = ['$_'],
    cache_select(TransactionType, Table, undefined, [{MatchHead, Guard, Result}]).

-spec cache_select_records(transaction_type(), tuple()) -> [any()].
cache_select_records(TransactionType, Record) ->
    Table = element(1, Record),
    MatchHead = Record,
    Guard =  [],
    Result = ['$_'],
    cache_select(TransactionType, Table, undefined, [{MatchHead, Guard, Result}]).

-spec cache_select(transaction_type(), table(), table_key()) -> [any()].
cache_select(TransactionType, Table, After) ->
    MatchHead = '$1',
    Guard =  [{'>=', {element, 2, '$1'}, After}],
    Result = ['$_'],
    cache_select(TransactionType, Table, After, [{MatchHead, Guard, Result}]).

-spec cache_select(transaction_type(), table(), table_key(), any()) -> [any()].
cache_select(?TRANSACTION_TYPE_SAFE, Table, _After, MatchSpec) ->
    SelectFun = fun () -> mnesia:select(Table, MatchSpec) end,
    case mnesia:transaction(SelectFun) of
        {atomic, Data} ->
            Data;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
cache_select(?TRANSACTION_TYPE_DIRTY, Table, _After, MatchSpec) ->
    mnesia:dirty_select(Table, MatchSpec).

-spec cache_select(transaction_type(), table(), table_key(), any(), pos_integer()) -> [any()].
cache_select(_TransactionType, Table, _After, MatchSpec, N) ->
    SelectFun = fun () -> mnesia:select(Table, MatchSpec, N, read) end,
    case mnesia:transaction(SelectFun) of
        {atomic, {Data, _}} ->
            Data;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end.

-spec get_ttl_and_field_index_internal(#app_metatable{}) -> time_to_live().
get_ttl_and_field_index_internal(TableInfo) -> 
    try
        #app_metatable{time_to_live = TTL, fields = Fields} = TableInfo,
        FieldIndex = get_index(?TIMESTAMP, Fields),
        {TTL, FieldIndex}
    catch
        _:_ ->
            {error, ?INVALID_TABLE}
    end.


-spec is_cache_valid(timestamp() | ?INFINITY, last_update() | ?DEFAULT_TIMESTAMP, timestamp()) -> boolean().
is_cache_valid(_TableTTL, ?DEFAULT_TIMESTAMP, _CurrentTime) ->
    %% There is no timestamp, so the data is not valid
    false;
is_cache_valid(?INFINITY, _LastUpdate, _CurrentTime) ->
    %% The table has in infinite TTL
    true;
is_cache_valid(TableTTL, LastUpdate, CurrentTime) ->
    LastUpdate + TableTTL > CurrentTime.

-spec current_time_in_gregorian_seconds() -> non_neg_integer().
current_time_in_gregorian_seconds() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

%%  @doc Filter the data before it is sent back to the user, so that any expired
%%       records that have not been scavenged are ignored. 
%%       Also run any transform necessary on the data
-spec filter_and_update_data(#data_functions{}, TableTTL::timestamp(), table_key_position(), Data::list()) -> any().
filter_and_update_data(DataFunctions, ?INFINITY, _, Data) ->
    read_transform_data(DataFunctions, Data);
filter_and_update_data(DataFunctions, TableTTL, TTLFieldIndex, Data) ->
    CurrentTime = current_time_in_gregorian_seconds(),
    lists:flatmap(fun(X) ->
                % '+ 1' because we're looking at the tuple, not the record
                LastUpdate = element(TTLFieldIndex + 1, X),
                %% We store the datetime as seconds in the Gregorian calendar (since Jan 1, 0001 at 00:00:00).
                case is_cache_valid(TableTTL, LastUpdate, CurrentTime) of
                    true ->
                        [read_transform_data(DataFunctions, X)];
                    false ->
                        []
                end
        end, Data).

%% Only add an index on timestamp if it is relevant
get_index_fields(?INFINITY, IndexFields) ->
    IndexFields;
get_index_fields(_TimeToLive, IndexFields) ->
    [?TIMESTAMP | IndexFields].


%% @doc Get the index of an element in a list
get_index(FieldName, Fields) -> get_index(FieldName, Fields, 1).
get_index(_FieldName, [], _) -> undefined;
get_index(FieldName, [FieldName | _], N) -> N;
get_index(FieldName, [_ | Tail], N) -> get_index(FieldName, Tail, N+1).

%% @doc Reset the timer on the scavenger for the table
-spec reset_helpers(#app_metatable{} | [#app_metatable{}]) -> ok.
reset_helpers([]) -> ok;
reset_helpers([#app_metatable{table = Table} | Rest]) -> 
    app_cache_scavenger:reset_timer(Table),
    app_cache_refresher:reset_function(Table),
    reset_helpers(Rest);
reset_helpers(#app_metatable{table = Table}) ->
    app_cache_scavenger:reset_timer(Table),
    app_cache_refresher:reset_function(Table),
    ok.

-spec update_tables_with_table_info(#app_metatable{}, [#app_metatable{}]) -> [#app_metatable{}].
update_tables_with_table_info(TableInfo, Tables) ->
    Table = TableInfo#app_metatable.table,
    case lists:keytake(Table, #app_metatable.table, Tables) of
        {value, _, Table1} ->
            [TableInfo | Table1];
        false ->
            [TableInfo | Tables]
    end.

%% @doc If there is a timestamp field in this record, set it to the current time
-spec get_timestamped_data(undefined | table_key_position(), any()) -> any().
get_timestamped_data(undefined, Data) ->
    Data;
get_timestamped_data(TTLFieldIndex, Data) ->
    CurrentTime = current_time_in_gregorian_seconds(),
    % '+ 1' because we're looking at the tuple, not the record
    setelement(TTLFieldIndex + 1, Data, CurrentTime).

%% @doc If there is a timestamp field in this record, and it is 'undefined', set
%%      '_' (for use in a matchspec)
-spec clear_timestamp_if_unset(undefined | table_key_position(), any()) -> any().
clear_timestamp_if_unset(undefined, Data) ->
    Data;
clear_timestamp_if_unset(TTLFieldIndex, Data) ->
    case element(TTLFieldIndex+1, Data) of
        undefined ->
            setelement(TTLFieldIndex + 1, Data, '_');
        _ ->
            Data
    end.

%% @doc If there is a timestamp field in this record,set it to '_' (for use in a matchspec)
-spec clear_timestamp_if_exists(any()) -> any().
clear_timestamp_if_exists(Record) ->
    Table = element(1, Record),
    TableInfo = table_info(Table),
    case get_ttl_and_field_index_internal(TableInfo) of
        {error, _} ->
            Record;
        {_TableTTL, TTLFieldIndex} -> 
            clear_timestamp_if_exists(TTLFieldIndex, Record)
    end.

-spec clear_timestamp_if_exists(undefined | table_key_position(), any()) -> any().
clear_timestamp_if_exists(undefined, Data) ->
    Data;
clear_timestamp_if_exists(TTLFieldIndex, Data) ->
    setelement(TTLFieldIndex + 1, Data, '_').

%% @doc Get the last N entries from the ordered set
-spec get_lifo_data(transaction_type(), table(), table_key(), list(), integer()) -> list().
get_lifo_data(_TransactionType, _Table, _Key, Acc, N) when N =< 0 -> lists:reverse(Acc);
get_lifo_data(TransactionType = ?TRANSACTION_TYPE_SAFE, Table, Key, Acc, N) ->
    case mnesia:prev(Table, Key) of
        '$end_of_table' ->
            lists:reverse(Acc);
        NKey ->
            get_lifo_data(TransactionType, Table, NKey, lists:flatten(mnesia:read(Table, NKey), Acc), N-1)
    end;
get_lifo_data(TransactionType = ?TRANSACTION_TYPE_DIRTY, Table, Key, Acc, N) ->
    case mnesia:dirty_prev(Table, Key) of
        '$end_of_table' ->
            lists:reverse(Acc);
        NKey ->
            get_lifo_data(TransactionType, Table, NKey, lists:flatten(mnesia:dirty_read(Table, NKey), Acc), N-1)
    end.

%% @doc Apply the write_transform to the given data
-spec write_transform_data(#data_functions{}, any()) -> any().
write_transform_data(#data_functions{write_transform_function = FunctionIdentifier}, Data) ->
    transform_data(FunctionIdentifier, Data).

%% @doc Apply the read_transform to the given data
-spec read_transform_data(#data_functions{}, any()) -> any().
read_transform_data(#data_functions{read_transform_function = FunctionIdentifier}, Data) ->
    transform_data(FunctionIdentifier, Data).

%% @doc Apply a given function to the record. This should be transparent, i.e., the
%%      output should be something recognizable as the same record
-spec transform_data(function() | undefined, any()) -> any().
transform_data(undefined, Data) -> Data;
transform_data({module_and_function, {Module, Function}}, Data) -> 
    erlang:apply(Module, Function, [Data]);
transform_data({function, Function}, Data) ->
    Function(Data);
transform_data(_Other, Data) -> Data.


%% @doc Get the keys (first item in the tuple)
%%      Note: duplicates are eliminated
-spec get_data_keys([tuple()] | undefined) -> [table_key()].
get_data_keys(undefined) ->
    [];
get_data_keys(Data) ->
    lists:usort([element(1, Item) || Item <- Data]).

%% @doc Make sure that the Function is sent in in the correct format
-spec validate_function_identifier(function_identifier()) -> boolean().
validate_function_identifier({function, Function}) when is_function(Function) ->
    true;
validate_function_identifier({module_and_function, {Module, Function}}) 
        when is_atom(Module) and is_atom(Function) ->
    true;
validate_function_identifier(undefined) ->
    true;
validate_function_identifier(_) ->
    false.

%% @doc Remove the refresher entry if there are no more keys
-spec delete_refresher_if_necessary(transaction_type(), table(), table_key()) -> ok.
delete_refresher_if_necessary(TransactionType, Table, Key) ->
    case check_key_exists(TransactionType, Table, Key) of
        false ->
            app_cache_refresher:remove_key(Table, Key);
        _ ->
            void
    end.

%%%
%%%  For Tests
%%%
%% @doc Double whatever is in the 'value' field of the inut
-spec double_test_table_1_value(#test_table_1{}) -> #test_table_1{}.
double_test_table_1_value(Data) when is_record(Data, test_table_1) ->
    Value = Data#test_table_1.value,
    Data#test_table_1{value = 2*Value}.
