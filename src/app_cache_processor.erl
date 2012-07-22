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

-compile([{parse_transform, dynarec}]).
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
-export([read_all_data/2]).
-export([read_last_n_entries/3]).
-export([read_first_n_entries/3]).
-export([write_data/2]).
-export([delete_data/3]).
-export([delete_record/2]).
-export([increment_data/3, increment_data/4]).

-export([table_fields/1]).



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

-spec get_ttl_and_field_index(table()) -> {timestamp(), table_key_position()}.
get_ttl_and_field_index(Table) ->
    gen_server:call(?PROCESSOR, {get_ttl_and_field_index, Table}).

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

-spec table_fields(table()) -> table_fields().
table_fields(Table) ->
    gen_server:call(?PROCESSOR, {table_fields, Table}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes]) ->
    try
        init_metatable_internal(Nodes),
        Tables = load_metatable_internal(),
        {ok, #state{tables = Tables}}
    catch
        _:Error ->
            lager:error("Error ~p initializing mnesia.  Did you forget to run ~p:setup()?~n", [Error, ?SERVER]),
            {stop, Error}
    end.

handle_call({cache_init, Nodes, Tables}, _From, State) ->
    Response = cache_init_internal(Nodes, Tables),
    reset_scavenger(Tables),
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
    reset_scavenger(table_info(Table, Tables)),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_tables, Nodes}, _From, State) ->
    Response = create_tables_internal(Nodes, State#state.tables),
    Tables = load_metatable_internal(),
    reset_scavenger(Tables),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_table, TableInfo, Nodes}, _From, State) ->
    Table = TableInfo#app_metatable.table,
    Tables = State#state.tables,
    NewTables = update_tables_with_table_info(TableInfo, Tables),
    Response = create_table_internal(Table, Nodes, NewTables),
    FinalTables = load_metatable_internal(),
    reset_scavenger(TableInfo),
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

handle_call({get_ttl_and_field_index, Table}, _From, State) ->
    Response = get_ttl_and_field_index(Table, State#state.tables),
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
    mnesia:dirty_match_object(#app_metatable{_ = '_'}).

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
    OldVersion = case mnesia:dirty_read(?METATABLE, Table) of
        [#app_metatable{version = Number}] ->
            Number;
        [] ->
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
    #app_metatable{version = Version, time_to_live = TimeToLive, type = Type, fields = Fields, secondary_index_fields = IndexFields} = table_info(Table, Tables),
    {atomic, ok} = mnesia:create_table(Table, [{access_mode, read_write},
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

update_table_time_to_live_internal(Table, TimeToLive) ->
    UpdateFun = fun() -> 
            [OldRecord] = mnesia:read(?METATABLE, Table),
            NewRecord = OldRecord#app_metatable{
                    table = Table,
                    time_to_live = TimeToLive,
                    last_update = current_time_in_gregorian_seconds(),
                    reason = update_ttl
                    }, 
            mnesia:write(NewRecord)
    end,
    {atomic, ok} = mnesia:transaction(UpdateFun).

%%
%%  These don't use  mnesia
%%

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
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_entry(TransactionType, Table, Key),
            case filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData) of
                [_Data] ->
                    true;
                _ ->
                    false
            end
    end.

-spec read_data(transaction_type(), table(), table_key()) -> list().
read_data(TransactionType, Table, Key) ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_entry(TransactionType, Table, Key),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_data_from_index(transaction_type(), table(), table_key(), table_key()) -> list().
read_data_from_index(TransactionType, Table, Key, IndexField) ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            Fields = table_fields(Table),
            IndexPosition = get_index(IndexField, Fields),
            CachedData = cache_entry_from_index(TransactionType, Table, Key, IndexPosition),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

%% @doc It returns the largest key in the table
%%      This assumes that the table is of type ordered_set. 
-spec read_data_by_last_key(transaction_type(), table()) -> list().
read_data_by_last_key(TransactionType, Table) ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_last_key_entry(TransactionType, Table),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_after(transaction_type(), table(), table_key()) -> list().
read_after(TransactionType, Table, After) ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            get_ttl_and_field_index(Table),
            CachedData = cache_select(TransactionType, Table, After),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_all_data(transaction_type(), table()) -> list().
read_all_data(TransactionType, Table) ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_select_all(TransactionType, Table),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_last_n_entries(transaction_type(), table(), pos_integer()) -> list().
read_last_n_entries(TransactionType, Table, N) when N > 0 ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_select_last_n_entries(TransactionType, Table, N),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec read_first_n_entries(transaction_type(), table(), pos_integer()) -> list().
read_first_n_entries(TransactionType, Table, N) when N > 0 ->
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {TableTTL, TTLFieldIndex} -> 
            CachedData = cache_select_first_n_entries(TransactionType, Table, N),
            filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData)
    end.

-spec write_data(transaction_type(), any()) -> ok | error().
write_data(TransactionType, Data) ->
    Table = element(1, Data),
    % return only the valid values
    case get_ttl_and_field_index(Table) of
        {error, _} = Error ->
            Error;
        {_, TTLFieldIndex} -> 
            % '+ 1' because we're looking at the tuple, not the record
            TimestampedData = get_timestamped_data(TTLFieldIndex, Data),
            write_timestamped_data(TransactionType, TimestampedData)
    end.

-spec write_timestamped_data(transaction_type(), any()) -> ok | error().
write_timestamped_data(?TRANSACTION_TYPE_SAFE, TimestampedData) -> 
    WriteFun = fun () -> mnesia:write(TimestampedData) end,
    case mnesia:transaction(WriteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
write_timestamped_data(?TRANSACTION_TYPE_DIRTY, TimestampedData) -> 
    mnesia:dirty_write(TimestampedData).


-spec delete_data(transaction_type(), Table::table(), Key::table_key()) -> ok | error().
delete_data(?TRANSACTION_TYPE_SAFE, Table, Key) ->
    DeleteFun = fun () -> mnesia:delete({Table, Key}) end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
delete_data(?TRANSACTION_TYPE_DIRTY, Table, Key) ->
    mnesia:dirty_delete({Table, Key}).

-spec delete_record(transaction_type(), tuple()) -> ok | error().
delete_record(?TRANSACTION_TYPE_SAFE, Record) ->
    DeleteFun = fun () -> mnesia:delete_object(Record) end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end;
delete_record(?TRANSACTION_TYPE_DIRTY, Record) ->
    mnesia:dirty_delete_object(Record).

-spec increment_data(transaction_type(), Table::table(), Key::table_key(), Value::sequence_value) -> ok | error().
increment_data(_TransactionType, Table, Key, Value) ->
    increment_data(Table, Key, Value).

-spec increment_data(Table::table(), Key::table_key(), Value::sequence_value) -> ok | error().
increment_data(Table, Key, Value) ->
    mnesia:dirty_update_counter(Table, Key, Value).

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

-spec get_ttl_and_field_index(table(), [#app_metatable{}]) -> time_to_live().
get_ttl_and_field_index(Table, Tables) -> 
    try
        #app_metatable{time_to_live = TTL, fields = Fields} = table_info(Table, Tables),
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


-spec filter_data_by_ttl(TableTTL::timestamp(), table_key_position(), Data::list()) -> any().
filter_data_by_ttl(?INFINITY, _, Data) ->
    Data;
filter_data_by_ttl(TableTTL, TTLFieldIndex, Data) ->
    CurrentTime = current_time_in_gregorian_seconds(),
    lists:filter(fun(X) ->
                % '+ 1' because we're looking at the tuple, not the record
                LastUpdate = element(TTLFieldIndex + 1, X),
                %% We store the datetime as seconds in the Gregorian calendar (since Jan 1, 0001 at 00:00:00).
                is_cache_valid(TableTTL, LastUpdate, CurrentTime)
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
-spec reset_scavenger(#app_metatable{} | [#app_metatable{}]) -> ok.
reset_scavenger([]) -> ok;
reset_scavenger([#app_metatable{table = Table} | Rest]) -> 
    app_cache_scavenger:reset_timer(Table),
    reset_scavenger(Rest);
reset_scavenger(#app_metatable{table = Table}) ->
    app_cache_scavenger:reset_timer(Table),
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

-spec get_timestamped_data(undefined | table_key_position(), any()) -> any().
get_timestamped_data(undefined, Data) ->
    Data;
get_timestamped_data(TTLFieldIndex, Data) ->
    CurrentTime = current_time_in_gregorian_seconds(),
    setelement(TTLFieldIndex + 1, Data, CurrentTime).

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
