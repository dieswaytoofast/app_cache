%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Generic app to provide caching
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(app_cache).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').

-compile([{parse_transform, lager_transform}]).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

%% Mnesia utility APIs
-export([get_env/0, get_env/1, get_env/2]).
-export([setup/0, start/0, stop/0, 
         cache_init/1, cache_init/2,
         init_metatable/0, init_metatable/1, init_table/1, init_table/2,
         get_metatable/0,
         create_tables/0, create_tables/1, create_metatable/0, create_metatable/1, create_table/1, create_table/2,
         upgrade_metatable/0, upgrade_table/1,
         table_info/1, table_version/1, table_time_to_live/1, table_fields/1,
         update_table_time_to_live/2,
         last_update_to_datetime/1, current_time_in_gregorian_seconds/0,
         cache_time_to_live/1, get_ttl_and_field_index/1
        ]).

%% Data accessor APIs
-export([key_exists/2, get_data_from_index/3, get_data/2, get_last_data/1, get_after/2, set_data/1, remove_data/2]).

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

-include("app_cache.hrl").

-define(SERVER, ?MODULE).

-record(state, {
            tables = []         :: [#app_metatable{}]
            }).



%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%%
%% Environment helper functions
%%

%% @doc Retrieve all key/value pairs in the env for the specified app.
-spec get_env() -> [{Key :: atom(), Value :: term()}].
get_env() ->
    application:get_all_env(?SERVER).

%% @doc The official way to get a value from the app's env.
%%      Will return the 'undefined' atom if that key is unset.
-spec get_env(Key :: atom()) -> term().
get_env(Key) ->
    get_env(Key, undefined).

%% @doc The official way to get a value from this application's env.
%%      Will return Default if that key is unset.
-spec get_env(Key :: atom(), Default :: term()) -> term().
get_env(Key, Default) ->
    case application:get_env(?SERVER, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.
%%
%% Mnesia utility functions
%%

%% @doc Start the application and all its dependencies.
-spec start() -> ok.
start() ->
    start_deps(?SERVER).

-spec start_deps(App :: atom()) -> ok.
start_deps(App) ->
    application:load(App),
    {ok, Deps} = application:get_key(App, applications),
    lists:foreach(fun start_deps/1, Deps),
    start_app(App).

-spec start_app(App :: atom()) -> ok.
start_app(App) ->
    case application:start(App) of
        {error, {already_started, _}} -> ok;
        ok                            -> ok
    end.


%% @doc Stop the application and all its dependencies.
-spec stop() -> ok.
stop() ->
    stop_deps(?SERVER).

-spec stop_deps(App :: atom()) -> ok.
stop_deps(App) ->
    stop_app(App),
    {ok, Deps} = application:get_key(App, applications),
    lists:foreach(fun stop_deps/1, lists:reverse(Deps)).

-spec stop_app(App :: atom()) -> ok.
stop_app(kernel) ->
    ok;
stop_app(stdlib) ->
    ok;
stop_app(App) ->
    case application:stop(App) of
        {error, {not_started, _}} -> ok;
        ok                        -> ok
    end.


%% @doc setup mnesia on this node for disc copies
-spec setup() -> {atomic, ok} | {error, Reason::any()}.
setup() ->
    mnesia:create_schema([node()]).

start_link() ->
    Nodes = get_env(cache_nodes, [node()]),
    start_link(Nodes).

start_link(Nodes) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Nodes], []).

-spec init_metatable() -> ok | {aborted, Reason :: any()}.
init_metatable() ->
    Nodes = get_env(cache_nodes, [node()]),
    init_metatable(Nodes).

-spec init_metatable([node()]) -> ok | {aborted, Reason :: any()}.
init_metatable(Nodes) ->
    gen_server:call(?SERVER, {init_metatable, Nodes}).

-spec get_metatable() -> [#app_metatable{}] | {aborted, Reason :: any()}.
get_metatable() ->
    gen_server:call(?SERVER, {get_metatable}).

-spec cache_init([#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
cache_init(Tables) ->
    Nodes = get_env(cache_init, [node()]),
    cache_init(Nodes, Tables).

-spec cache_init([node()], [#app_metatable{}]) -> ok | {aborted, Reason :: any()}.
cache_init(Nodes, Tables) ->
    gen_server:call(?SERVER, {cache_init, Nodes, Tables}).

-spec create_tables() -> ok | {aborted, Reason :: any()}.
create_tables() ->
    Nodes = get_env(cache_nodes, [node()]),
    create_tables(Nodes).

-spec create_tables([node()]) -> ok | {aborted, Reason :: any()}.
create_tables(Nodes) ->
    gen_server:call(?SERVER, {create_tables, Nodes}).


create_metatable() ->
    Nodes = get_env(cache_nodes, [node()]),
    create_metatable(Nodes).

-spec create_metatable([node()]) -> ok | {aborted, Reason :: any()}.
create_metatable(Nodes) ->
    case mnesia:create_table(?METATABLE, [{access_mode, read_write},
                                               {record_name, ?METATABLE},
                                               {attributes,
                                               record_util:get_fields_for_record(?METATABLE)},
                                               {disc_copies, Nodes},
                                               {type, set}]) of
        {atomic, ok} ->
            ok;
        Error ->
            throw(Error)
    end.

-spec init_table(table()) -> ok | {aborted, Reason :: any()}.
init_table(Table) ->
    Nodes = get_env(cache_nodes, [node()]),
    init_table(Table, Nodes).

-spec init_table(table(), [node()]) -> ok | {aborted, Reason :: any()}.
init_table(Table, Nodes) ->
    gen_server:call(?SERVER, {init_table, Table, Nodes}).

-spec create_table(table()) -> ok | {aborted, Reason :: any()}.
create_table(Table) ->
    Nodes = get_env(cache_nodes, [node()]),
    create_table(Table, Nodes).

-spec create_table(table(), [node()]) -> ok | {aborted, Reason :: any()}.
create_table(Table, Nodes) ->
    gen_server:call(?SERVER, {create_table, Table, Nodes}).

% TODO make this dependant on the node
-spec upgrade_metatable() -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_metatable() ->
    upgrade_table(?METATABLE, record_util:get_fields_for_record(?METATABLE)).

-spec upgrade_table(table()) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table) ->
    Fields = table_fields(Table),
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

-spec table_info(table()) -> #app_metatable{} | undefined.
table_info(Table) ->
    gen_server:call(?SERVER, {table_info, Table}).

-spec table_version(table()) -> table_version().
table_version(Table) ->
    gen_server:call(?SERVER, {table_version, Table}).

-spec table_time_to_live(table()) -> time_to_live().
table_time_to_live(Table) ->
    gen_server:call(?SERVER, {table_time_to_live, Table}).

-spec table_fields(table()) -> table_fields().
table_fields(Table) ->
    gen_server:call(?SERVER, {table_fields, Table}).

-spec cache_time_to_live(table()) -> time_to_live().
cache_time_to_live(Table) ->
    {TTL, _} = get_ttl_and_field_index(Table),
    TTL.

-spec last_update_to_datetime(last_update()) -> calendar:datetime().
last_update_to_datetime(LastUpdate) ->
    calendar:gregorian_seconds_to_datetime(LastUpdate).

-spec get_ttl_and_field_index(table()) -> {timestamp(), table_key_position()}.
get_ttl_and_field_index(Table) ->
    gen_server:call(?SERVER, {get_ttl_and_field_index, Table}).

-spec update_table_time_to_live(table(), time_to_live()) -> {timestamp(), table_key_position()}.
update_table_time_to_live(Table, TTL) ->
    gen_server:call(?SERVER, {update_table_time_to_live, Table, TTL}).

%%
%% Table accessors
%%
-spec key_exists(Table::table(), Key::table_key()) -> any().
key_exists(Table, Key) ->
    check_key_exists(Table, Key).

-spec get_data(Table::table(), Key::table_key()) -> any().
get_data(Table, Key) ->
    read_data(Table, Key).

-spec get_data_from_index(Table::table(), Key::table_key(), IndexField::table_key()) -> any().
get_data_from_index(Table, Key, IndexField) ->
    gen_server:call(?SERVER, {get_data_from_index, Table, Key, IndexField}).

-spec get_last_data(Table::table()) -> any().
get_last_data(Table) ->
    read_last_data(Table).

%% Get data after (in erlang term order) a value
-spec get_after(table(), table_key()) -> any().
get_after(Table, After) ->
    read_after(Table, After).

-spec set_data(Value::any()) -> ok | error().
set_data(Value) ->
    write_data(Value).

-spec remove_data(Table::table(), Key::table_key()) -> ok | error().
remove_data(Table, Key) ->
    delete_data(Table, Key).


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
    reset_scavenger(Table),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_tables, Nodes}, _From, State) ->
    Response = create_tables_internal(Nodes, State#state.tables),
    Tables = load_metatable_internal(),
    reset_scavenger(Tables),
    {reply, Response, State#state{tables = Tables}};

handle_call({create_table, Table, Nodes}, _From, State) ->
    Response = create_table_internal(Table, Nodes, State#state.tables),
    Tables = load_metatable_internal(),
    reset_scavenger(Table),
    {reply, Response, State#state{tables = Tables}};

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

handle_call({get_data_from_index, Table, Key, IndexField}, _From, State) ->
    Response = read_data_from_index(Table, Key, IndexField, State#state.tables),
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

-spec init_metatable_internal([node()]) -> ok | {aborted, Reason :: any()}.
init_metatable_internal(Nodes) ->
    Fields = record_util:get_fields_for_record(?METATABLE),
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
            get_env(cache_time_to_live)
    end.

-spec table_fields(table(), [#app_metatable{}]) -> table_fields().
table_fields(Table, Tables) ->
    case lists:keyfind(Table, #app_metatable.table, Tables) of
        #app_metatable{fields = Fields} ->
            Fields;
        false ->
            []
    end.

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
%% Table accessors
%% 

-spec check_key_exists(table(), table_key()) -> boolean().
check_key_exists(Table, Key) ->
    {TableTTL, TTLFieldIndex} = get_ttl_and_field_index(Table),
    CachedData = cache_entry(Table, Key),
    case filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData) of
        [_Data] ->
            true;
        _ ->
            false
    end.

-spec read_data(table(), table_key()) -> list().
read_data(Table, Key) ->
    {TableTTL, TTLFieldIndex} = get_ttl_and_field_index(Table),
    CachedData = cache_entry(Table, Key),
    filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData).

-spec read_data_from_index(table(), table_key(), table_key(), [#app_metatable{}]) -> list().
read_data_from_index(Table, Key, IndexField, Tables) ->
    {TableTTL, TTLFieldIndex} = get_ttl_and_field_index(Table),
    Fields = table_fields(Table, Tables),
    IndexPosition = get_index(IndexField, Fields),
    CachedData = cache_entry_from_index(Table, Key, IndexPosition),
    filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData).

-spec read_last_data(table()) -> list().
read_last_data(Table) ->
    {TableTTL, TTLFieldIndex} = get_ttl_and_field_index(Table),
    CachedData = cache_last_entry(Table),
    filter_data_by_ttl(TableTTL, TTLFieldIndex, CachedData).

-spec read_after(table(), table_key()) -> list().
read_after(Table, After) ->
    MatchHead = '$1',
    Guard =  [{'>=', {element, 2, '$1'}, After}],
    Result = ['$_'],
    Data = mnesia:dirty_select(Table, [{MatchHead, Guard, Result}]),

    % return only the valid values
    {TableTTL, TTLFieldIndex} = get_ttl_and_field_index(Table),
    filter_data_by_ttl(TableTTL, TTLFieldIndex, Data).

-spec write_data(any()) -> ok | error().
write_data(Data) ->
    CurrentTime = current_time_in_gregorian_seconds(),
    Table = element(1, Data),
    {_, TTLFieldIndex} = get_ttl_and_field_index(Table),
    % '+ 1' because we're looking at the tuple, not the record
    TimestampedData = setelement(TTLFieldIndex + 1, Data, CurrentTime),
    WriteFun = fun () -> mnesia:write(TimestampedData) end,
    case mnesia:transaction(WriteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end.

-spec delete_data(Table::table(), Key::table_key()) -> ok | error().
delete_data(Table, Key) ->
    DeleteFun = fun () -> mnesia:delete({Table, Key}) end,
    case mnesia:transaction(DeleteFun) of
        {atomic, ok} ->
            ok;
        {aborted, {Reason, MData}} ->
            {error, {Reason, MData}}
    end.

-spec cache_entry(table(), table_key()) -> [any()].
cache_entry(Table, Key) ->
    mnesia:dirty_read(Table, Key).

-spec cache_entry_from_index(table(), table_key(), table_key_position()) -> [any()].
cache_entry_from_index(Table, Key, IndexPosition) ->
    mnesia:dirty_index_read(Table, Key, IndexPosition).

-spec cache_last_entry(table()) -> [any()].
cache_last_entry(Table) ->
    case mnesia:dirty_first(Table) of
        '$end_of_table' ->
            [];
        Key ->
            cache_entry(Table, Key)
    end.


-spec get_ttl_and_field_index(table(), [#app_metatable{}]) -> time_to_live().
get_ttl_and_field_index(Table, Tables) -> 
    #app_metatable{time_to_live = TTL, fields = Fields} = table_info(Table, Tables),
    FieldIndex = get_index(?TIMESTAMP, Fields),
    {TTL, FieldIndex}.


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
-spec reset_scavenger(table() | [table()]) -> ok.
reset_scavenger([]) -> ok;
reset_scavenger([#app_metatable{table = Table} | Rest]) -> 
    app_cache_scavenger:reset_timer(Table),
    reset_scavenger(Rest);
reset_scavenger(#app_metatable{table = Table}) ->
    app_cache_scavenger:reset_timer(Table),
    ok.





