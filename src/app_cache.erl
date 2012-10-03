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

-compile([{parse_transform, dynarec}]).
-compile([{parse_transform, lager_transform}]).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

%% Mnesia utility APIs
-export([get_env/0, get_env/1, get_env/2]).
-export([setup/0, setup/1,
         start/0, stop/0,
         cache_init/1, cache_init/2,
         init_metatable/0, init_metatable/1, init_table/1, init_table/2,
         get_metatable/0,
         create_tables/0, create_tables/1, create_metatable/0, create_metatable/1, create_table/1, create_table/2,
         upgrade_metatable/0, upgrade_table/1, upgrade_table/2, upgrade_table/4,
         table_info/1, table_version/1, table_time_to_live/1, table_fields/1,
         update_table_time_to_live/2,
         last_update_to_datetime/1, current_time_in_gregorian_seconds/0,
         cache_time_to_live/1,
         get_record_fields/1,
         set_read_transform_function/2, set_write_transform_function/2,
         set_refresh_function/2,
         set_persist_function/2
        ]).

%% Data accessor APIs
-export([key_exists/2,
         get_data_from_index/3, get_data/2, get_all_data/1,
         get_data_by_last_key/1, get_data_by_first_key/1,
         get_last_n_entries/2, get_first_n_entries/2,
         get_records/1,
         get_after/2,
         set_data/1, set_data_overwriting_timestamp/1,
         remove_data/2, remove_all_data/1,
         remove_record/1, remove_record_ignoring_timestamp/1]).
-export([key_exists/3,
         get_data_from_index/4, get_data/3, get_all_data/2,
         get_data_by_last_key/2, get_data_by_first_key/2,
         get_records/2,
         get_after/3,
         set_data/2,set_data_overwriting_timestamp/2,
         get_last_n_entries/3, get_first_n_entries/3,
         remove_data/3, remove_all_data/2,
         remove_record/2, remove_record_ignoring_timestamp/2]).

-export([sequence_create/1, sequence_create/2,
         sequence_set_value/2,
         sequence_current_value/1, sequence_next_value/1, sequence_next_value/2,
         sequence_delete/1,
         sequence_all_sequences/0]).
-export([cached_sequence_create/1, cached_sequence_create/2, cached_sequence_create/3,
         cached_sequence_set_value/2,
         cached_sequence_current_value/1, cached_sequence_next_value/1, cached_sequence_next_value/2,
         cached_sequence_delete/1,
         cached_sequence_all_sequences/0]).


%% ------------------------------------------------------------------
%% Includes & Defines
%% ------------------------------------------------------------------

-include("defaults.hrl").

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
%%
%% Mnesia utility functions
%%
-spec setup() -> ok | {error, Reason::any()}.
%% @equiv setup([node()])
setup() ->
    setup([node()]).

-spec setup([node()]) -> ok | {error, Reason::any()}.
%% @doc Does the necessary housekeeping on these nodes to run Disc Nodes
setup(Nodes) when is_list(Nodes) ->
    mnesia:create_schema(Nodes).

-spec init_metatable() -> ok | {aborted, Reason :: any()}.
init_metatable() ->
    Nodes = get_env(cache_nodes, [node()]),
    init_metatable(Nodes).

-spec init_metatable([node()]) -> ok | {aborted, Reason :: any()}.
init_metatable(Nodes) ->
    gen_server:call(?PROCESSOR, {init_metatable, Nodes}).

-spec get_metatable() -> [#app_metatable{}] | {aborted, Reason :: any()}.
get_metatable() ->
    gen_server:call(?PROCESSOR, {get_metatable}).

-spec cache_init([#app_metatable{}]) -> ok | error().
cache_init(Tables) ->
    Nodes = get_env(cache_nodes, [node()]),
    cache_init(Nodes, Tables).

-spec cache_init([node()], [#app_metatable{}]) -> ok | error().
cache_init(Nodes, Tables) ->
    gen_server:call(?PROCESSOR, {cache_init, Nodes, Tables}).


-spec create_tables() -> ok | {aborted, Reason :: any()}.
create_tables() ->
    Nodes = get_env(cache_nodes, [node()]),
    create_tables(Nodes).

-spec create_tables([node()]) -> ok | {aborted, Reason :: any()}.
create_tables(Nodes) ->
    gen_server:call(?PROCESSOR, {create_tables, Nodes}).


create_metatable() ->
    Nodes = get_env(cache_nodes, [node()]),
    create_metatable(Nodes).

-spec create_metatable([node()]) -> ok.
create_metatable(Nodes) ->
    app_cache_processor:create_metatable(Nodes).

-spec init_table(table()) -> ok | {aborted, Reason :: any()}.
init_table(Table) ->
    Nodes = get_env(cache_nodes, [node()]),
    init_table(Table, Nodes).

-spec init_table(table(), [node()]) -> ok | {aborted, Reason :: any()}.
init_table(Table, Nodes) ->
    gen_server:call(?PROCESSOR, {init_table, Table, Nodes}).

-spec create_table(#app_metatable{}) -> ok | {aborted, Reason :: any()}.
create_table(TableInfo) ->
    Nodes = get_env(cache_nodes, [node()]),
    create_table(TableInfo, Nodes).

-spec create_table(#app_metatable{}, [node()]) -> ok | {aborted, Reason :: any()}.
create_table(TableInfo, Nodes) ->
    gen_server:call(?PROCESSOR, {create_table, TableInfo, Nodes}).

% TODO make this dependant on the node
-spec upgrade_metatable() -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_metatable() ->
    app_cache_processor:upgrade_metatable().

-spec upgrade_table(table()) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table) ->
    app_cache_processor:upgrade_table(Table).


-spec upgrade_table(table(), [app_field()]) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table, Fields) ->
    app_cache_processor:upgrade_table(Table, Fields).

-spec upgrade_table(table(), OldVersion :: non_neg_integer(), NewVersion :: non_neg_integer(), [app_field()]) -> {atomic, ok} | {aborted, Reason :: any()}.
upgrade_table(Table, OldVersion, NewVersion, Fields) ->
    app_cache_processor:upgrade_table(Table, OldVersion, NewVersion, Fields).

-spec table_info(table()) -> #app_metatable{} | undefined.
table_info(Table) ->
    app_cache_processor:table_info(Table).

-spec table_version(table()) -> table_version().
table_version(Table) ->
    gen_server:call(?PROCESSOR, {table_version, Table}).

-spec table_time_to_live(table()) -> time_to_live().
table_time_to_live(Table) ->
    gen_server:call(?PROCESSOR, {table_time_to_live, Table}).

-spec table_fields(table()) -> table_fields().
table_fields(Table) ->
    app_cache_processor:table_fields(Table).

-spec current_time_in_gregorian_seconds() -> non_neg_integer().
current_time_in_gregorian_seconds() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

-spec cache_time_to_live(table()) -> time_to_live().
cache_time_to_live(Table) ->
    TableInfo = table_info(Table),
    case app_cache_processor:get_ttl_and_field_index(TableInfo) of
        {error, _} = Error ->
            Error;
        {TTL, _} ->
            TTL
    end.

-spec last_update_to_datetime(last_update()) -> calendar:datetime().
last_update_to_datetime(LastUpdate) ->
    calendar:gregorian_seconds_to_datetime(LastUpdate).

-spec update_table_time_to_live(table(), time_to_live()) -> {timestamp(), table_key_position()}.
update_table_time_to_live(Table, TTL) ->
    gen_server:call(?PROCESSOR, {update_table_time_to_live, Table, TTL}).

%%
%% Table accessors
%%
-spec key_exists(Table::table(), Key::table_key()) -> boolean().
%% @equiv key_exists(safe, Table, Key)
key_exists(Table, Key) ->
    key_exists(?TRANSACTION_TYPE_SAFE, Table, Key).

-spec key_exists(transaction_type(), Table::table(), Key::table_key()) -> boolean().
%% @doc Check to see if a record w/ key Key exists in Table
key_exists(TransactionType, Table, Key) ->
    app_cache_processor:check_key_exists(TransactionType, Table, Key).

-spec get_data(Table::table(), Key::table_key()) -> [any()].
%% @equiv get_data(safe, Table, Key)
get_data(Table, Key) ->
    get_data(?TRANSACTION_TYPE_SAFE, Table, Key).

-spec get_data(transaction_type(), Table::table(), Key::table_key()) -> [any()].
%% @doc Get all the records from the Table with the key Key
%% @end
get_data(TransactionType, Table, Key) ->
    app_cache_processor:read_data(TransactionType, Table, Key).

-spec get_data_from_index(Table::table(), Value::table_key(), IndexField::table_key()) -> [any()].
%% @equiv get_data_from_index(safe, Table, Value, IndexField)
get_data_from_index(Table, Value, IndexField) ->
    get_data_from_index(?TRANSACTION_TYPE_SAFE, Table, Value, IndexField).

-spec get_data_from_index(transaction_type(), Table::table(), Value::table_key(), IndexField::table_key()) -> [any()].
%% @doc Get all the records from the Table where the Value matches the value
%%      in field IndexField
%%      <p>e.g. get_data_from_index(test_table_1, "some thing here", value)</p>
%%      <p>where 'value' is an indexed field in test_table_1</p>
%% @end
get_data_from_index(TransactionType, Table, Value, IndexField) ->
    app_cache_processor:read_data_from_index(TransactionType, Table, Value, IndexField).

-spec get_data_by_last_key(Table::table()) -> [any()].
%% @equiv get_data_by_last_key(safe, Table)
get_data_by_last_key(Table) ->
    get_data_by_last_key(?TRANSACTION_TYPE_SAFE, Table).

-spec get_data_by_last_key(transaction_type(), Table::table()) -> [any()].
%% @doc Get the last item (in erlang term order) in Table.
%%      Performant on <i>ordered_set</i>s
%% @end
get_data_by_last_key(TransactionType, Table) ->
    app_cache_processor:read_data_by_last_key(TransactionType, Table).

-spec get_data_by_first_key(Table::table()) -> [any()].
%% @equiv get_data_by_first_key(safe, Table)
get_data_by_first_key(Table) ->
    get_data_by_first_key(?TRANSACTION_TYPE_SAFE, Table).

-spec get_data_by_first_key(transaction_type(), Table::table()) -> [any()].
%% @doc Get the first item (in erlang term order) in Table.
%%      Performant on <i>ordered_set</i>s
%% @end
get_data_by_first_key(TransactionType, Table) ->
    app_cache_processor:read_data_by_first_key(TransactionType, Table).

-spec get_last_n_entries(Table::table(), pos_integer()) -> [any()].
%% @equiv get_last_n_entries(safe, Table, N)
get_last_n_entries(Table, N) ->
    get_last_n_entries(?TRANSACTION_TYPE_SAFE, Table, N).

-spec get_last_n_entries(transaction_type(), Table::table(), pos_integer()) -> [any()].
%% @doc Get the last N entries (in erlang term order) in Table
%%      <p>Performant on <i>ordered_set</i>s</p>
%% @end
get_last_n_entries(TransactionType, Table, N) ->
    app_cache_processor:read_last_n_entries(TransactionType, Table, N).

-spec get_first_n_entries(Table::table(), pos_integer()) -> [any()].
%% @equiv get_first_n_entries(safe, Table, N)
get_first_n_entries(Table, N) ->
    get_first_n_entries(?TRANSACTION_TYPE_SAFE, Table, N).

-spec get_first_n_entries(transaction_type(), Table::table(), pos_integer()) -> [any()].
%% @doc Get the first N entries in the table. <i>First</i> is in erlang term
%%      order.
%%      <p>Performant on <i>ordered_set</i>s, requires table scans for all other table types</p>
%% @end
get_first_n_entries(TransactionType, Table, N) ->
    app_cache_processor:read_first_n_entries(TransactionType, Table, N).

-spec get_after(table(), table_key()) -> [any()].
%% @equiv get_after(safe, Table, After)
get_after(Table, After) ->
    get_after(?TRANSACTION_TYPE_SAFE, Table, After).

%% Get data after (in erlang term order) a value
-spec get_after(transaction_type(), table(), table_key()) -> [any()].
%% @doc Get all the entries in a table greater than or equal to the Key "After". Keys are sorted in
%%      erlang term order.
%%      <p>Performant on <i>ordered_set</i>s, requires table scans for all other table types</p>
%% @end
get_after(TransactionType, Table, After) ->
    app_cache_processor:read_after(TransactionType, Table, After).

-spec get_records(tuple()) -> [any()].
%% @equiv get_records(safe, Record)
get_records(Record) ->
    get_records(?TRANSACTION_TYPE_SAFE, Record).

-spec get_records(transaction_type(), tuple()) -> [any()].
%% @doc Get any items in the table that (exactly) match Record
%%      <p>This is of particular use for bags with <i>timestamp</i> fields. If
%%      you pass in a record with <i>timestamp =:= undefined</i>, you will get
%%      back all the records that match regardless of the timestamp</p>
%% @end
get_records(TransactionType, Record) ->
    app_cache_processor:read_records(TransactionType, Record).

%% Get all data in table
-spec get_all_data(table()) -> [any()].
%% @equiv get_all_data(safe, Table)
get_all_data(Table) ->
    get_all_data(?TRANSACTION_TYPE_SAFE, Table).

-spec get_all_data(transaction_type(), table()) -> [any()].
%% @doc Get all the data in the table
get_all_data(TransactionType, Table) ->
    app_cache_processor:read_all_data(TransactionType, Table).

%% Write the record
-spec set_data(Value::tuple()) -> ok | error().
%% @equiv set_data(safe, Value)
set_data(Value) ->
    set_data(?TRANSACTION_TYPE_SAFE, Value).

-spec set_data(transaction_type(), Value::tuple()) -> ok | error().
%% @doc Write the record "Value" to the table
%%      <p>The table name is element(1, Value)</p>
%% @end
set_data(TransactionType, Value) ->
    app_cache_processor:write_data(TransactionType, Value).

-spec set_data_overwriting_timestamp(Value::tuple()) -> ok | error().
%% @equiv set_data_overwriting_timestamp(safe, Value)
set_data_overwriting_timestamp(Value) ->
    set_data_overwriting_timestamp(?TRANSACTION_TYPE_SAFE, Value).

-spec set_data_overwriting_timestamp(transaction_type(), Value::tuple()) -> ok | error().
%% @doc If your table is a <i>bag</i> and contains a <i>timestamp</i> field,
%%      but you actually don't care about the timestamp in your
%%      records, then <i>set_data</i> will create new records each time you write.
%%      <p> This is something you probably won't appreciate</p>
%%      <p><i>set_data_overwriting_timestamp</i> will delete any existing record w/ the
%%      same fields (ignoring timestamp, of course), thus ensuring that the timestamp
%%      field doesn't cause spurious writes</p>
%% @end
set_data_overwriting_timestamp(TransactionType, Value) ->
    app_cache_processor:write_data_overwriting_timestamp(TransactionType, Value).

-spec remove_data(Table::table(), Key::table_key()) -> ok | error().
%% @equiv remove_data(safe, Table, Key)
remove_data(Table, Key) ->
    remove_data(?TRANSACTION_TYPE_SAFE, Table, Key).

-spec remove_data(transaction_type(), Table::table(), Key::table_key()) -> ok | error().
%% @doc Remove (all) the record(s) with key Key in Table
remove_data(TransactionType, Table, Key) ->
    app_cache_processor:delete_data(TransactionType, Table, Key).

-spec remove_all_data(Table::table()) -> ok | error().
%% @equiv remove_all_data(safe, Table)
remove_all_data(Table) ->
    remove_all_data(?TRANSACTION_TYPE_SAFE, Table).

-spec remove_all_data(transaction_type(), Table::table()) -> ok | error().
%% @doc Remove <i>all</i> the data in Table
remove_all_data(TransactionType, Table) ->
    app_cache_processor:delete_all_data(TransactionType, Table).

-spec remove_record(tuple()) -> ok | error().
%% @equiv remove_record(safe, Record)
remove_record(Record) ->
    remove_record(?TRANSACTION_TYPE_SAFE, Record).

-spec remove_record(transaction_type(), tuple()) -> ok | error().
%% @doc Remove the record Record.
%%      <p>Note that <i>all</i> fields need to match, including <i>timestamp</i></p>
remove_record(TransactionType, Record) ->
    app_cache_processor:delete_record(TransactionType, _IgnoreTimestamp = false, Record).

-spec remove_record_ignoring_timestamp(tuple()) -> ok | error().
%% @equiv remove_record_ignoring_timestamp(safe, Record)
remove_record_ignoring_timestamp(Record) ->
    remove_record_ignoring_timestamp(?TRANSACTION_TYPE_SAFE, Record).

-spec remove_record_ignoring_timestamp(transaction_type(), tuple()) -> ok | error().
%% @doc Remove the record Record ignoring any existing timestamp field.
%%      <p>The difference between this and <i>remove_record</i> is that if the
%%      record contains a <i>timestamp</i> field, it is ignored.</p>
%%      <p>This is of use w/ bags where you might have the same record w/
%%      multiple timestamps, and you want them all gone</p>
remove_record_ignoring_timestamp(TransactionType, Record) ->
    app_cache_processor:delete_record(TransactionType, _IgnoreTimestamp = true, Record).

-spec sequence_create(sequence_key()) -> ok.
%% @equiv sequence_create(Key, 1)
sequence_create(Key) ->
    Start = get_env(cache_start, ?DEFAULT_CACHE_START),
    sequence_create(Key, Start).

-spec sequence_create(sequence_key(), sequence_value()) -> ok.
%% @doc Create a sequence identified by Key, starting at Start
sequence_create(Key, Start) when Start >= 0 ->
    set_data(?TRANSACTION_TYPE_SAFE, {?SEQUENCE_TABLE, Key, Start}).

-spec sequence_set_value(sequence_key(), sequence_value()) -> ok.
%% @doc Set the value of the sequence identified by Key to Start
sequence_set_value(Key, Start) when Start >= 0 ->
    sequence_create(Key, Start).

-spec sequence_current_value(sequence_key()) -> sequence_value().
%% @doc Get the current value of the sequence identified by Key
sequence_current_value(Key) ->
    Data = get_data(?TRANSACTION_TYPE_SAFE, ?SEQUENCE_TABLE, Key),
    case get_sequence_value(Data) of
        undefined ->
            sequence_create(Key),
            ?DEFAULT_CACHE_START;
        Value ->
            Value
    end.

-spec sequence_next_value(sequence_key()) -> sequence_value().
%% @equiv sequence_next_value(Key, 1)
sequence_next_value(Key) ->
    Increment = get_env(cache_increment, ?DEFAULT_CACHE_INCREMENT),
    sequence_next_value(Key, Increment).

-spec sequence_next_value(sequence_key(), sequence_value()) -> sequence_value().
%% @doc Get the next value of the sequence identified by Key incremented by Increment
sequence_next_value(Key, Increment) when is_integer(Increment) ->
    app_cache_processor:increment_data(?SEQUENCE_TABLE, Key, Increment).


-spec sequence_delete(sequence_key()) -> ok.
%% @doc Remove the sequence identified by Key
sequence_delete(Key) ->
    remove_data(?TRANSACTION_TYPE_SAFE, ?SEQUENCE_TABLE, Key).

-spec cached_sequence_create(sequence_key()) -> ok.
%% @equiv cached_sequence_create(Key, 1, 10)
cached_sequence_create(Key) ->
    Start = get_env(cache_start, ?DEFAULT_CACHE_START),
    cached_sequence_create(Key, Start).

-spec cached_sequence_create(sequence_key(), sequence_value()) -> ok.
%% @equiv cached_sequence_create(Key, Start, 10)
cached_sequence_create(Key, Start) when Start >= 0 ->
    UpperBoundIncrement = get_env(cache_upper_bound_increment, ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT),
    cached_sequence_create(Key, Start, UpperBoundIncrement).

-spec cached_sequence_create(sequence_key(), sequence_value(), sequence_value()) -> ok.
%% @doc Create a cached sequence identified by Key, starting at Start, returning
%%      UpperBoundIncrement values at a time
cached_sequence_create(Key, Start, UpperBoundIncrement) when Start >= 0, UpperBoundIncrement >= 0 ->
    gen_server:call(?SEQUENCE_CACHE, {create, Key, Start, UpperBoundIncrement}).

-spec cached_sequence_set_value(sequence_key(), sequence_value()) -> ok.
%% @doc Set the cached sequence identified by Key to value Start
cached_sequence_set_value(Key, Start) when Start >= 0 ->
    gen_server:call(?SEQUENCE_CACHE, {set_value, Key, Start}).

-spec cached_sequence_current_value(sequence_key()) -> sequence_value().
%% @doc Get the current value of the cached sequence identified by Key
cached_sequence_current_value(Key) ->
    gen_server:call(?SEQUENCE_CACHE, {current_value, Key}).

-spec cached_sequence_next_value(sequence_key()) -> sequence_value().
%% @equiv cached_sequence_next_value(Key, 1)
cached_sequence_next_value(Key) ->
    Increment = get_env(cache_increment, ?DEFAULT_CACHE_INCREMENT),
    cached_sequence_next_value(Key, Increment).

-spec cached_sequence_next_value(sequence_key(), sequence_value()) -> sequence_value().
%% @doc Get the next value of the cached sequence identified by Key
cached_sequence_next_value(Key, Increment) when is_integer(Increment) ->
    gen_server:call(?SEQUENCE_CACHE, {next_value, Key, Increment}).

-spec cached_sequence_delete(sequence_key()) -> ok.
%% @doc Remove the cached sequence identified by Key
cached_sequence_delete(Key) ->
    gen_server:call(?SEQUENCE_CACHE, {delete, Key}).

-spec sequence_all_sequences() -> [#sequence_cache{}].
%% @equiv cached_sequence_all_sequences()
sequence_all_sequences() ->
    app_cache_processor:read_all_data(?TRANSACTION_TYPE_SAFE, ?SEQUENCE_TABLE).

-spec cached_sequence_all_sequences() -> [#sequence_cache{}].
%% @doc Returns the list of all the sequences known to app_cache
cached_sequence_all_sequences() ->
    gen_server:call(?SEQUENCE_CACHE, {all_sequences}).

%% Secondary functions
-spec set_read_transform_function(table(), function_identifier()) -> ok | error().
set_read_transform_function(Table, FunctionIdentifier) ->
    gen_server:call(?PROCESSOR, {set_read_transform_function, Table, FunctionIdentifier}).

-spec set_write_transform_function(table(), function_identifier()) -> ok | error().
set_write_transform_function(Table, FunctionIdentifier) ->
    gen_server:call(?PROCESSOR, {set_write_transform_function, Table, FunctionIdentifier}).

-spec set_refresh_function(table(), #refresh_data{}) -> ok | error().
set_refresh_function(Table, RefreshData) when is_record(RefreshData, refresh_data)->
    gen_server:call(?PROCESSOR, {set_refresh_function, Table, RefreshData});
set_refresh_function(Table, RefreshData) ->
    {error, {?INVALID_REFRESH_FUNCTION, {Table, RefreshData}}}.

-spec set_persist_function(table(), #persist_data{}) -> ok | error().
set_persist_function(Table, PersistData) when is_record(PersistData, persist_data) ->
    gen_server:call(?PROCESSOR, {set_persist_function, Table, PersistData});
set_persist_function(Table, PersistData) ->
    {error, {?INVALID_PERSIST_FUNCTION, {Table, PersistData}}}.

-spec get_record_fields(table_key()) -> list().
get_record_fields(RecordName) ->
    fields(RecordName).

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
%% Application utility functions
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



%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
-spec get_sequence_value([{sequence_key(), sequence_value()}]) -> sequence_value() | undefined.
get_sequence_value([]) ->
    undefined;
get_sequence_value([{_Table, _Key, Value}]) ->
    Value;
get_sequence_value(_) ->
    undefined.
