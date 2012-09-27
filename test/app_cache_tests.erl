%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Main module for the timer2 application.
%%% @end
%%%-------------------------------------------------------------------
-module(app_cache_tests).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

%% ------------------------------------------------------------------
%% Includes
%% ------------------------------------------------------------------

-include("../src/defaults.hrl").
-include_lib("eunit/include/eunit.hrl").


%% ------------------------------------------------------------------
%% Defines
%% ------------------------------------------------------------------

-define(setup(F), {setup, fun start/0, fun stop/1, F}).
-define(foreach(F), {foreach, fun start/0, fun stop/1, F}).

%%% Dummy data
-define(TEST_TABLE_1, test_table_1).
-define(TEST_TABLE_2, test_table_2).
-define(TABLE1, #app_metatable{
                table = test_table_1,
                version = 1,
                time_to_live = 60,
                type = ordered_set,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
-define(TABLE2, #app_metatable{
                table = test_table_2,
                version = 1,
                time_to_live = 60,
                type = bag,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
-define(TABLE3, #app_metatable{
                table = test_table_3,
                version = 1,
                time_to_live = infinity,
                type = set,
                fields = [key, value],
                secondary_index_fields = []
            }).
-define(KEY, foo).
-define(KEY2, foo2).
-define(KEY3, foo3).
-define(KEY4, foo4).
-define(VALUE, bar).
-define(VALUE2, bar2).
-define(VALUE3, bar3).
-define(VALUE4, 2).
-define(NAME, baz).
-define(NAME2, baz2).
-define(NAME3, baz3).
-define(NAME4, baz4).
-define(RECORD, {test_table_1, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD2, {test_table_1, ?KEY2, undefined, ?VALUE2, ?NAME2}).
-define(RECORD3, {test_table_1, ?KEY3, undefined, ?VALUE3, ?NAME3}).
-define(RECORD30, {test_table_2, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD31, {test_table_2, ?KEY, undefined, ?VALUE2, ?NAME2}).
-define(RECORD4, {test_table_1, ?KEY4, undefined, ?VALUE4, ?NAME4}).

%% ------------------------------------------------------------------
%% Test Function Definitions
%% ------------------------------------------------------------------

%%
%% Test Descriptions
%%

app_cache_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun(_) ->
                [
                 ?debugVal(t_set_data_with_refresh_fun1()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_refresh_fun2()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_refresh_fun3()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_refresh_fun4()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_refresh_fun5()),
                 empty_all_tables(),
                 ?debugVal(t_delete_record()),
                 empty_all_tables(),
                 ?debugVal(t_delete_record_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_delete_record_ignoring_timestamp()),
                 empty_all_tables(),
                 ?debugVal(t_delete_record_ignoring_timestamp_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_records()),
                 empty_all_tables(),
                 ?debugVal(t_get_records_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_table_info()),
                 empty_all_tables(),
                 ?debugVal(t_table_version()),
                 empty_all_tables(),
                 ?debugVal(t_table_time_to_live()),
                 empty_all_tables(),
                 ?debugVal(t_table_fields()),
                 empty_all_tables(),
                 ?debugVal(t_cache_time_to_live()),
                 empty_all_tables(),
                 ?debugVal(t_ttl_and_field_index()),
                 empty_all_tables(),
                 ?debugVal(t_update_time_to_live()),
                 empty_all_tables(),
                 ?debugVal(t_set_data()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_async_persist_fun1()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_async_persist_fun1_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_sync_persist_fun1()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_sync_persist_fun1_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_sync_persist_fun2()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_sync_persist_fun2_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_transform_fun()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_with_transform_fun_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_data_with_transform_fun()),
                 empty_all_tables(),
                 ?debugVal(t_get_data_with_transform_fun_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_data()),
                 empty_all_tables(),
                 ?debugVal(t_get_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_bag_data()),
                 empty_all_tables(),
                 ?debugVal(t_get_bag_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_overwriting_timestamp()),
                 empty_all_tables(),
                 ?debugVal(t_set_data_overwriting_timestamp_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_data_from_index()),
                 empty_all_tables(),
                 ?debugVal(t_get_data_from_index_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_last_entered_data()),
                 empty_all_tables(),
                 ?debugVal(t_get_last_entered_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_first_entered_data()),
                 empty_all_tables(),
                 ?debugVal(t_get_first_entered_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_first_n_entries()),
                 empty_all_tables(),
                 ?debugVal(t_get_first_n_entries_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_get_last_n_entries()),
                 empty_all_tables(),
                 ?debugVal(t_get_last_n_entries_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_key_exists()),
                 empty_all_tables(),
                 ?debugVal(t_key_exists_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_delete_data()),
                 empty_all_tables(),
                 ?debugVal(t_delete_data_dirty()),
                 empty_all_tables(),
                 ?debugVal(t_delete_all_data()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_create()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_create_default()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_current_value()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_current_value_0()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_current_value_1()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_next_value()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_delete()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_set_value()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_all_sequences()),
                 empty_all_tables(),
                 ?debugVal(t_cached_sequence_all_sequences_one()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_create()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_create_default()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_current_value()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_current_value_0()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_current_value_1()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_next_value()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_set_value()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_delete()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_all_sequences()),
                 empty_all_tables(),
                 ?debugVal(t_sequence_all_sequences_one()),
                 empty_all_tables(),
                 ?debugVal(t_set_and_get_data_many()),
                 empty_all_tables(),
                 ?debugVal(t_cache_expiration())] end}.

app_cache_init_table_test_() ->
    {setup,
     fun start_with_schema/0,
     fun stop/1,
     fun(_) ->
             [
              ?debugVal(t_init_metatable()),
              empty_all_tables(),
              ?debugVal(t_get_metatable()),
              empty_all_tables(),
              ?debugVal(t_init_metatable_nodes()),
              empty_all_tables(),
              ?debugVal(t_init_table()),
              empty_all_tables(),
              ?debugVal(t_init_table_nodes()),
              empty_all_tables()] end}.



%%
%% Setup Functions
%%
start() ->
    app_cache:setup(),
    app_cache:start(),
    app_cache:create_table(?TABLE1),
    app_cache:create_table(?TABLE2),
    app_cache:create_table(?TABLE3).


stop(_) ->
    app_cache:stop(),
    mnesia:delete_schema([node()]).

start_with_schema() ->
    start(),
    app_cache:stop(),
    app_cache:start().



%%
%% Helper Functions
%%

t_sequence_create() ->
    ok = app_cache:sequence_create(?KEY, 1),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 1}], MData).

t_sequence_create_default() ->
    Start = app_cache:get_env(cache_start, ?DEFAULT_CACHE_START),
    ok = app_cache:sequence_create(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = Start}], MData).

t_sequence_current_value() ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    Value = app_cache:sequence_current_value(?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual(11, Value).

t_sequence_current_value_0() ->
    ok = app_cache:sequence_create(?KEY, 1),
    Value = app_cache:sequence_current_value(?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual(1, Value).

t_sequence_current_value_1() ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    Value = app_cache:sequence_current_value(?KEY),
    % 'cos the first 'next_value' is the 'set-value'
    ok = app_cache:sequence_create(?KEY, 1),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual(11, Value).

t_sequence_next_value() ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 11}], MData).

t_sequence_set_value() ->
    ok = app_cache:sequence_create(?KEY, 1),
    ok = app_cache:sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 9999}], MData).

t_sequence_delete() ->
    ok = app_cache:sequence_create(?KEY, 1),
    app_cache:sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([], MData).

t_sequence_all_sequences() ->
    All = app_cache:sequence_all_sequences(),
    ?_assertEqual([], All).

t_sequence_all_sequences_one() ->
    ok = app_cache:sequence_create(?KEY, 1),
    All = app_cache:sequence_all_sequences(),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 1}], All).

t_cached_sequence_create() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 1 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}], MData).

t_cached_sequence_create_default() ->
    Start = app_cache:get_env(cache_start, ?DEFAULT_CACHE_START),
    ok = app_cache:cached_sequence_create(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = Start + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}], MData).


t_cached_sequence_current_value_0() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    Value = app_cache:cached_sequence_current_value(?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual(1, Value).

t_cached_sequence_current_value() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual(21, Value).

t_cached_sequence_current_value_1() ->
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual(21, Value).

t_cached_sequence_next_value() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 31}], MData).

t_cached_sequence_delete() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    app_cache:cached_sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([], MData).

t_cached_sequence_set_value() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    ok = app_cache:cached_sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    Value = 9999 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT,
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = Value}], MData).

t_cached_sequence_all_sequences() ->
    All = app_cache:cached_sequence_all_sequences(),
    ?_assertEqual([], All).

t_cached_sequence_all_sequences_one() ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    All = app_cache:cached_sequence_all_sequences(),
    app_cache:cached_sequence_delete(?KEY),
    ?_assertEqual([#sequence_cache{key =?KEY, start = 1}], All).

t_table_info() ->
    Data = app_cache:table_info(?TEST_TABLE_1),
    MTableInfo = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual([Data], MTableInfo).

t_table_version() ->
    Data = app_cache:table_version(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.version).

t_table_time_to_live() ->
    Data = app_cache:table_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.time_to_live).

t_table_fields() ->
    Data = app_cache:table_fields(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.fields).

t_cache_time_to_live() ->
    Data = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.time_to_live).

t_ttl_and_field_index() ->
    TableInfo = app_cache:table_info(?TEST_TABLE_1),
    Data = app_cache_processor:get_ttl_and_field_index(TableInfo),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, {MTableInfo#app_metatable.time_to_live, 2}).

t_update_time_to_live() ->
    TTL = 700,
    app_cache:update_table_time_to_live(?TEST_TABLE_1, TTL),
    TTL = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(TTL, MTableInfo#app_metatable.time_to_live).

t_set_data() ->
    Result = app_cache:set_data(?RECORD),
    ?_assertEqual(Result, ok).

t_set_data_dirty() ->
    Result = app_cache:set_data(dirty, ?RECORD),
    ?_assertEqual(Result, ok).

t_set_data_with_transform_fun() ->
    app_cache:set_write_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, 2 * ?VALUE4).

t_set_data_with_transform_fun_dirty() ->
    app_cache:set_write_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, 2 * ?VALUE4).

t_set_data_with_sync_persist_fun1() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, ?VALUE4).

t_set_data_with_sync_persist_fun1_dirty() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, ?VALUE4).

t_set_data_with_async_persist_fun1() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = false,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, ?VALUE4).

t_set_data_with_async_persist_fun1_dirty() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = false,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, ?VALUE4).

t_set_data_with_sync_persist_fun2() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function,
                                                                        fun(_X) -> erlang:exit(bah) end}}),
    app_cache:set_data(?RECORD4),
    Result = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, []).

t_set_data_with_sync_persist_fun2_dirty() ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function,
                                                                        fun(_X) -> erlang:exit(bah) end}}),
    app_cache:set_data(dirty, ?RECORD4),
    Result = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, []).

t_set_data_with_refresh_fun1() ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = true,
                                                                after_each_read = false,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% Before, so each read already has been refreshed
    [#test_table_1{value = ?VALUE4*2}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, ?VALUE4*4).

t_set_data_with_refresh_fun2() ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = false,
                                                                after_each_read = false,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% No refreshing going on
    [#test_table_1{value = ?VALUE4}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, ?VALUE4).

t_set_data_with_refresh_fun3() ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = false,
                                                                after_each_read = false,
                                                                refresh_interval = 5,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% After, so each read has *not* been refreshed
    [#test_table_1{value = ?VALUE4}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    timer:sleep(7000),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, ?VALUE4*2).

t_set_data_with_refresh_fun4() ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = false,
                                                                after_each_read = true,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% After, so each read gets refreshed *after* the read
    [#test_table_1{value = ?VALUE4}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    %% Sleep, to ensure no race conditions
    timer:sleep(1000),
    [#test_table_1{value = ?VALUE4*2}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    timer:sleep(1000),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, ?VALUE4*4).

t_set_data_with_refresh_fun5() ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = true,
                                                                after_each_read = true,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% Before *and* after, so each read gets refreshed before *and* after
    [#test_table_1{value = ?VALUE4*2}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    timer:sleep(1000),
    [#test_table_1{value = ?VALUE4*8}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    timer:sleep(1000),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Result, ?VALUE4*32).

t_get_data() ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_data_dirty() ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(dirty, ?TEST_TABLE_1, ?KEY),
    ?_assertEqual(?VALUE, Data#test_table_1.value).


t_get_data_with_transform_fun() ->
    app_cache:set_read_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [CleanData] = mnesia:dirty_read(?TEST_TABLE_1, ?KEY4),
    OldValue = CleanData#test_table_1.value,
    OldValue = ?VALUE4,
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, 2 * ?VALUE4).

t_get_data_with_transform_fun_dirty() ->
    app_cache:set_read_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [CleanData] = mnesia:dirty_read(?TEST_TABLE_1, ?KEY4),
    OldValue = CleanData#test_table_1.value,
    OldValue = ?VALUE4,
    [Data] = app_cache:get_data(dirty, ?TEST_TABLE_1, ?KEY4),
    ?_assertEqual(Data#test_table_1.value, 2 * ?VALUE4).

t_get_bag_data() ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual(2, length(Data)).

t_get_bag_data_dirty() ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(dirty, ?TEST_TABLE_2, ?KEY),
    ?_assertEqual(2, length(Data)).

t_set_data_overwriting_timestamp() ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data_overwriting_timestamp(?RECORD30),
    [Data] = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual({?KEY, ?VALUE, ?NAME},
                  {Data#test_table_2.key, Data#test_table_2.value, Data#test_table_2.name}).

t_set_data_overwriting_timestamp_dirty() ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data_overwriting_timestamp(dirty, ?RECORD30),
    [Data] = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual({?KEY, ?VALUE, ?NAME},
                  {Data#test_table_2.key, Data#test_table_2.value, Data#test_table_2.name}).

t_get_data_from_index() ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(?TEST_TABLE_1, ?NAME, name),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_data_from_index_dirty() ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(dirty, ?TEST_TABLE_1, ?NAME, name),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_last_entered_data() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

t_get_last_entered_data_dirty() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(dirty, ?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

t_get_first_entered_data() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_first_key(?TEST_TABLE_1),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_first_entered_data_dirty() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_first_key(dirty, ?TEST_TABLE_1),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_last_n_entries() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_last_n_entries_dirty() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(dirty, ?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_first_n_entries() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_first_n_entries_dirty() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(dirty, ?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_key_exists() ->
    false = app_cache:key_exists(?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:key_exists(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, true).

t_key_exists_dirty() ->
    false = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, true).

t_delete_data() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_data(safe, ?TEST_TABLE_1, ?KEY),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_data_dirty() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_data(dirty, ?TEST_TABLE_1, ?KEY),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_all_data() ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_all_data(?TEST_TABLE_1),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_record() ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(Record),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_record_dirty() ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(dirty, Record),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_record_ignoring_timestamp() ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    Len = length(app_cache:get_data(?TEST_TABLE_2, ?KEY)),
    app_cache:remove_record_ignoring_timestamp(?RECORD30),
    Data = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual({[], 2}, {Data, Len}).

t_delete_record_ignoring_timestamp_dirty() ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    Len = length(app_cache:get_data(?TEST_TABLE_2, ?KEY)),
    app_cache:remove_record_ignoring_timestamp(dirty, ?RECORD30),
    Data = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual({[], 2}, {Data, Len}).

t_get_records() ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    Len = length(app_cache:get_records(?RECORD30)),
    ?_assertEqual(2, Len).

t_get_records_dirty() ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    Len = length(app_cache:get_records(dirty, ?RECORD30)),
    ?_assertEqual(2, Len).

t_set_and_get_data_many() ->
    LoadFun = get_load_data_fun(100),
    LoadFun(),
    [Data] = app_cache:get_data(?TEST_TABLE_1, 35),
    ?_assertEqual(Data#test_table_1.value, {35}).

t_cache_expiration() ->
    app_cache:update_table_time_to_live(?TEST_TABLE_1, 2),
    app_cache_scavenger:reset_timer(?TEST_TABLE_1),
    timer:sleep(1000),
    LoadFun = get_load_data_fun(10),
    {_LTime, _LValue} = timer:tc(LoadFun),
    timer:sleep(10000),
    Data1 = app_cache:get_after(?TEST_TABLE_1, 0),
    ?_assertEqual(Data1, []).

t_init_table() ->
    Res = app_cache:init_table(?TEST_TABLE_1),
    ?_assertEqual(ok, Res).

t_init_table_nodes() ->
    Res = app_cache:init_table(?TEST_TABLE_2, [node()]),
    ?_assertEqual(ok, Res).

t_init_metatable() ->
    Res = app_cache:init_metatable(),
    ?_assertEqual(ok, Res).

t_init_metatable_nodes() ->
    Res = app_cache:init_metatable([node()]),
    ?_assertEqual(ok, Res).

t_get_metatable() ->
    Res = app_cache:get_metatable(),
    Metatables = [M || M = #app_metatable{} <- Res],
    ?_assert(length(Metatables) > 0).


get_load_data_fun(Count) ->
    fun() ->
            lists:map(fun(X) -> Record = #test_table_1{key = X, value = {X}}, app_cache:set_data(Record) end, lists:seq(1,Count))
    end.

empty_all_tables() ->
    lists:foreach(fun(Table) -> empty_table(Table) end, [?TEST_TABLE_1, ?TEST_TABLE_2]),
    app_cache:sequence_delete(?KEY),
    ?_assertEqual(ok, ok).

empty_table(Table) ->
    DeleteFun = fun() ->
            app_cache:set_write_transform_function(Table, undefined),
            app_cache:set_read_transform_function(Table, undefined),
            app_cache:set_persist_function(Table, #persist_data{}),
            app_cache:set_refresh_function(Table, #refresh_data{}),
            [mnesia:delete({Table, Key}) || Key <- mnesia:all_keys(Table)] end,
    mnesia:transaction(DeleteFun).

transform_fun(Record) ->
    Record#test_table_1{value = 2 * Record#test_table_1.value}.
