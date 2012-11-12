-module(app_cache_SUITE).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Paul Oliver <puzza007@gmail.com>').

%% Note: This directive should only be used in test suites.
-compile(export_all).

-include("../src/defaults.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

%% ------------------------------------------------------------------
%% Defines
%% ------------------------------------------------------------------

-define(PROPTEST(A), true = proper:quickcheck(A())).
-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).

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

suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap,{minutes,1}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(start_with_schema, Config) ->
    start_with_schema(),
    Config;
init_per_group(_, Config) ->
    start(),
    Config.

end_per_group(_GroupName, _Config) ->
    stop(),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(sequence, _Config) ->
    app_cache:sequence_delete(?KEY);
end_per_testcase(cached_sequence, _Config) ->
    app_cache:cached_sequence_delete(?KEY);
end_per_testcase(_TestCase, _Config) ->
    empty_all_tables().

groups() ->
    [{sequence, [],
      [t_prop_sequence_create,
       t_sequence_create_default,
       t_prop_sequence_current_value,
       t_sequence_current_value_0,
       t_sequence_current_value_1,
       t_sequence_next_value,
       t_sequence_set_value,
       t_sequence_delete,
       t_sequence_all_sequences,
       t_sequence_all_sequences_one,
       t_sequence_statem]},

     {cached_sequence, [],
      [t_cached_sequence_create,
       t_cached_sequence_create_default,
       t_cached_sequence_current_value_0,
       t_cached_sequence_current_value,
       t_cached_sequence_current_value_1,
       t_cached_sequence_next_value,
       t_cached_sequence_delete,
       t_cached_sequence_set_value,
       t_cached_sequence_all_sequences,
       t_cached_sequence_all_sequences_one,
       t_cached_sequence_statem
      ]},

    {start_with_schema, [],
     [t_init_table,
      t_init_table_nodes,
      t_init_metatable,
      t_init_metatable_nodes,
      t_get_metatable]},

    {crud, [],
     [t_table_info,
      t_table_version,
      t_table_time_to_live,
      t_table_fields,
      t_cache_time_to_live,
      t_cache_time_to_live_bad_table,
      t_ttl_and_field_index,
      t_update_time_to_live,
      t_set_data,
      t_set_data_dirty,
      t_set_data_with_transform_fun,
      t_set_data_with_transform_fun_dirty,
      t_set_data_with_sync_persist_fun1,
      t_set_data_with_sync_persist_fun1_dirty,
      t_set_data_with_async_persist_fun1,
      t_set_data_with_async_persist_fun1_dirty,
      t_set_data_with_sync_persist_fun2,
      t_set_data_with_sync_persist_fun2_dirty,
      t_set_data_with_refresh_fun1,
      t_set_data_with_refresh_fun2,
      t_set_data_with_refresh_fun3,
      t_set_data_with_refresh_fun4,
      t_set_data_with_refresh_fun5,
      t_set_refresh_function_bad,
      t_set_persist_function_bad,
      t_get_functions,
      t_get_data,
      t_get_data_dirty,
      t_get_data_with_transform_fun,
      t_get_data_with_transform_fun_dirty,
      t_get_bag_data,
      t_get_bag_data_dirty,
      t_set_data_overwriting_timestamp,
      t_set_data_overwriting_timestamp_dirty,
      t_get_data_from_index,
      t_get_data_from_index_dirty,
      t_get_last_entered_data,
      t_get_last_entered_data_dirty,
      t_get_first_entered_data,
      t_get_first_entered_data_dirty,
      t_get_last_n_entries,
      t_get_last_n_entries_dirty,
      t_get_first_n_entries,
      t_get_first_n_entries_dirty,
      t_key_exists,
      t_key_exists_dirty,
      t_delete_data,
      t_delete_data_dirty,
      t_delete_all_data,
      t_delete_record,
      t_delete_record_dirty,
      t_delete_record_ignoring_timestamp,
      t_delete_record_ignoring_timestamp_dirty,
      t_get_records,
      t_get_records_dirty,
      t_set_and_get_data_many,
      t_cache_expiration]}].

all() ->
    [{group, sequence},
     {group, cached_sequence},
     {group, start_with_schema},
     {group, crud},
     t_app_cache_last_update_to_datetime_test].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

t_prop_sequence_create(_) ->
    ?PROPTEST(prop_sequence_create).

prop_sequence_create() ->
    ?FORALL({Key, Start}, {any(), sequence_value()},
            begin
                ok = app_cache:sequence_create(Key, Start),
                MData = mnesia:dirty_read(sequence_table, Key),
                app_cache:sequence_delete(Key),
                [#sequence_table{key = Key, value = Start}] =:= MData
            end).

t_sequence_create_default(_) ->
    Start = app_cache:get_env(cache_start, ?DEFAULT_CACHE_START),
    ok = app_cache:sequence_create(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [#sequence_table{key = ?KEY, value = Start}] =:= MData.

t_prop_sequence_current_value(_) ->
    ?PROPTEST(prop_sequence_current_value).

prop_sequence_current_value() ->
    ?FORALL({Key, Start, Num}, {any(), sequence_value(), integer(0, 500)},
            begin
                ok = app_cache:sequence_create(Key, Start),
                lists:foreach(fun(_X) -> app_cache:sequence_next_value(Key) end,
                              lists:seq(1, Num)),
                Value = app_cache:sequence_current_value(Key),
                app_cache:sequence_delete(Key),
                Value =:= (Start + Num)
            end).

t_sequence_current_value_0(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    Value = app_cache:sequence_current_value(?KEY),
    1 = Value.

t_sequence_current_value_1(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    Value = app_cache:sequence_current_value(?KEY),
    % 'cos the first 'next_value' is the 'set-value'
    ok = app_cache:sequence_create(?KEY, 1),
    11 = Value.

t_sequence_next_value(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [#sequence_table{key =?KEY, value = 11}] = MData.

t_sequence_set_value(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    ok = app_cache:sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [#sequence_table{key =?KEY, value = 9999}] = MData.

t_sequence_delete(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    app_cache:sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [] = MData.

t_sequence_all_sequences(_) ->
    [] = app_cache:sequence_all_sequences().

t_sequence_all_sequences_one(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    All = app_cache:sequence_all_sequences(),
    [#sequence_table{key =?KEY, value = 1}] = All.

t_sequence_statem(_) ->
    ?PROPTEST(app_cache_sequence_proper, prop_sequence).

t_cached_sequence_create(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    [#sequence_table{key =?KEY, value = 1 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}] = MData.

t_cached_sequence_create_default(_) ->
    Start = app_cache:get_env(cache_start, ?DEFAULT_CACHE_START),
    ok = app_cache:cached_sequence_create(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    MData = [#sequence_table{key =?KEY, value = Start + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}].

t_cached_sequence_current_value_0(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    Value = app_cache:cached_sequence_current_value(?KEY),
    app_cache:cached_sequence_delete(?KEY),
    1 = Value.

t_cached_sequence_current_value(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    21 = Value.

t_cached_sequence_current_value_1(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    app_cache:cached_sequence_delete(?KEY),
    21 = Value.

t_cached_sequence_next_value(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    [#sequence_table{key =?KEY, value = 31}] = MData.

t_cached_sequence_delete(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    app_cache:cached_sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    app_cache:cached_sequence_delete(?KEY),
    [] = MData.

t_cached_sequence_set_value(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    ok = app_cache:cached_sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    Value = 9999 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT,
    app_cache:cached_sequence_delete(?KEY),
    [#sequence_table{key =?KEY, value = Value}] = MData.

t_cached_sequence_all_sequences(_) ->
    [] = app_cache:cached_sequence_all_sequences().

t_cached_sequence_all_sequences_one(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    All = app_cache:cached_sequence_all_sequences(),
    app_cache:cached_sequence_delete(?KEY),
    [#sequence_cache{key =?KEY, start = 1}] = All.

t_cached_sequence_statem(_) ->
    ?PROPTEST(app_cache_cached_sequence_proper, prop_sequence).

t_table_info(_) ->
    Data = app_cache:table_info(?TEST_TABLE_1),
    MTableInfo = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    [Data] = MTableInfo.

t_table_version(_) ->
    Data = app_cache:table_version(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    Data = MTableInfo#app_metatable.version.

t_table_time_to_live(_) ->
    Data = app_cache:table_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    Data = MTableInfo#app_metatable.time_to_live.

t_table_fields(_) ->
    Data = app_cache:table_fields(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    Data = MTableInfo#app_metatable.fields.

t_cache_time_to_live(_) ->
    Data = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    Data = MTableInfo#app_metatable.time_to_live.

t_cache_time_to_live_bad_table(_) ->
    {error, _} = app_cache:cache_time_to_live(bad_table).

t_ttl_and_field_index(_) ->
    TableInfo = app_cache:table_info(?TEST_TABLE_1),
    Data = app_cache_processor:get_ttl_and_field_index(TableInfo),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    Data = {MTableInfo#app_metatable.time_to_live, 2}.

t_update_time_to_live(_) ->
    TTL = 700,
    app_cache:update_table_time_to_live(?TEST_TABLE_1, TTL),
    TTL = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    TTL = MTableInfo#app_metatable.time_to_live.

t_set_data(_) ->
    ok = app_cache:set_data(?RECORD).

t_set_data_dirty(_) ->
    ok = app_cache:set_data(dirty, ?RECORD).

t_set_data_with_transform_fun(_) ->
    app_cache:set_write_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    2 * ?VALUE4 = Data#test_table_1.value.

t_set_data_with_transform_fun_dirty(_) ->
    app_cache:set_write_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    2 * ?VALUE4 = Data#test_table_1.value.

t_set_data_with_sync_persist_fun1(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?VALUE4 = Data#test_table_1.value.

t_set_data_with_sync_persist_fun1_dirty(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?VALUE4 = Data#test_table_1.value.

t_set_data_with_async_persist_fun1(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = false,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?VALUE4 = Data#test_table_1.value.

t_set_data_with_async_persist_fun1_dirty(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = false,
                                                 function_identifier = {function, fun transform_fun/1}}),
    app_cache:set_data(dirty, ?RECORD4),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    ?VALUE4 = Data#test_table_1.value.

t_set_data_with_sync_persist_fun2(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function,
                                                                        fun(_X) -> erlang:exit(bah) end}}),
    app_cache:set_data(?RECORD4),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY4).

t_set_data_with_sync_persist_fun2_dirty(_) ->
    app_cache:set_persist_function(?TEST_TABLE_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = {function,
                                                                        fun(_X) -> erlang:exit(bah) end}}),
    app_cache:set_data(dirty, ?RECORD4),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY4).

t_set_data_with_refresh_fun1(_) ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = true,
                                                                after_each_read = false,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% Before, so each read already has been refreshed
    [#test_table_1{value = ?VALUE4*2}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    Result = ?VALUE4*4.

t_set_data_with_refresh_fun2(_) ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = false,
                                                                after_each_read = false,
                                                                refresh_interval = ?INFINITY,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% No refreshing going on
    [#test_table_1{value = ?VALUE4}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    Result = ?VALUE4.

t_set_data_with_refresh_fun3(_) ->
    app_cache:set_refresh_function(?TEST_TABLE_1, #refresh_data{before_each_read = false,
                                                                after_each_read = false,
                                                                refresh_interval = 5,
                                                                function_identifier = {module_and_function, {app_cache_refresher, double_value}}}),
    app_cache:set_data(safe, ?RECORD4),
    %% After, so each read has *not* been refreshed
    [#test_table_1{value = ?VALUE4}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    timer:sleep(7000),
    [#test_table_1{value = Result}] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    Result = ?VALUE4*2.

t_set_data_with_refresh_fun4(_) ->
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
    Result = ?VALUE4*4.

t_set_data_with_refresh_fun5(_) ->
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
    Result = ?VALUE4*32.

t_set_refresh_function_bad(_) ->
    Res = app_cache:set_refresh_function(?TEST_TABLE_1, bad_function),
    Res = {error, {?INVALID_REFRESH_FUNCTION, {?TEST_TABLE_1, bad_function}}}.

t_set_persist_function_bad(_) ->
    Res = app_cache:set_persist_function(?TEST_TABLE_1, bad_function),
    Res = {error, {?INVALID_PERSIST_FUNCTION, {?TEST_TABLE_1, bad_function}}}.

t_get_functions(_) ->
    ReadTransformFun = WriteTransformFun = {function, fun transform_fun/1},
    RefreshData = #refresh_data{before_each_read = true,
                               after_each_read = false,
                               refresh_interval = ?INFINITY,
                               function_identifier = {module_and_function, {app_cache_refresher, double_value}}},
    PersistData = #persist_data{synchronous = true,
                                function_identifier = {function, fun transform_fun/1}},
    app_cache:set_read_transform_function(?TEST_TABLE_1, ReadTransformFun),
    app_cache:set_write_transform_function(?TEST_TABLE_1, WriteTransformFun),
    app_cache:set_refresh_function(?TEST_TABLE_1, RefreshData),
    app_cache:set_persist_function(?TEST_TABLE_1, PersistData),
    #data_functions{read_transform_function = ReadTransformFun,
                   write_transform_function = WriteTransformFun,
                   refresh_function = RefreshData,
                   persist_function = PersistData} = app_cache_processor:get_functions(?TEST_TABLE_1).

t_get_data(_) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?VALUE = Data#test_table_1.value.

t_get_data_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(dirty, ?TEST_TABLE_1, ?KEY),
    ?VALUE = Data#test_table_1.value.

t_get_data_with_transform_fun(_) ->
    app_cache:set_read_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [CleanData] = mnesia:dirty_read(?TEST_TABLE_1, ?KEY4),
    OldValue = CleanData#test_table_1.value,
    OldValue = ?VALUE4,
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY4),
    2 * ?VALUE4 = Data#test_table_1.value.

t_get_data_with_transform_fun_dirty(_) ->
    app_cache:set_read_transform_function(?TEST_TABLE_1, {function, fun transform_fun/1}),
    app_cache:set_data(?RECORD4),
    [CleanData] = mnesia:dirty_read(?TEST_TABLE_1, ?KEY4),
    OldValue = CleanData#test_table_1.value,
    OldValue = ?VALUE4,
    [Data] = app_cache:get_data(dirty, ?TEST_TABLE_1, ?KEY4),
    2 * ?VALUE4 = Data#test_table_1.value.

t_get_bag_data(_) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    2 = length(Data).

t_get_bag_data_dirty(_) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(dirty, ?TEST_TABLE_2, ?KEY),
    2 = length(Data).

t_set_data_overwriting_timestamp(_) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data_overwriting_timestamp(?RECORD30),
    [Data] = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    {?KEY, ?VALUE, ?NAME} =
        {Data#test_table_2.key, Data#test_table_2.value, Data#test_table_2.name}.

t_set_data_overwriting_timestamp_dirty(_) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data_overwriting_timestamp(dirty, ?RECORD30),
    [Data] = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    {?KEY, ?VALUE, ?NAME} =
        {Data#test_table_2.key, Data#test_table_2.value, Data#test_table_2.name}.

t_get_data_from_index(_) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(?TEST_TABLE_1, ?NAME, name),
    ?VALUE = Data#test_table_1.value.

t_get_data_from_index_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(dirty, ?TEST_TABLE_1, ?NAME, name),
    ?VALUE = Data#test_table_1.value.

t_get_last_entered_data(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(?TEST_TABLE_1),
    ?VALUE2 = Data#test_table_1.value.

t_get_last_entered_data_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(dirty, ?TEST_TABLE_1),
    ?VALUE2 = Data#test_table_1.value.

t_get_first_entered_data(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_first_key(?TEST_TABLE_1),
    ?VALUE = Data#test_table_1.value.

t_get_first_entered_data_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_first_key(dirty, ?TEST_TABLE_1),
    ?VALUE = Data#test_table_1.value.

t_get_last_n_entries(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(?TEST_TABLE_1, 2),
    ?VALUE2 = T#test_table_1.value.

t_get_last_n_entries_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(dirty, ?TEST_TABLE_1, 2),
    ?VALUE2 = T#test_table_1.value.

t_get_first_n_entries(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(?TEST_TABLE_1, 2),
    ?VALUE2 = T#test_table_1.value.

t_get_first_n_entries_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(dirty, ?TEST_TABLE_1, 2),
     ?VALUE2 = T#test_table_1.value.

t_key_exists(_) ->
    false = app_cache:key_exists(?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    true = app_cache:key_exists(?TEST_TABLE_1, ?KEY).

t_key_exists_dirty(_) ->
    false = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    true = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY).

t_delete_data(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_data(safe, ?TEST_TABLE_1, ?KEY),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY).

t_delete_data_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_data(dirty, ?TEST_TABLE_1, ?KEY),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY).

t_delete_all_data(_) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:remove_all_data(?TEST_TABLE_1),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY).

t_delete_record(_) ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(Record),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY).

t_delete_record_dirty(_) ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(dirty, Record),
    [] = app_cache:get_data(?TEST_TABLE_1, ?KEY).

t_delete_record_ignoring_timestamp(_) ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    2 = length(app_cache:get_data(?TEST_TABLE_2, ?KEY)),
    app_cache:remove_record_ignoring_timestamp(?RECORD30),
    [] = app_cache:get_data(?TEST_TABLE_2, ?KEY).

t_delete_record_ignoring_timestamp_dirty(_) ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    2 = length(app_cache:get_data(?TEST_TABLE_2, ?KEY)),
    app_cache:remove_record_ignoring_timestamp(dirty, ?RECORD30),
    [] = app_cache:get_data(?TEST_TABLE_2, ?KEY).

t_get_records(_) ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    2 = length(app_cache:get_records(?RECORD30)).

t_get_records_dirty(_) ->
    ok = app_cache:set_data(?RECORD30),
    %% timestamps have 1s resolution
    receive
    after 1050 ->
            ok
    end,
    ok = app_cache:set_data(?RECORD30),
    2 = length(app_cache:get_records(dirty, ?RECORD30)).

t_set_and_get_data_many(_) ->
    LoadFun = get_load_data_fun(100),
    LoadFun(),
    [Data] = app_cache:get_data(?TEST_TABLE_1, 35),
    {35} = Data#test_table_1.value.

t_cache_expiration(_) ->
    app_cache:update_table_time_to_live(?TEST_TABLE_1, 2),
    app_cache_scavenger:reset_timer(?TEST_TABLE_1),
    timer:sleep(1000),
    LoadFun = get_load_data_fun(10),
    {_LTime, _LValue} = timer:tc(LoadFun),
    timer:sleep(10000),
    [] = app_cache:get_after(?TEST_TABLE_1, 0).

t_init_table(_) ->
    ok = app_cache:init_table(?TEST_TABLE_1).

t_init_table_nodes(_) ->
    ok = app_cache:init_table(?TEST_TABLE_2, [node()]).

t_init_metatable(_) ->
    ok = app_cache:init_metatable().

t_init_metatable_nodes(_) ->
    ok = app_cache:init_metatable([node()]).

t_get_metatable(_) ->
    Res = app_cache:get_metatable(),
    Metatables = [M || M = #app_metatable{} <- Res],
    true = (length(Metatables) > 0).

t_app_cache_last_update_to_datetime_test(_) ->
    {{0,1,1},{0,0,0}} = app_cache:last_update_to_datetime(0).

get_load_data_fun(Count) ->
    fun() ->
            lists:map(fun(X) -> Record = #test_table_1{key = X, value = {X}}, app_cache:set_data(Record) end, lists:seq(1,Count))
    end.

empty_all_tables() ->
    lists:foreach(fun(Table) -> empty_table(Table) end, [?TEST_TABLE_1, ?TEST_TABLE_2]),
    app_cache:sequence_delete(?KEY).

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

%%
%% Setup Functions
%%
start() ->
    app_cache:setup(),
    app_cache:start(),
    app_cache:create_table(?TABLE1),
    app_cache:create_table(?TABLE2),
    app_cache:create_table(?TABLE3).


stop() ->
    app_cache:stop(),
    mnesia:delete_schema([node()]).

start_with_schema() ->
    start(),
    app_cache:stop(),
    app_cache:start().
