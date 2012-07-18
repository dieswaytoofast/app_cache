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
-define(VALUE, bar).
-define(VALUE2, bar2).
-define(VALUE3, bar3).
-define(NAME, baz).
-define(NAME2, baz2).
-define(NAME3, baz3).
-define(RECORD, {test_table_1, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD2, {test_table_1, ?KEY2, undefined, ?VALUE2, ?NAME2}).
-define(RECORD3, {test_table_1, ?KEY3, undefined, ?VALUE3, ?NAME3}).
-define(RECORD30, {test_table_2, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD31, {test_table_2, ?KEY, undefined, ?VALUE2, ?NAME2}).

%% ------------------------------------------------------------------
%% Test Function Definitions
%% ------------------------------------------------------------------

%%
%% Test Descriptions
%%
table_get_first_n_entries_test_() ->
    [{"table_first_n_entries",
     ?setup(fun t_get_first_n_entries/1)}].

table_get_first_n_entries_dirty_test_() ->
    [{"table_first_n_entries_dirty",
     ?setup(fun t_get_first_n_entries_dirty/1)}].

table_get_last_n_entries_test_() ->
    [{"table_last_n_entries",
     ?setup(fun t_get_last_n_entries/1)}].

table_get_last_n_entries_dirty_test_() ->
    [{"table_last_n_entries_dirty",
     ?setup(fun t_get_last_n_entries_dirty/1)}].

table_delete_record_test_() ->
    [{"table_delete_record",
     ?setup(fun t_delete_record/1)}].

table_delete_record_dirty_test_() ->
    [{"table_delete_record_dirty_test_",
     ?setup(fun t_delete_record_dirty/1)}].

table_info_test_() ->
    [{"table_info",
     ?setup(fun t_table_info/1)}].

table_version_test_() ->
    [{"table_version",
     ?setup(fun t_table_version/1)}].

table_time_to_live_test_() ->
    [{"table_time_to_live",
     ?setup(fun t_table_time_to_live/1)}].

table_fields_test_() ->
    [{"table_fields",
     ?setup(fun t_table_fields/1)}].

table_cache_time_to_live_test_() ->
    [{"table_cache_time_to_live",
     ?setup(fun t_cache_time_to_live/1)}].

table_ttl_and_field_index_test_() ->
    [{"table_ttl_and_field_index",
     ?setup(fun t_ttl_and_field_index/1)}].

table_update_time_to_live_test_() ->
    [{"table_update_time_to_live",
     ?setup(fun t_update_time_to_live/1)}].

table_set_data_test_() ->
    [{"table_set_data",
     ?setup(fun t_set_data/1)}].

table_set_data_dirty_test_() ->
    [{"table_set_data_dirty",
     ?setup(fun t_set_data_dirty/1)}].

table_get_data_test_() ->
    [{"table_get_data",
     ?setup(fun t_get_data/1)}].

table_get_data_dirty_test_() ->
    [{"table_get_data_dirty",
     ?setup(fun t_get_data_dirty/1)}].

table_get_bag_data_test_() ->
    [{"table_get_bag_data",
     ?setup(fun t_get_bag_data/1)}].

table_get_bag_data_dirty_test_() ->
    [{"table_get_bag_data_dirty",
     ?setup(fun t_get_bag_data_dirty/1)}].

table_get_data_from_index_test_() ->
    [{"table_get_data_from_index",
     ?setup(fun t_get_data_from_index/1)}].

table_get_data__from_index_dirty_test_() ->
    [{"table_get_data__from_indexdirty",
     ?setup(fun t_get_data_from_index_dirty/1)}].

table_get_last_entered_data_test_() ->
    [{"table_last_entered_data",
     ?setup(fun t_get_last_entered_data/1)}].

table_get_last_entered_data_dirty_test_() ->
    [{"table_last_entered_data_dirty",
     ?setup(fun t_get_last_entered_data_dirty/1)}].

table_key_exists_test_() ->
    [{"table_key_exists",
     ?setup(fun t_key_exists/1)}].

table_key_exists_dirty_test_() ->
    [{"table_key_exists_dirty",
     ?setup(fun t_key_exists_dirty/1)}].

table_delete_data_test_() ->
    [{"table_delete_data",
     ?setup(fun t_delete_data/1)}].

table_delete_data_dirty_test_() ->
    [{"table_delete_data_dirty_test_",
     ?setup(fun t_delete_data_dirty/1)}].

t_cached_sequence_create_test_() ->
    [{"create a cached_sequence",
     ?setup(fun t_cached_sequence_create/1)}].

t_cached_sequence_current_value_test_() ->
    [{"cached_sequence current_value",
     ?setup(fun t_cached_sequence_current_value/1)}].

t_cached_sequence_current_value_0_test_() ->
    [{"cached_sequence current_value_0",
     ?setup(fun t_cached_sequence_current_value_0/1)}].

t_cached_sequence_current_value_1_test_() ->
    [{"cached_sequence current_value_1",
     ?setup(fun t_cached_sequence_current_value_1/1)}].

t_cached_sequence_next_value_test_() ->
    [{"cached_sequence next_value",
     ?setup(fun t_cached_sequence_next_value/1)}].

t_cached_sequence_delete_test_() ->
    [{"cached_sequence delete",
     ?setup(fun t_cached_sequence_delete/1)}].

t_sequence_create_test_() ->
    [{"create a sequence",
     ?setup(fun t_sequence_create/1)}].

t_sequence_current_value_test_() ->
    [{"sequence current_value",
     ?setup(fun t_sequence_current_value/1)}].

t_sequence_current_value_0_test_() ->
    [{"sequence current_value_0",
     ?setup(fun t_sequence_current_value_0/1)}].

t_sequence_current_value_1_test_() ->
    [{"sequence current_value_1",
     ?setup(fun t_sequence_current_value_1/1)}].

t_sequence_next_value_test_() ->
    [{"sequence next_value",
     ?setup(fun t_sequence_next_value/1)}].

t_sequence_delete_test_() ->
    [{"sequence delete",
     ?setup(fun t_sequence_delete/1)}].

set_and_get_data_many_test_() ->
    [{"many record is inserted and read",
     ?setup(fun t_set_and_get_data_many/1)}].

cache_expiration_test_() ->
    [{"cache expiration",
     ?setup(fun t_cache_expiration/1)}].

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



%%
%% Helper Functions
%%

t_sequence_create(_In) ->
    ok = app_cache:sequence_create(?KEY, 1),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 1}], MData).

t_sequence_current_value(_In) ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    Value = app_cache:sequence_current_value(?KEY),
    ?_assertEqual(11, Value).

t_sequence_current_value_0(_In) ->
    ok = app_cache:sequence_create(?KEY, 1),
    Value = app_cache:sequence_current_value(?KEY),
    ?_assertEqual(1, Value).

t_sequence_current_value_1(_In) ->
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    Value = app_cache:sequence_current_value(?KEY),
    % 'cos the first 'next_value' is the 'set-value'
    ?_assertEqual(10, Value).

t_sequence_next_value(_In) ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 11}], MData).

t_sequence_delete(_In) ->
    ok = app_cache:sequence_create(?KEY, 1),
    app_cache:sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([], MData).

t_cached_sequence_create(_In) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 1 +?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}], MData).

t_cached_sequence_current_value_0(_In) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    Value = app_cache:cached_sequence_current_value(?KEY),
    ?_assertEqual(1, Value).

t_cached_sequence_current_value(_In) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    ?_assertEqual(21, Value).

t_cached_sequence_current_value_1(_In) ->
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    Value = app_cache:cached_sequence_current_value(?KEY),
    ?_assertEqual(21, Value).

t_cached_sequence_next_value(_In) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:cached_sequence_next_value(?KEY) end,
                  lists:seq(1,20)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([#sequence_table{key =?KEY, value = 31}], MData).

t_cached_sequence_delete(_In) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    app_cache:cached_sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    ?_assertEqual([], MData).

t_table_info(_In) ->
    Data = app_cache:table_info(?TEST_TABLE_1),
    MTableInfo = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual([Data], MTableInfo).

t_table_version(_In) ->
    Data = app_cache:table_version(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.version).

t_table_time_to_live(_In) ->
    Data = app_cache:table_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.time_to_live).

t_table_fields(_In) ->
    Data = app_cache:table_fields(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.fields).

t_cache_time_to_live(_In) ->
    Data = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, MTableInfo#app_metatable.time_to_live).

t_ttl_and_field_index(_In) ->
    Data = app_cache:get_ttl_and_field_index(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(Data, {MTableInfo#app_metatable.time_to_live, 2}).

t_update_time_to_live(_In) ->
    TTL = 700,
    app_cache:update_table_time_to_live(?TEST_TABLE_1, TTL),
    TTL = app_cache:cache_time_to_live(?TEST_TABLE_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, ?TEST_TABLE_1),
    ?_assertEqual(TTL, MTableInfo#app_metatable.time_to_live).

t_set_data(_In) ->
    Result = app_cache:set_data(?RECORD),
    ?_assertEqual(Result, ok).

t_set_data_dirty(_In) ->
    Result = app_cache:set_data(dirty, ?RECORD),
    ?_assertEqual(Result, ok).

t_get_data(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data(dirty, ?TEST_TABLE_1, ?KEY),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_bag_data(_In) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(?TEST_TABLE_2, ?KEY),
    ?_assertEqual(2, length(Data)).

t_get_bag_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD30),
    ok = app_cache:set_data(?RECORD31),
    Data = app_cache:get_data(dirty, ?TEST_TABLE_2, ?KEY),
    ?_assertEqual(2, length(Data)).

t_get_data_from_index(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(?TEST_TABLE_1, ?NAME, name),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_data_from_index_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Data] = app_cache:get_data_from_index(dirty, ?TEST_TABLE_1, ?NAME, name),
    ?_assertEqual(?VALUE, Data#test_table_1.value).

t_get_last_entered_data(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

t_get_last_entered_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_data_by_last_key(dirty, ?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

t_get_last_n_entries(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_last_n_entries_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_last_n_entries(dirty, ?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_first_n_entries(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_get_first_n_entries_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    ok = app_cache:set_data(?RECORD3),
    [_H|[T]] = app_cache:get_first_n_entries(dirty, ?TEST_TABLE_1, 2),
    ?_assertEqual(?VALUE2, T#test_table_1.value).

t_key_exists(_In) ->
    false = app_cache:key_exists(?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:key_exists(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, true).

t_key_exists_dirty(_In) ->
    false = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY),
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:key_exists(dirty, ?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, true).

t_delete_data(_In) ->
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:remove_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, ok).

t_delete_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:remove_data(dirty, ?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Result, ok).

t_delete_record(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(Record),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_delete_record_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    [Record] = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    app_cache:remove_record(dirty, Record),
    Data = app_cache:get_data(?TEST_TABLE_1, ?KEY),
    ?_assertEqual(Data, []).

t_set_and_get_data_many(_In) ->
    LoadFun = get_load_data_fun(100),
    LoadFun(),
    [Data] = app_cache:get_data(?TEST_TABLE_1, 35),
    ?_assertEqual(Data#test_table_1.value, {35}).

t_cache_expiration(_In) ->
    app_cache:update_table_time_to_live(?TEST_TABLE_1, 2),
    app_cache_scavenger:reset_timer(?TEST_TABLE_1),
    timer:sleep(1000),
    LoadFun = get_load_data_fun(10),
    {_LTime, _LValue} = timer:tc(LoadFun),
    timer:sleep(10000),
    Data1 = app_cache:get_after(?TEST_TABLE_1, 0),
    ?_assertEqual(Data1, []).

get_load_data_fun(Count) ->
    fun() -> 
            lists:map(fun(X) -> Record = #test_table_1{key = X, value = {X}}, app_cache:set_data(Record) end, lists:seq(1,Count)) 
    end.

