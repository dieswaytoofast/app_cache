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
-define(KEY, foo).
-define(KEY2, foo).
-define(VALUE, bar).
-define(VALUE2, bar2).
-define(NAME, baz).
-define(NAME2, baz2).
-define(RECORD, {test_table_1, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD2, {test_table_1, ?KEY2, undefined, ?VALUE2, ?NAME2}).
-define(RECORD30, {test_table_2, ?KEY, undefined, ?VALUE, ?NAME}).
-define(RECORD31, {test_table_2, ?KEY, undefined, ?VALUE2, ?NAME2}).
-define(TABLES, [ 
            #app_metatable{
                table = test_table_1,
                version = 1, 
                time_to_live = 60,
                type = ordered_set,
                fields = [key, timestamp, value, name]
                },
            #app_metatable{
                table = test_table_2,
                version = 1, 
                time_to_live = 180,
                type = set,
                fields = [id, timestamp, name, pretty_name]}]).

%% ------------------------------------------------------------------
%% Test Function Definitions
%% ------------------------------------------------------------------

%%
%% Test Descriptions
%%
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
    app_cache:create_table(?TABLE2).


stop(_) ->
    app_cache:stop(),
    mnesia:delete_schema([node()]).



%%
%% Helper Functions
%%

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
    [Data] = app_cache:get_last_entered_data(?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

t_get_last_entered_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    ok = app_cache:set_data(?RECORD2),
    [Data] = app_cache:get_last_entered_data(dirty, ?TEST_TABLE_1),
    ?_assertEqual(?VALUE2, Data#test_table_1.value).

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
    Result = app_cache:remove_data(test_table_1, ?KEY),
    ?_assertEqual(Result, ok).

t_delete_data_dirty(_In) ->
    ok = app_cache:set_data(?RECORD),
    Result = app_cache:remove_data(dirty, test_table_1, ?KEY),
    ?_assertEqual(Result, ok).

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

