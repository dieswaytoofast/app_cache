%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc The cache processor 
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
%%%
%%% TODO:
%%%         - async tests
%%%
%%%
-module(app_cache_SUITE).

-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Paul Oliver <puzza007@gmail.com>').

%% Note: This directive should only be used in test suites.
-compile(export_all).

-export([p_record_1_transform_fun_e/1]).
-export([p_record_1_persist_fun_e/1]).
-export([p_record_1_fail_fun_e/1]).
-export([p_record_1_refresh_fun_e/1]).

-include("../src/defaults.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

%% ------------------------------------------------------------------
%% Defines
%% ------------------------------------------------------------------

-define(PROPTEST(A), true = proper:quickcheck(A(), [{numtests, 1000}])).
-define(PROPTEST_LONG(A), true = proper:quickcheck(A(), [{spec_timeout, 1000000}, 
                                                         {numtests, 1000}])).
-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).

-define(MUL2, 2).
-define(MUL3, 3).
%%% Dummy data

-record(p_record_1, {
        key                         :: table_key(),
        timestamp                   :: timestamp(),
        value                       :: any(),
        name                        :: any()
        }).

-record(p_record_2, {
        key                         :: table_key(),
        timestamp                   :: timestamp(),
        value                       :: any(),
        name                        :: any()
        }).

-record(p_record_3, {
        key                         :: table_key(),
        value                       :: any()
        }).

-type p_record_1()                     :: #p_record_1{}.
-type p_record_2()                     :: #p_record_2{}.
-type p_record_3()                     :: #p_record_3{}.



-define(P_TABLE_1, #app_metatable{
                table = p_record_1,
                version = 1,
                time_to_live = 60,
                type = ordered_set,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
-define(P_TABLE_2, #app_metatable{
                table = p_record_2,
                version = 1,
                time_to_live = 60,
                type = bag,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
-define(P_TABLE_3, #app_metatable{
                table = p_record_3,
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

suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap,{minutes,1}}].

init_per_suite(Config) ->
    start_applications(),
    setup_lager(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_, Config) ->
    start(),
    Config.

end_per_group(_GroupName, _Config) ->
    stop(),
    ok.

init_per_testcase(_TestCase, Config) ->
    setup_lager(),
    Config.

end_per_testcase(sequence, _Config) ->
    app_cache:sequence_delete(?KEY);
end_per_testcase(cached_sequence, _Config) ->
    app_cache:cached_sequence_delete(?KEY);
end_per_testcase(_TestCase, _Config) ->
    clear_data().

groups() ->
    [{sequence, [],
      [t_prop_sequence_create,
       t_prop_sequence_current_value,
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

    {prop_tests, [],
     [
      t_set_data,
      t_set_data_with_transform_function,
      t_set_data_with_sync_persist_function,
      t_set_data_with_persist_function_bad,
      t_set_data_with_sync_persist_fail_function,
      t_set_data_with_refresh_function_1,
      t_set_data_with_refresh_function_2,
%      t_set_data_with_refresh_function_3,      %long
      t_set_data_with_refresh_function_4,
      t_set_data_with_refresh_function_5,
      t_set_data_with_refresh_function_bad,
      t_set_data_overwriting_timestamp,
      t_set_data_if_unique,
      t_get_functions,
      t_get_data,
      t_get_data_with_transform_function,
       t_get_bag_data,
       t_get_data_from_index,
       t_get_last_entered_data,
       t_get_first_entered_data,
       t_get_last_n_entries,
       t_get_first_n_entries,
       t_key_exists,
       t_delete_data,
       t_delete_all_data,
       t_delete_record
%       t_delete_record_ignoring_timestamp,     %long
%       t_get_bag_records,                        % long
%       t_cache_expiration                       % long

                ]},

    {table, [],
     [t_table_info,
      t_table_version,
      t_table_time_to_live,
      t_table_fields,
      t_cache_time_to_live,
      t_cache_time_to_live_bad_table,
      t_ttl_and_field_index,
      t_update_time_to_live]}].

all() ->
    [{group, sequence},
     {group, cached_sequence},
     {group, start_with_schema},
     {group, table},
     {group, prop_tests},
     t_app_cache_last_update_to_datetime_test].
%    [{group, prop_tests}].

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

t_sequence_next_value(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    lists:foreach(fun(_X) -> app_cache:sequence_next_value(?KEY) end,
                  lists:seq(1,10)),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [#sequence_table{key =?KEY, value = 11}] = MData,
    app_cache:sequence_delete(?KEY).

t_sequence_set_value(_) ->
    ok = app_cache:sequence_create(?KEY, 1),
    ok = app_cache:sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [#sequence_table{key =?KEY, value = 9999}] = MData,
    app_cache:sequence_delete(?KEY).

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
    [#sequence_table{key =?KEY, value = 1 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}] = MData,
    app_cache:cached_sequence_delete(?KEY).

t_cached_sequence_create_default(_) ->
    Start = app_cache:get_env(cache_start, ?DEFAULT_CACHE_START),
    ok = app_cache:cached_sequence_create(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    MData = [#sequence_table{key =?KEY, value = Start + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT}],
    app_cache:cached_sequence_delete(?KEY).

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
    21 = Value,
    app_cache:cached_sequence_delete(?KEY).

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
    [#sequence_table{key =?KEY, value = 31}] = MData,
    app_cache:cached_sequence_delete(?KEY).

t_cached_sequence_delete(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    app_cache:cached_sequence_delete(?KEY),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    [] = MData.

t_cached_sequence_set_value(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    ok = app_cache:cached_sequence_set_value(?KEY, 9999),
    MData = mnesia:dirty_read(sequence_table, ?KEY),
    Value = 9999 + ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT,
    [#sequence_table{key =?KEY, value = Value}] = MData,
    app_cache:cached_sequence_delete(?KEY).

t_cached_sequence_all_sequences(_) ->
    [] = app_cache:cached_sequence_all_sequences().

t_cached_sequence_all_sequences_one(_) ->
    ok = app_cache:cached_sequence_create(?KEY, 1),
    All = app_cache:cached_sequence_all_sequences(),
    [#sequence_cache{key =?KEY, start = 1}] = All,
    app_cache:cached_sequence_delete(?KEY).

t_cached_sequence_statem(_) ->
    ok.
%    ?PROPTEST(app_cache_cached_sequence_proper, prop_sequence).

t_table_info(_) ->
    Data = app_cache:table_info(p_record_1),
    MTableInfo = mnesia:dirty_read(app_metatable, p_record_1),
    [Data] = MTableInfo.

t_table_version(_) ->
    Data = app_cache:table_version(p_record_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    Data = MTableInfo#app_metatable.version.

t_table_time_to_live(_) ->
    Data = app_cache:table_time_to_live(p_record_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    Data = MTableInfo#app_metatable.time_to_live.

t_table_fields(_) ->
    Data = app_cache:table_fields(p_record_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    Data = MTableInfo#app_metatable.fields.

t_cache_time_to_live(_) ->
    Data = app_cache:cache_time_to_live(p_record_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    Data = MTableInfo#app_metatable.time_to_live.

t_cache_time_to_live_bad_table(_) ->
    {error, _} = app_cache:cache_time_to_live(bad_table).

t_ttl_and_field_index(_) ->
    TableInfo = app_cache:table_info(p_record_1),
    Data = app_cache_processor:get_ttl_and_field_index(TableInfo),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    Data = {MTableInfo#app_metatable.time_to_live, 2}.

t_update_time_to_live(_) ->
    TTL = 700,
    app_cache:update_table_time_to_live(p_record_1, TTL),
    TTL = app_cache:cache_time_to_live(p_record_1),
    [MTableInfo] = mnesia:dirty_read(app_metatable, p_record_1),
    TTL = MTableInfo#app_metatable.time_to_live.

t_init_table(_) ->
    ok = app_cache:init_table(p_record_1).

t_init_table_nodes(_) ->
    ok = app_cache:init_table(p_record_2, [node()]).

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


t_set_data(_) -> ?PROPTEST(prop_set_data).
prop_set_data() ->
    ?FORALL({TransactionType, Record}, {transaction_type(), p_record_1()}, 
            do_set_data(TransactionType, Record)).
do_set_data(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    ok = app_cache:set_data(TransactionType, URecord),
    clear_data().

t_set_data_with_transform_function(_) -> ?PROPTEST(prop_set_data_with_transform_function).
prop_set_data_with_transform_function() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{value = Integer})},
            do_set_data_with_transform_function(TransactionType, Record)).
do_set_data_with_transform_function(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    {Function, Multiplier} = transform_function_and_multiplier(),
    app_cache:set_write_transform_function(p_record_1, Function),
    ok = app_cache:set_data(TransactionType, URecord),
    [Data] = app_cache:get_data(TransactionType, p_record_1, Record#p_record_1.key),
    clear_data(),
    Multiplier * URecord#p_record_1.value =:= Data#p_record_1.value.

t_set_data_with_sync_persist_function(_) -> ?PROPTEST(prop_set_data_with_sync_persist_function).
prop_set_data_with_sync_persist_function() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{value = Integer})},
            do_set_data_with_sync_persist_function(TransactionType, Record)).
do_set_data_with_sync_persist_function(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    {Function, Multiplier} = persist_function_and_multiplier(),
    ok = app_cache:set_persist_function(p_record_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    % The transform function writes to the pure KV table
    [Data] = app_cache:get_data(TransactionType, p_record_3, Record#p_record_1.key),
    clear_data(),
    Multiplier * URecord#p_record_1.value =:= Data#p_record_3.value.

t_set_data_with_persist_function_bad(_) -> ?PROPTEST(prop_set_data_with_persist_function_bad).
prop_set_data_with_persist_function_bad() ->
    ?FORALL(NotAFunction, ?SUCHTHAT(Item, any(), not_valid_function(Item)),
            do_set_data_with_persist_function_bad(NotAFunction)).
do_set_data_with_persist_function_bad(NotAFunction) ->
    {error, {invalid_persist_function, _}} = app_cache:set_persist_function(p_record_1, NotAFunction),
    clear_data().

t_set_data_with_sync_persist_fail_function(_) -> ?PROPTEST(prop_set_data_with_sync_persist_fail_function).
prop_set_data_with_sync_persist_fail_function() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{value = Integer})},
            do_set_data_with_sync_persist_fail_function(TransactionType, Record)).
do_set_data_with_sync_persist_fail_function(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    Function = fail_function(),
    ok = app_cache:set_persist_function(p_record_1,
                                   #persist_data{synchronous = true,
                                                 function_identifier = Function}),
    {error, {persist_failure, _}} = app_cache:set_data(TransactionType, URecord),
    [] = app_cache:get_data(TransactionType, p_record_1, Record#p_record_1.key),
    clear_data().

% Using the refresh function immediately
t_set_data_with_refresh_function_1(_) -> ?PROPTEST(prop_set_data_with_refresh_function_1).
prop_set_data_with_refresh_function_1() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{key = Integer,
                                                                                    value = Integer})},
            do_set_data_with_refresh_function_1(TransactionType, Record)).
do_set_data_with_refresh_function_1(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    Key = Value = URecord#p_record_1.key,
    {Function, Multiplier} = refresh_function_and_multiplier(),
    ok = app_cache:set_refresh_function(p_record_1, #refresh_data{before_each_read = true,
                                                             after_each_read = false,
                                                             refresh_interval = ?INFINITY, 
                                                             function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    %% Before, so each read already has been refreshed
    [Data1] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value1 = Multiplier * Value,
    Value1 = Data1#p_record_1.value,
    [Data2] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value2 = Multiplier * Value1,
    Value2 = Data2#p_record_1.value,
    clear_data().

% Not using the refresh function
t_set_data_with_refresh_function_2(_) -> ?PROPTEST(prop_set_data_with_refresh_function_2).
prop_set_data_with_refresh_function_2() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{key = Integer,
                                                                                    value = Integer})},
            do_set_data_with_refresh_function_2(TransactionType, Record)).
do_set_data_with_refresh_function_2(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    Key = Value = URecord#p_record_1.key,
    {Function, _Increment} = refresh_function_and_multiplier(),
    ok = app_cache:set_refresh_function(p_record_1, #refresh_data{before_each_read = false,
                                                             after_each_read = false,
                                                             refresh_interval = ?INFINITY, 
                                                             function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    %% Before, so each read already has been refreshed
    [Data1] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value = Data1#p_record_1.value,
    [Data2] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value = Data2#p_record_1.value,
    clear_data().

% Using the refresh function after 1 second
t_set_data_with_refresh_function_3(_) -> ?PROPTEST(prop_set_data_with_refresh_function_3).
prop_set_data_with_refresh_function_3() ->
    numtests(10, ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{key = Integer,
                                                                                    value = Integer})},
                        do_set_data_with_refresh_function_3(TransactionType, Record))).
do_set_data_with_refresh_function_3(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    Key = Value = URecord#p_record_1.key,
    {Function, Multiplier} = refresh_function_and_multiplier(),
    ok = app_cache:set_refresh_function(p_record_1, #refresh_data{before_each_read = false,
                                                             after_each_read = false,
                                                             refresh_interval = 2,
                                                             function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    % Get some data, so that we can start the refreshing
    [_Data0] = app_cache:get_data(TransactionType, p_record_1, Key),
    timer:sleep(3000),
    [Data1] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value1 = Multiplier * Value,
    Value1 = Data1#p_record_1.value,
    clear_data().

t_set_data_with_refresh_function_4(_) -> ?PROPTEST(prop_set_data_with_refresh_function_4).
prop_set_data_with_refresh_function_4() ->
    numtests(10, ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{key = Integer,
                                                                                    value = Integer})},
                        do_set_data_with_refresh_function_4(TransactionType, Record))).
do_set_data_with_refresh_function_4(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    Key = Value = URecord#p_record_1.key,
    {Function, Multiplier} = refresh_function_and_multiplier(),
    ok = app_cache:set_refresh_function(p_record_1, #refresh_data{before_each_read = false,
                                                             after_each_read = true,
                                                             refresh_interval = ?INFINITY,
                                                             function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    % Get some data, so that we can start the refreshing
    [_Data0] = app_cache:get_data(TransactionType, p_record_1, Key),
    timer:sleep(1000),
    [Data1] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value1 = Multiplier * Value,
    Value1 = Data1#p_record_1.value,
    timer:sleep(1000),
    [Data2] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value2 = Multiplier * Value1,
    Value2 = Data2#p_record_1.value,
    clear_data().

t_set_data_with_refresh_function_5(_) -> ?PROPTEST(prop_set_data_with_refresh_function_5).
prop_set_data_with_refresh_function_5() ->
    numtests(10, ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{key = Integer,
                                                                                    value = Integer})},
                        do_set_data_with_refresh_function_5(TransactionType, Record))).
do_set_data_with_refresh_function_5(TransactionType, Record) -> URecord = Record#p_record_1{timestamp = undefined},
    Key = Value = URecord#p_record_1.key,
    {Function, Multiplier} = refresh_function_and_multiplier(),
    ok = app_cache:set_refresh_function(p_record_1, #refresh_data{before_each_read = true,
                                                             after_each_read = true,
                                                             refresh_interval = ?INFINITY,
                                                             function_identifier = Function}),
    ok = app_cache:set_data(TransactionType, URecord),
    % Get some data, so that we can start the refreshing
    [Data0] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value0 = Multiplier * Value,
    Value0 = Data0#p_record_1.value,
    timer:sleep(1000),
    [Data1] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value1 = Multiplier * Multiplier * Value0,
    Value1 = Data1#p_record_1.value,
    timer:sleep(1000),
    [Data2] = app_cache:get_data(TransactionType, p_record_1, Key),
    Value2 = Multiplier * Multiplier * Value1,
    Value2 = Data2#p_record_1.value,
    clear_data().

t_set_data_with_refresh_function_bad(_) -> ?PROPTEST(prop_set_data_with_refresh_function_bad).
prop_set_data_with_refresh_function_bad() ->
    ?FORALL(NotAFunction, ?SUCHTHAT(Item, any(), not_valid_function(Item)),
            do_set_data_with_refresh_function_bad(NotAFunction)).
do_set_data_with_refresh_function_bad(NotAFunction) ->
    {error, {invalid_refresh_function, _}} = app_cache:set_refresh_function(p_record_1, NotAFunction),
    clear_data().

t_set_data_overwriting_timestamp(_) -> ?PROPTEST(prop_set_data_overwriting_timestamp).
prop_set_data_overwriting_timestamp() ->
    ?FORALL({TransactionType, Record}, 
                         {transaction_type(), p_record_2()}, 
            do_set_data_overwriting_timestamp(TransactionType, Record)).
do_set_data_overwriting_timestamp(TransactionType, Record) -> 
    Key = Record#p_record_2.key,
    Value = Record#p_record_2.value,
    Name = Record#p_record_2.name,
    ok = app_cache:set_data(TransactionType, Record),
    try
        case app_cache:set_data_overwriting_timestamp(TransactionType, Record) of
            ok ->
                [Data] = app_cache:get_data(TransactionType, p_record_2, Key),
                {Key, Value, Name} = {Data#p_record_2.key, Data#p_record_2.value, Data#p_record_2.name};
            % Got an '_' somewhere. If you put one in, its your own damn
            % fault
            {error, {bad_type, p_record_2, _}} ->
                true
        end
    catch
        % Got an '_' somewhere. If you put one in, its your own damn
        % fault
        _:{aborted, {bad_type, p_record_2, _}} ->
            true
    end,
    clear_data().

t_set_data_if_unique(_) -> ?PROPTEST(prop_set_data_if_unique).
prop_set_data_if_unique() ->
    ?FORALL({TransactionType, Record, Value}, 
            ?SUCHTHAT({_TType, R, V}, 
                      {transaction_type(), p_record_2(), any()}, 
                      V =/= R#p_record_2.value),
                      do_set_data_if_unique(TransactionType, Record, Value)).
do_set_data_if_unique(TransactionType, Record, Value) -> 
    ok = app_cache:set_data(TransactionType, Record),
    {error, {item_already_exists, _}} = app_cache:set_data_if_unique(TransactionType, Record),
    {error, {key_already_exists, _}} = app_cache:set_data_if_unique(TransactionType, Record#p_record_2{value = Value}),
    clear_data().

t_get_functions(_) -> ?PROPTEST(prop_get_function).
prop_get_function() ->
    ?FORALL({Before, After, Interval, Sync}, {boolean(), boolean(), non_neg_integer(), boolean()}, 
            do_get_functions(Before, After, Interval, Sync)).
do_get_functions(Before, After, Interval, Sync) ->
    {RefreshFunction, _} = refresh_function_and_multiplier(),
    {PersistFunction, _} = persist_function_and_multiplier(),
    {ReadFunction, _} = transform_function_and_multiplier(),
    {WriteFunction, _} = transform_function_and_multiplier(),
    RefreshData = #refresh_data{before_each_read = Before,
                               after_each_read = After,
                               refresh_interval = Interval,
                               function_identifier = RefreshFunction},
    PersistData = #persist_data{synchronous = Sync,
                                function_identifier = PersistFunction},
    app_cache:set_read_transform_function(p_record_1, ReadFunction),
    app_cache:set_write_transform_function(p_record_1, WriteFunction),
    app_cache:set_refresh_function(p_record_1, RefreshData),
    app_cache:set_persist_function(p_record_1, PersistData),
    #data_functions{read_transform_function = ReadFunction,
                   write_transform_function = WriteFunction,
                   refresh_function = RefreshData,
                   persist_function = PersistData} = app_cache_processor:get_functions(p_record_1),
    clear_data().

t_get_data(_) -> ?PROPTEST(prop_get_data).
prop_get_data() ->
    ?FORALL({TransactionType, Record}, {transaction_type(), p_record_1()},
            do_get_data(TransactionType, Record)).
do_get_data(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    ok = app_cache:set_data(TransactionType, URecord),
    [Data] = app_cache:get_data(TransactionType, p_record_1, Record#p_record_1.key),
    URecord = Data#p_record_1{timestamp = undefined},
    clear_data().

t_get_data_with_transform_function(_) -> ?PROPTEST(prop_get_data_with_transform_function).
prop_get_data_with_transform_function() ->
    ?FORALL({TransactionType, Record}, 
            {transaction_type(), 
             ?LET({ARecord, Integer}, {p_record_1(), integer()}, ARecord#p_record_1{value = Integer})},
            do_get_data_with_transform_function(TransactionType, Record)).
do_get_data_with_transform_function(TransactionType, Record) ->
    URecord = Record#p_record_1{timestamp = undefined},
    {Function, Multiplier} = transform_function_and_multiplier(),
    app_cache:set_read_transform_function(p_record_1, Function),
    ok = app_cache:set_data(TransactionType, Record#p_record_1{timestamp = undefined}),
    [Data] = app_cache:get_data(TransactionType, p_record_1, Record#p_record_1.key),
    clear_data(),
    Multiplier * URecord#p_record_1.value =:= Data#p_record_1.value.


t_get_bag_data(_) -> ?PROPTEST(prop_get_bag_data).
prop_get_bag_data() ->
    ?FORALL({TransactionType, RecordList, Key}, 
            {transaction_type(), list(p_record_2()), any()}, do_get_bag_data(TransactionType, RecordList, Key)).
do_get_bag_data(TransactionType, RecordList, Key) ->
    % Records with identical keys and values get overwritten!
    UniqueRecordList = lists:foldl(fun(X, Acc) -> 
                    case lists:keyfind(X#p_record_2.value, #p_record_2.value, RecordList) of
                        false -> [X|Acc];
                        _ -> Acc
                    end end, [], RecordList),
    Length = length(UniqueRecordList),
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item#p_record_2{key = Key})
        end, UniqueRecordList),
    Data = app_cache:get_data(TransactionType, p_record_2, Key),
    Length = length(Data),
    clear_data().

t_get_data_from_index(_) -> ?PROPTEST(prop_get_data_from_index).
prop_get_data_from_index() ->
    ?FORALL({TransactionType, RecordList, Record}, 
            {transaction_type(), list(p_record_1()), p_record_1()}, do_get_data_from_index(TransactionType, RecordList, Record)).
do_get_data_from_index(TransactionType, RecordList, Record) ->
    Key = Record#p_record_1.key,
    Value = Record#p_record_1.value,
    Name = Record#p_record_1.name,
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item#p_record_1{key = Key})
        end, RecordList),
    ok = app_cache:set_data(TransactionType, Record),
    [Data] = app_cache:get_data_from_index(TransactionType, p_record_1, Name, name),
    {Key, Value} = {Data#p_record_1.key, Data#p_record_1.value},
    clear_data().

t_get_last_entered_data(_) -> ?PROPTEST(prop_get_last_entered_data).
prop_get_last_entered_data() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())}, do_get_last_entered_data(TransactionType, RecordList)).
do_get_last_entered_data(TransactionType, RecordList) ->
    {UniqueRecordList, _KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    {LastData, LKey, LValue, LName} = case UniqueRecordList of
        [] ->
            {[], undefined, undefined, undefined};
        _ -> Item = lists:max(UniqueRecordList),
            {[Item], 
             Item#p_record_1.key, 
             Item#p_record_1.value, 
             Item#p_record_1.name}
    end,
    case app_cache:get_data_by_last_key(TransactionType, p_record_1) of
        [] ->
            LastData = [];
        [Data] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name}
    end,
    clear_data().

t_get_first_entered_data(_) -> ?PROPTEST(prop_get_first_entered_data).
prop_get_first_entered_data() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())}, do_get_first_entered_data(TransactionType, RecordList)).
do_get_first_entered_data(TransactionType, RecordList) ->
    {UniqueRecordList, _KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    {FirstData, LKey, LValue, LName} = case UniqueRecordList of
        [] ->
            {[], undefined, undefined, undefined};
        _ -> 
            % Need the 'erlang first term' order item
            Item = lists:min(UniqueRecordList),
            {[Item], 
             Item#p_record_1.key, 
             Item#p_record_1.value, 
             Item#p_record_1.name}
    end,
    case app_cache:get_data_by_first_key(TransactionType, p_record_1) of
        [] ->
            FirstData = [];
        [Data] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name}
    end,
    clear_data().

t_get_last_n_entries(_) -> ?PROPTEST(prop_get_last_n_entries).
prop_get_last_n_entries() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())}, do_get_last_n_entries(TransactionType, RecordList)).
do_get_last_n_entries(TransactionType, RecordList) ->
    {UniqueRecordList, _KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    {FirstData, LKey, LValue, LName} = case UniqueRecordList of
        [] ->
            {[], undefined, undefined, undefined};
        _ -> 
            % Need the 'erlang first term' order item
            Item = lists:max(UniqueRecordList),
            {[Item], 
             Item#p_record_1.key, 
             Item#p_record_1.value, 
             Item#p_record_1.name}
    end,
    case app_cache:get_last_n_entries(TransactionType, p_record_1, 3) of
        [] ->
            FirstData =:= [];
        [Data] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name};
        [Data | _Tail] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name}

    end,
    clear_data().

t_get_first_n_entries(_) -> ?PROPTEST(prop_get_first_n_entries).
prop_get_first_n_entries() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())}, do_get_first_n_entries(TransactionType, RecordList)).
do_get_first_n_entries(TransactionType, RecordList) ->
    {UniqueRecordList, _KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    {FirstData, LKey, LValue, LName} = case UniqueRecordList of
        [] ->
            {[], undefined, undefined, undefined};
        _ -> 
            % Need the 'erlang first term' order item
            Item = lists:min(UniqueRecordList),
            {[Item], 
             Item#p_record_1.key, 
             Item#p_record_1.value, 
             Item#p_record_1.name}
    end,
    case app_cache:get_first_n_entries(TransactionType, p_record_1, 3) of
        [] ->
            FirstData =:= [];
        [Data] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name};
        [Data | _Tail] ->
            {LKey, LValue, LName} = {Data#p_record_1.key, 
                                     Data#p_record_1.value,
                                     Data#p_record_1.name}

    end,
    clear_data().

t_key_exists(_) -> ?PROPTEST(prop_key_exists).
prop_key_exists() ->
    ?FORALL({TransactionType, RecordList, Key}, 
            % Need a key that is not in the record list
            ?SUCHTHAT({_TType, RList, K}, 
                      {transaction_type(), list(p_record_1()), any()}, 
                      case lists:keyfind(K, #p_record_1.key, RList) of
                      false -> true;
                      _ -> false
                      end), do_key_exists(TransactionType, RecordList, Key)).
do_key_exists(TransactionType, RecordList, Key) ->
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    lists:foreach(fun(InKey) ->
                true = app_cache:key_exists(TransactionType, p_record_1, InKey)
        end, KeyList),
    false = app_cache:key_exists(TransactionType, p_record_1, Key),
    clear_data().

t_delete_data(_) -> ?PROPTEST(prop_delete_data).
prop_delete_data() ->
    ?FORALL({TransactionType, RecordList, Key}, 
            % Need a key that is not in the record list
            ?SUCHTHAT({_TType, RList, K}, 
                      {transaction_type(), list(p_record_1()), any()}, 
                      case lists:keyfind(K, #p_record_1.key, RList) of
                      false -> true;
                      _ -> false
                      end), do_delete_data(TransactionType, RecordList, Key)).
do_delete_data(TransactionType, RecordList, Key) ->
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % Unknown Key
    ok = app_cache:remove_data(TransactionType, p_record_1, Key),
    [] = app_cache:get_data(TransactionType, p_record_1, Key),
    % Valid key
    case UniqueRecordList of
        [#p_record_1{key = Key1} | _Rest] ->
            ok = app_cache:remove_data(TransactionType, p_record_1, Key1),
            [] = app_cache:get_data(TransactionType, p_record_1, Key1);
        _ ->
            ok
    end,
    % all keys
    ok = app_cache:remove_lots_of_data(TransactionType, p_record_1, KeyList),
    lists:foreach(fun(IKey) ->
                [] = app_cache:get_data(TransactionType, p_record_1, IKey)
        end, KeyList),
    clear_data().

t_delete_all_data(_) -> ?PROPTEST(prop_delete_all_data).
prop_delete_all_data() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())},
            do_delete_all_data(TransactionType, RecordList)).
do_delete_all_data(TransactionType, RecordList) ->
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % Unknown Key
    ok = app_cache:remove_all_data(TransactionType, p_record_1),
    lists:foreach(fun(IKey) ->
                [] = app_cache:get_data(TransactionType, p_record_1, IKey)
        end, KeyList),
    clear_data().

t_delete_record(_) -> ?PROPTEST(prop_delete_record).
prop_delete_record() ->
    ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())},
                          do_delete_record(TransactionType, RecordList)).
do_delete_record(TransactionType, RecordList) ->
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % Remove records
    lists:foreach(fun(IKey) ->
                [Record] = app_cache:get_data(TransactionType, p_record_1, IKey),


                try
	                case app_cache:remove_record(TransactionType, Record) of
	                    ok -> 
	                        ok;
	                    % Got an '_' somewhere. If you put one in, its your own damn
	                    % fault
	                    {error, {bad_type, p_record_1, _}} ->
	                        app_cache:remove_data(TransactionType, p_record_1, IKey)
	                end
                catch
                    % Got an '_' somewhere. If you put one in, its your own damn
                    % fault
                    _:{aborted, {bad_type, p_record_1, _}} ->
                        app_cache:remove_data(TransactionType, p_record_1, IKey),
                        ok
                end,
                [] = app_cache:get_data(TransactionType, p_record_1, IKey)
        end, KeyList),
    clear_data().

% Particularly relevant for bags, where we can have multiple (same)
% records w/ different timestamps
t_delete_record_ignoring_timestamp(_) -> ?PROPTEST_LONG(prop_delete_record_ignoring_timestamp).
prop_delete_record_ignoring_timestamp() ->
    numtests(10, ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_2())},
            do_delete_record_ignoring_timestamp(TransactionType, RecordList))).
do_delete_record_ignoring_timestamp(TransactionType, RecordList) ->
    % Need unique keys here
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_2.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_2.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item),
                % app_cache timers have a 1s resolution
                timer:sleep(1050),
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % There should be two of each record
    lists:foreach(fun(IKey) ->
                [Item1, Item2] = app_cache:get_data(TransactionType, p_record_2, IKey),
                app_cache:remove_record_ignoring_timestamp(TransactionType, Item1),
                try
                    case app_cache:remove_record_ignoring_timestamp(TransactionType, Item1) of
                        ok -> 
                            ok;
                            % Got an '_' somewhere. If you put one in, its your own damn
                            % fault
                        {error, {bad_type, p_record_2, _}} ->
                            app_cache:remove_data(TransactionType, p_record_2, Item1#p_record_2.key),
                            app_cache:remove_data(TransactionType, p_record_2, Item2#p_record_2.key)
                end
                catch
                    % Got an '_' somewhere. If you put one in, its your own damn
                    % fault
                    _:{aborted, {bad_type, p_record_2, _}} ->
                        app_cache:remove_data(TransactionType, p_record_2, Item1#p_record_2.key),
                        app_cache:remove_data(TransactionType, p_record_2, Item2#p_record_2.key),
                        ok
                end,
                [] = app_cache:get_data(TransactionType, p_record_2, IKey)
        end, KeyList),
    clear_data().

% Particularly relevant for bags, where we can have multiple (same)
% records w/ different timestamps
t_get_bag_records(_) -> ?PROPTEST_LONG(prop_get_bag_records).
prop_get_bag_records() ->
    numtests(10, ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_2())},
            do_get_bag_records(TransactionType, RecordList))).
do_get_bag_records(TransactionType, RecordList) ->
    % Need unique keys here
    {UniqueRecordList, KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_2.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_2.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item),
                % app_cache timers have a 1s resolution
                timer:sleep(1050),
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % There should be two of each record
    lists:foreach(fun(IKey) ->
                [_Item1, _Item2] = app_cache:get_data(TransactionType, p_record_2, IKey)
        end, KeyList),
    clear_data().

% Particularly relevant for bags, where we can have multiple (same)
% records w/ different timestamps
t_cache_expiration(_) -> ?PROPTEST_LONG(prop_cache_expiration).
prop_cache_expiration() ->
    numtests(10, ?FORALL({TransactionType, RecordList}, 
            {transaction_type(), list(p_record_1())},
            do_cache_expiration(TransactionType, RecordList))).
do_cache_expiration(TransactionType, RecordList) ->
    % Need unique keys here
    {UniqueRecordList, _KeyList} = lists:foldl(fun(X, {RAcc, KAcc} = Acc) -> 
                    case lists:member(X#p_record_1.key, KAcc) of
                        false -> {[X|RAcc], [X#p_record_1.key|KAcc]};
                        _ -> Acc
                    end end, {[], []}, RecordList),
    % Set the TTL to something small, and reset
    app_cache:update_table_time_to_live(p_record_1, 1),
    app_cache_scavenger:reset_timer(p_record_1),
    timer:sleep(1000),
    % Store the data
    lists:foreach(fun(Item) -> 
                ok = app_cache:set_data(TransactionType, Item)
        end, UniqueRecordList),
    % Wait for everything to expire
    timer:sleep(3000),
    [] = app_cache:get_all_data(p_record_1),
    clear_data().




%%
%% Setup Functions
%%
start() ->
    random:seed(erlang:now()),
    app_cache:setup(),
    application:start(mnesia),
    application:start(app_cache),
    app_cache:create_table(?P_TABLE_1),
    app_cache:create_table(?P_TABLE_2),
    app_cache:create_table(?P_TABLE_3).


stop() ->
    application:stop(app_cache),
    application:stop(mnesia),
    mnesia:delete_schema([node()]).

start_applications() ->
    application:start(sasl),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager),
    application:start(gproc),
    application:start(timer2).

setup_lager() ->
    lager:set_loglevel(lager_console_backend, debug),
    lager:set_loglevel(lager_file_backend, "console.log", debug).


%% Internal functions
p_record_1_transform_fun(Record) ->
    OldValue = Record#p_record_1.value,
    Record#p_record_1{value = ?MUL2 * OldValue}.

p_record_1_transform_fun_e(Record) ->
    OldValue = Record#p_record_1.value,
    Record#p_record_1{value = ?MUL3 * OldValue}.

p_record_1_persist_fun(Record) ->
    Key = Record#p_record_1.key,
    Value = ?MUL2 * Record#p_record_1.value,
    store_p_record_3(Key, Value).

p_record_1_persist_fun_e(Record) ->
    Key = Record#p_record_1.key,
    Value = ?MUL3 * Record#p_record_1.value,
    store_p_record_3(Key, Value).

store_p_record_3(Key, Value) ->
    case mnesia:transaction(fun() -> 
    mnesia:write(#p_record_3{key = Key, value = Value}) end) of
        {atomic, ok} -> ok;
        Error -> Error
    end.

p_record_1_fail_fun(_Record) ->
    erlang:exit(fail).

p_record_1_fail_fun_e(_Record) ->
    erlang:exit(fail).

% Create a 'temporary' KV pair in p_record_3 that we use to do the test with
p_record_1_refresh_fun(Key) ->
    update_p_record_3(Key, ?MUL2).

p_record_1_refresh_fun_e(Key) ->
    update_p_record_3(Key, ?MUL3).

update_p_record_3(Key, Multiplier) ->
    Fun = fun() ->
            Value = 
            case mnesia:wread({p_record_3, Key}) of
                [#p_record_3{value = V}] ->
                    Multiplier * V;
                _Error ->
                    Multiplier * Key
            end,
            mnesia:write(#p_record_3{key = Key, value = Value}),
            #p_record_1{key = Key, value = Value}
    end,
    case mnesia:transaction(Fun) of
        {atomic, Record} -> 
            Record;
        Error -> 
            Error
    end.

clear_data() ->
    app_cache:sequence_delete(?KEY),
    mnesia:clear_table(p_record_1),
    mnesia:clear_table(p_record_2),
    mnesia:clear_table(p_record_3),
    ok = app_cache:set_read_transform_function(p_record_2, undefined),
    ok = app_cache:set_write_transform_function(p_record_2, undefined),
    ok = app_cache:set_persist_function(p_record_2, #persist_data{}),
    ok = app_cache:set_refresh_function(p_record_2, #refresh_data{}),
    ok = app_cache:set_read_transform_function(p_record_1, undefined),
    ok = app_cache:set_write_transform_function(p_record_1, undefined),
    ok = app_cache:set_persist_function(p_record_1, #persist_data{}),
    ok =:= app_cache:set_refresh_function(p_record_1, #refresh_data{}).


%%
%% Property functions
%%
transform_function_and_multiplier() ->
    case random:uniform(2) of
        1 ->
            {{function, fun p_record_1_transform_fun/1}, ?MUL2};
        2 ->
            {{module_and_function, {?MODULE,
                                   p_record_1_transform_fun_e}}, ?MUL3}
    end.

persist_function_and_multiplier() ->
    case random:uniform(2) of
        1 ->
            {{function, fun p_record_1_persist_fun/1}, ?MUL2};
        2 ->
            {{module_and_function, {?MODULE,
                                   p_record_1_persist_fun_e}}, ?MUL3}
    end.

fail_function() ->
    case random:uniform(2) of
        1 ->
            {function, fun p_record_1_fail_fun/1};
        2 ->
            {module_and_function, {?MODULE,
                                   p_record_1_fail_fun_3}}
    end.

refresh_function_and_multiplier() ->
    case random:uniform(1) of
        2 ->
            {{function, fun p_record_1_refresh_fun/1}, ?MUL2};
        1 ->
            {{module_and_function, {?MODULE,
                                   p_record_1_refresh_fun_e}}, ?MUL3}
    end.


not_valid_function({function, Fun}) when is_function(Fun) -> false;
not_valid_function({module_and_function, {M, F}}) when is_atom(M), is_atom(F) -> false;
not_valid_function(_) -> true.
