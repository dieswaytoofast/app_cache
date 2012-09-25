%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Cache template type definitions and records.
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------

-include("types.hrl").

%% Errors
-define(NO_SUCH_SEQUENCE, no_such_sequence).
-define(INVALID_TABLE, invalid_table).
-define(INVALID_PERSIST_FUNCTION, invalid_persist_function).
-define(INVALID_REFRESH_FUNCTION, invalid_refresh_function).
-define(INVALID_FUNCTION_IDENTIFIER, invalid_function_identifier).
-define(PERSIST_FAILURE, persist_failure).

-define(PROCESSOR,     app_cache_processor).
-define(SCAVENGER,     app_cache_scavenger).
-define(SEQUENCE_CACHE,     app_cache_sequence_cache).

-define(INFINITY,     infinity).
-define(META_VERSION, 1).

-define(SCAVENGE_FACTOR, 2*1000).       %% 1000 'cos of microseconds

-define(DEFAULT_CACHE_START, 1).
-define(DEFAULT_CACHE_INCREMENT, 1).
-define(DEFAULT_CACHE_CACHED_INCREMENT, 1).
-define(DEFAULT_CACHE_UPPER_BOUND_INCREMENT, 10).
-define(DEFAULT_CACHE_TTL, ?INFINITY).
-define(TIMESTAMP,     timestamp).
-define(DEFAULT_TIMESTAMP, undefined).
-define(DEFAULT_TYPE, set).

-define(METATABLE, app_metatable).

-define(TRANSACTION_TYPE_SAFE, safe).
-define(TRANSACTION_TYPE_DIRTY, dirty).

-record(refresh_data, {
          before_each_read = false                          :: boolean(),
          after_each_read = false                           :: boolean(),
          refresh_interval = ?INFINITY                      :: time_to_live(),   % Seconds
          function_identifier                               :: function_identifier()
         }).

-record(persist_data, {
          synchronous = false                               :: boolean(),
          function_identifier                               :: function_identifier()
         }).

-record(app_metatable, {
          table                                             :: table() | '_',
          version = ?META_VERSION                           :: table_version() | '_',
          time_to_live = ?INFINITY                          :: time_to_live() | '_',    %% in seconds
          type = ?DEFAULT_TYPE                              :: table_type() | '_',
          fields = []                                       :: [table_key()] | '_',
          secondary_index_fields = []                       :: [table_key()] | '_',
          read_transform_function                           :: function_identifier() | '_',
          write_transform_function                          :: function_identifier() | '_',
          refresh_function = #refresh_data{}                :: #refresh_data{} | '_',
          persist_function = #persist_data{}                :: #persist_data{} | '_',
          last_update = 0                                   :: non_neg_integer() | '_',
          reason                                            :: any(),
          extras                                            :: any()
         }).

%% Helper record to keep track of the functions that we use
-record(data_functions, {
          read_transform_function                           :: function() | undefined,
          write_transform_function                          :: function() | undefined,
          refresh_function = #refresh_data{}                :: #refresh_data{},
          persist_function = #persist_data{}                :: #persist_data{}
          }).

-define(REFRESH_TABLE, refresh_table).
-record(?REFRESH_TABLE, {
            key                                             :: refresh_key(),
            value                                           :: table_key() | '_',
            time_to_live                                    :: time_to_live() | '_',
            last_update                                     :: non_neg_integer() | '_',
            timer                                           :: timer2:tref() | '$2'
            }).

-define(REFRESH_TABLE_DEF, #app_metatable{
                            table = ?REFRESH_TABLE,
                            time_to_live = ?INFINITY,
                            type = ordered_set,
                            fields = [key, value, time_to_live, last_update, timer]
                          }).


-define(SEQUENCE_TABLE, sequence_table).
-record(?SEQUENCE_TABLE, {
            key                                             :: sequence_key(),
            value                                           :: sequence_value()
            }).

-define(SEQUENCE_TABLE_DEF, #app_metatable{
                            table = ?SEQUENCE_TABLE,
                            time_to_live = ?INFINITY,
                            type = set,
                            fields = [key, value]
                          }).

-record(sequence_cache, {
            key                                                        :: sequence_key(),
            start = ?DEFAULT_CACHE_START                               :: sequence_value(),
            cached_value = ?DEFAULT_CACHE_START                        :: sequence_value(),
            upper_bound = ?DEFAULT_CACHE_START                         :: sequence_value(),
            upper_bound_increment = ?DEFAULT_CACHE_UPPER_BOUND_INCREMENT :: sequence_value()
         }).


%% Testing
-record(test_table_1, {
            key,
            timestamp,
            value,
            name}).
-define(TEST_METATABLE1, #app_metatable{
                table = test_table_1,
                version = 1, 
                time_to_live = 600,
                type = ordered_set,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
-record(test_table_2, {
            key,
            timestamp,
            value,
            name}).
-define(TEST_METATABLE2, #app_metatable{
                table = test_table_2,
                version = 1, 
                time_to_live = 60,
                type = bag,
                fields = [key, timestamp, value, name],
                secondary_index_fields = [name]
            }).
