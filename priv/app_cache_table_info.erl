%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @author Tom Heinan <me@tomheinan.com>
%%% @copyright (C) 2012 Juan Jose Comellas, Mahesh Paolini-Subramanya, Tom Heinan
%%% @doc helper module to build #app_metatable{} for use with app_cache
%%% @end
%%%-------------------------------------------------------------------
-module(app_cache_table_info).
-author('Juan Jose Comellas <juanjo@comellas.org>').
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').
-author('Tom Heinan <me@tomheinan.com>').

-compile([{parse_transform, dynarec}]).

%% Exports
-export([table_info/1, table_info/2, table_info/3, table_info/4, table_info/5]).

-include("defaults.hrl").

-type mt_table()                                   :: atom().
-type mt_table_version()                           :: any().
-type mt_time_to_live()                            :: pos_integer().
-type mt_table_type()                              :: set | ordered_set | bag.
-type mt_table_key()                               :: atom().
-type mt_secondary_index_fields()                  :: [mt_table_key()].

%% app_cache metadata
-record(app_metatable, {
          table                                 :: mt_table(),
          version                               :: mt_table_version(),
          time_to_live                          :: mt_time_to_live() | infinity,    %% in seconds
          type                                  :: mt_table_type(),
          fields = []                           :: [mt_table_key()],
          secondary_index_fields = []           :: mt_secondary_index_fields(),
          last_update,
          reason,
          extras
         }).


-define(DEFAULT_TTL, infinity).
-define(DEFAULT_VERSION, 1).
-define(DEFAULT_TYPE, set).
-define(DEFAULT_SECONDARY_INDEX_FIELDS, []).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-spec table_info(mt_table()) -> [#app_metatable{}].
table_info(RecordName) ->
    table_info(RecordName, ?DEFAULT_TYPE, ?DEFAULT_TTL, ?DEFAULT_SECONDARY_INDEX_FIELDS, ?DEFAULT_VERSION).

-spec table_info(mt_table(), mt_table_type()) -> [#app_metatable{}].
table_info(RecordName, Type) ->
    table_info(RecordName, Type, ?DEFAULT_TTL, ?DEFAULT_SECONDARY_INDEX_FIELDS, ?DEFAULT_VERSION).

-spec table_info(mt_table(), mt_table_type(), mt_time_to_live()) -> [#app_metatable{}].
table_info(RecordName, Type, TTL) ->
    table_info(RecordName, Type, TTL, ?DEFAULT_SECONDARY_INDEX_FIELDS, ?DEFAULT_VERSION).

-spec table_info(mt_table(), mt_table_type(), mt_time_to_live(), 
                 mt_secondary_index_fields()) -> [#app_metatable{}].
table_info(RecordName, Type, TTL, SecondaryIndexFields) ->
    table_info(RecordName, Type, TTL, SecondaryIndexFields, ?DEFAULT_VERSION).

-spec table_info(mt_table(), mt_table_type(), mt_time_to_live(), 
                 mt_secondary_index_fields(), mt_table_version()) -> [#app_metatable{}].
table_info(RecordName, Type, TTL, SecondaryIndexFields, Version) ->
    validate_table(RecordName),
    validate_type(Type),
    validate_ttl(TTL),
    validate_secondary_index_fields(SecondaryIndexFields, RecordName),
    #app_metatable{table = RecordName,
                   version = Version,
                   time_to_live = TTL,
                   type = Type,
                   fields = get_record_fields(RecordName),
                   secondary_index_fields = SecondaryIndexFields}.

-spec validate_table(mt_table()) -> ok.
validate_table(RecordName) when is_atom(RecordName) -> ok;
validate_table(RecordName) ->
    throw({error, {must_be_atom, RecordName}}).

-spec validate_type(mt_table_type()) -> ok.
validate_type(bag) -> ok;
validate_type(set) -> ok;
validate_type(ordered_set) -> ok;
validate_type(Type) -> throw({error, {invalid_table_type, Type}}).

-spec validate_ttl(mt_time_to_live()) -> ok.
validate_ttl(infinity) -> ok;
validate_ttl(TTL) when is_integer(TTL) ->
    if TTL > 0 ->
            ok;
        true ->
            throw({error, {must_be_positive, TTL}})
    end;
validate_ttl(TTL) ->
    throw({error, {must_be_integer, TTL}}).

-spec validate_secondary_index_fields(mt_secondary_index_fields(), mt_table()) -> ok.
validate_secondary_index_fields(SecondaryIndexFields, RecordName) when
        is_list(SecondaryIndexFields), is_atom(RecordName) -> 
    Fields = get_record_fields(RecordName),
    lists:foreach(fun(Field) ->
                case lists:member(Field, Fields) of
                    true ->
                        ok;
                    false ->
                        throw({error, {not_a_record_field, Field, RecordName}})
                end
        end, SecondaryIndexFields);
validate_secondary_index_fields(SecondaryIndexFields, RecordName) when
        is_list(SecondaryIndexFields) ->
    throw({error, {must_be_atom, RecordName}});
validate_secondary_index_fields(SecondaryIndexFields, RecordName) when
        is_atom(RecordName) -> 
    throw({error, {must_be_list, SecondaryIndexFields}}).

-spec get_record_fields(mt_table()) -> [mt_table_key()].
get_record_fields(RecordName) ->
    fields(RecordName).










