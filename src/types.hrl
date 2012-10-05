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
-type error()           :: {error, Reason :: term()}.
-type table()           :: atom().
-type table_key()       :: any().
-type table_key_position()  :: non_neg_integer().
-type table_version()   :: non_neg_integer().
-type table_type()      :: atom().
-type timestamp()       :: non_neg_integer().
-type last_update()     :: timestamp().
-type time_to_live()    :: non_neg_integer() | infinity.
-type table_fields()    :: [table_key()].
-type transaction_type()    :: safe | dirty.
-type sequence_key()    :: any().
-type sequence_value()  :: non_neg_integer().

-type function_identifier() :: {function, fun((any()) -> any())} |
                               {module_and_function, {atom(), atom()}} |
                               undefined.

-type app_field()       :: atom().

-type refresh_key()       :: {any(), any()}.
-type function_dict() :: any().
