-module(app_cache_sequence_proper).

-include("../src/defaults.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-behaviour(proper_statem).

-export([command/1, initial_state/0, next_state/3, postcondition/3,
         precondition/2]).

-record(state, {keys=sets:new(), seqs=dict:new()}). %% {Key, CurValue} -> true

-define(KEYS, [a,b,c,d,e,f,g]).

initial_state() ->
    #state{}.

command(#state{}) ->
    oneof([
           {call, app_cache, sequence_create, [key()]},
           {call, app_cache, sequence_create, [key(), sequence_value()]},
           {call, app_cache, sequence_set_value, [key(), sequence_value()]},
           {call, app_cache, sequence_current_value, [key()]},
           {call, app_cache, sequence_next_value, [key()]},
           {call, app_cache, sequence_next_value, [key(), sequence_value()]},
           {call, app_cache, sequence_delete, [key()]}
          ]).

next_state(State=#state{keys=Keys, seqs=Seqs}, _Result, {call, app_cache, sequence_create, [Key]}) ->
    State#state{keys=sets:add_element(Key, Keys), seqs=dict:store(Key, ?DEFAULT_CACHE_START, Seqs)};
next_state(State=#state{keys=Keys, seqs=Seqs}, _Result, {call, app_cache, sequence_create, [Key, Start]}) ->
    State#state{keys=sets:add_element(Key, Keys), seqs=dict:store(Key, Start, Seqs)};
next_state(State=#state{seqs=Seqs}, _Result, {call, app_cache, sequence_set_value, [Key, Start]}) ->
    State#state{seqs=dict:store(Key, Start, Seqs)};
next_state(State, _Result, {call, app_cache, sequence_current_value, [_Key]}) ->
    State;
next_state(State=#state{seqs=Seqs}, _Result, {call, app_cache, sequence_next_value, [Key]}) ->
    Cur = dict:fetch(Key, Seqs),
    State#state{seqs=dict:store(Key, Cur + 1, Seqs)};
next_state(State=#state{seqs=Seqs}, _Result, {call, app_cache, sequence_next_value, [Key, Increment]}) ->
    Cur = dict:fetch(Key, Seqs),
    State#state{seqs=dict:store(Key, Cur + Increment, Seqs)};
next_state(State=#state{keys=Keys, seqs=Seqs}, _Result, {call, app_cache, sequence_delete, [Key]}) ->
    State#state{keys=sets:del_element(Key, Keys), seqs=dict:erase(Key, Seqs)}.

precondition(_State, {call, app_cache, sequence_create, [_Key]}) ->
    true;
precondition(_State, {call, app_cache, sequence_create, [_Key, _Start]}) ->
    true;
precondition(#state{keys=Keys}, {call, app_cache, sequence_set_value, [Key, _Start]}) ->
    sets:is_element(Key, Keys);
precondition(_State, {call, app_cache, sequence_current_value, [_Key]}) ->
    true;
precondition(#state{keys=Keys}, {call, app_cache, sequence_next_value, [Key]}) ->
    sets:is_element(Key, Keys);
precondition(#state{keys=Keys}, {call, app_cache, sequence_next_value, [Key, _Increment]}) ->
    sets:is_element(Key, Keys);
precondition(#state{keys=Keys}, {call, app_cache, sequence_delete, [Key]}) ->
    sets:is_element(Key, Keys).

postcondition(_State, {call, app_cache, sequence_create, [_Key]}, Res) ->
    Res =:= ok;
postcondition(_State, {call, app_cache, sequence_create, [_Key, _Start]}, Res) ->
    Res =:= ok;
postcondition(_State, {call, app_cache, sequence_set_value, [_Key, _Start]}, Res) ->
    Res =:= ok;
postcondition(#state{keys=Keys, seqs=Seqs}, {call, app_cache, sequence_current_value, [Key]}, Value) ->
    case sets:is_element(Key, Keys) of
        true ->
            Value =:= dict:fetch(Key, Seqs);
        false ->
            Value =:= ?DEFAULT_CACHE_START
    end;
postcondition(#state{seqs=Seqs}, {call, app_cache, sequence_next_value, [Key]}, Value) ->
    Value =:= (dict:fetch(Key, Seqs) + 1);
postcondition(#state{seqs=Seqs}, {call, app_cache, sequence_next_value, [Key, Increment]}, Value) ->
    Value =:= (dict:fetch(Key, Seqs) + Increment);
postcondition(#state{seqs=Seqs}, {call, app_cache, sequence_delete, [Key]}, Res) ->
    Res =:= ok andalso dict:find(Key, Seqs) =/= false.

prop_sequence() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   {Hist, State, Res} = run_commands(?MODULE, Cmds),
                   cleanup(),
                   ?WHENFAIL(error_logger:info_msg("Commands: ~w\nHistory: ~w\nState: ~w\nResult: ~w",
                                                   [Hist, State, Res, Cmds]),
                             aggregate(command_names(Cmds),
                                       Res =:= ok))
               end)).

key() ->
    elements(?KEYS).

sequence_value() ->
    non_neg_integer().

cleanup() ->
    [app_cache:sequence_delete(Key) || Key <- ?KEYS].
