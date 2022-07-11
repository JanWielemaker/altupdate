:- use_module(altupdate).

dbg :-
    set_prolog_flag(message_context, [thread,time]),
    debug(alt),
    trace(dispatch/2),
    trace(alt_peek_/3),
%   trace(needs_update/2),
    tmon,
    debug.

init :-
    alt_create_pool(test,
                    [ threads(4)
                    ]).

post(Goal) :-
    ignore(q(_)),                       % update table
    alt_submit(test, Goal).

wait(Goal) :-
    alt_wait(test, Goal, []).

wait(Goal, Time) :-
    alt_wait(test, Goal, [timeout(Time)]).

trip(Val, Action) :-
    trip(Val, Action, []).

trip(Val, Goal, Options) :-
    ignore(q(_)),                      % update table
    alt_submit(test, add(Val)),
    alt_submit(test, sub(Val)),
    alt_wait(test, Goal, Options),
    forall(q(X), writeln(X)).

:- dynamic p/1 as incremental.
:- table q/1 as incremental.

q(X) :- p(X).

add(Val) :-
    (   q(X)
    ->  X2 is X+Val
    ;   X2 = Val
    ),
    asserta(p(X2)),
    ignore(q(_)),
    random_sleep(1),
    format("Added ~p~n", [p(X2)]).

sub(Val) :-
    (   q(X)
    ->  X2 is X-Val
    ;   X2 is -Val
    ),
    asserta(p(X2)),
    ignore(q(_)),
    random_sleep(1),
    format("Subtracted ~p~n", [p(X2)]).

random_sleep(Max) :-
    Wait is random_float*Max,
    sleep(Wait).
