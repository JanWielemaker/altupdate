:- use_module(altupdate).

dbg :-
    set_prolog_flag(message_context, [thread,time]),
    debug(alt),
    trace(dispatch/2),
    tmon,
    debug.

init :-
    alt_create_workers(test, [add, subtract]).

post(N, Id) :-
    ignore(q(_)),
    alt_request(test, N, Id).

wait(Id) :-
    alt_wait(test, Id, Request, []),
    writeln(Request).

trip(Val, Action) :-
    trip(Val, Action, []).

trip(Val, Action, Options) :-
    ignore(q(_)),
    Request = i(Val, Action),
    alt_request(test, Request, Id),
    alt_wait(test, Id, Request, Options),
    forall(q(X), writeln(X)).

:- dynamic p/1 as incremental.
:- table q/1 as incremental.

q(X) :- p(X).

add(i(Val, added)) :-
    (   q(X)
    ->  X2 is X+Val
    ;   X2 = Val
    ),
    asserta(p(X2)),
    ignore(q(_)),
    random_sleep(1),
    format("Adder: Added ~p~n", [p(X2)]).

subtract(i(Val, substracted)) :-
    (   q(X)
    ->  X2 is X-Val
    ;   X2 is -Val
    ),
    asserta(p(X2)),
    ignore(q(_)),
    random_sleep(1),
    format("Subtractor: Added ~p~n", [p(X2)]).

random_sleep(Max) :-
    Wait is random_float*Max,
    sleep(Wait).
