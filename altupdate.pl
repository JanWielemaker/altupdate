:- module(altupdate,
          [ alt_create_workers/1, % +Goals
            alt_request/2,        % +Request,-Id
            alt_wait/3            % +Id, -Request, +Options
          ]).
:- use_module(library(apply)).
:- use_module(library(increval)).
:- use_module(library(debug)).

:- meta_predicate
    alt_create_workers(:).

/** <module> Implement concurrent alternative strategies
*/

:- dynamic
    worker/2,		% Goal, Thread
    queue/1,
    done/1.             % Id

%!  alt_create_workers(:Goals)
%
%   Create a set of worker threads for processing requests. Each of the
%   threads will execute call(Goal,Request)

alt_create_workers(M:Goals) :-
    message_queue_create(Q),
    asserta(queue(Q)),
    foldl(alt_create_worker(M), Goals, 1, _).

:- meta_predicate
    alt_create_worker(0, +),
    request(0, -),
    run(1, +).

alt_create_worker(M, Goal, Id, Nid) :-
    alt_create_worker(M:Goal, Id),
    Nid is Id+1.

alt_create_worker(Goal, Id) :-
    Goal = _:G,
    pi_head(PI, G),
    format(atom(Alias), 'alt_~d_~w', [Id, PI]),
    thread_create(alt_worker(Goal), TID, [alias(Alias)]),
    asserta(worker(Goal, TID)).

alt_worker(Goal) :-
    thread_get_message(Msg),
    dispatch(Msg, Goal),
    alt_worker(Goal).

dispatch(request(Id, Request), Goal) =>
    (   catch(transaction(request(run(Goal, Request), Changes),
                          only_first(Id),
                          altupdate_mutex),
              Exception,
              true)
    ->  (   var(Exception)
        ->  debug(alt, 'Won!  Committing ~p', [Changes]),
            send_changes(Id, Request, Changes)
        ;   debug(alt, 'Exception: ~p', [Exception])
        )
    ;   debug(alt, 'Failed', [])
    ).
dispatch(updates(_Id, Updates), _Goal) =>
    maplist(update, Updates).

request(Goal, Changes) :-
    call(Goal),
    transaction_updates(Changes).

run(Goal, Request) :-
    call(Goal, Request),
    !.
run(_, _) :-
    fail.

only_first(Id) :-
    (   done(Id)
    ->  fail
    ;   asserta(done(Id))
    ).

send_changes(Id, Request, Updates) :-
    thread_self(Me),
    forall(worker(_, TID),
           (   TID == Me
           ->  true
           ;   thread_signal(TID, lost_from(Me)),
               thread_send_message(TID, updates(Id, Updates))
           )),
    queue(Q),
    thread_send_message(Q, success(Id, Request, Updates)).

lost_from(Winner) :-
    in_run,
    !,
    throw(lost_from(Winner)).
lost_from(_).

cancelled :-
    in_run,
    !,
    throw(cancelled).
cancelled.

in_run :-
    prolog_current_frame(Frame),
    prolog_frame_attribute(Frame, parent_goal, run(_,_)).

%!  alt_request(+Request, -Id) is det.
%
%   Post a request on the pool. This predicate succeeds immediately. Use
%   alt_wait/1 to wait for the request to finish.

alt_request(Request, Id) :-
    flag(alt_request_id, Id, Id+1),
    forall(worker(_, TID),
           thread_send_message(TID, request(Id, Request))).

%!  alt_wait(+Id, -Request, +Options) is semidet.
%
%   Wait for request Id to be completed. Unify Request with the possibly
%   instantiated request. Options are   passed  to thread_get_message/3,
%   where notably timeout(+Seconds) and deadline(+AbdTime) are useful.

alt_wait(Id, Request, Options) :-
    queue(Q),
    (   thread_get_message(Q, success(Id, Request, Updates), Options)
    ->  updates(Updates)
    ;   forall(worker(_, TID),
               thread_signal(TID, cancelled)),
        (   thread_get_message(Q, updates(Id, Updates), [timeout(0.01)])
        ->  updates(Updates)
        ;   fail        % timeout
        )
    ).


updates(Updates) :-
    maplist(update, Updates).

%!  update(+Update) is det.

:- det(update/1).
update(asserta(ClauseRef)) :-
    catch(clause(Head,_Body,ClauseRef), error(_,_), fail),
    !,
    incr_invalidate_calls(Head).
update(assertz(ClauseRef)) :-
    catch(clause(Head,_Body,ClauseRef), error(_,_), fail),
    !,
    incr_invalidate_calls(Head).
update(erase(ClauseRef)) :-
    catch('$clause'(Head, _Body, ClauseRef, _Bindings), error(_,_), fail),
    !,
    incr_invalidate_calls(Head).
update(_).
