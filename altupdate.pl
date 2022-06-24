:- module(altupdate,
          [ alt_create_workers/1, % +Goals
            alt_request/2,        % +Request,-Id
            alt_wait/1            % +Id
          ]).
:- use_module(library(apply)).
:- use_module(library(increval)).

:- meta_predicate
    alt_create_workers(:),
    alt_create_worker(0).

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
    maplist(alt_create_worker(M), Goals).

alt_create_worker(M, Goal) :-
    alt_create_worker(M:Goal).

alt_create_worker(Goal) :-
    thread_create(alt_worker(Goal), TID, []),
    asserta(worker(Goal, TID)).

alt_worker(Goal) :-
    thread_get_message(Msg),
    dispatch(Msg, Goal),
    alt_worker(Goal).

dispatch(request(Id, Request), Goal) :-
    (   catch(transaction(request(call(Goal, Request), Changes),
                          only_first(Id),
                          altupdate_mutex),
              Exception,
              true)
    ->  (   var(Exception)
        ->  send_changes(Id, Changes)
        ;   true
        )
    ;   true
    ).
dispatch(update(_Id, Updates), _Goal) :-
    maplist(update, Updates).

request(Goal, Changes) :-
    call(Goal),
    !,
    transaction_updates(Changes).

only_first(Id) :-
    (   done(Id)
    ->  fail
    ;   asserta(done(Id))
    ).

send_changes(Id, Updates) :-
    thread_self(Me),
    forall(worker(_, TID),
           (   TID == Me
           ->  true
           ;   thread_signal(TID, lost_from(Me)),
               thread_send_message(TID, updates(Id, Updates))
           )),
    queue(Q),
    thread_send_message(Q, updates(Id, Updates)).

lost_from(Me) :-
    throw(lost_from(Me)).

%!  alt_request(+Request, -Id) is det.
%
%   Post a request on the pool. This predicate succeeds immediately. Use
%   alt_wait/1 to wait for the request to finish.

alt_request(Request, Id) :-
    flag(alt_request_id, Id, Id+1),
    forall(worker(_, TID),
           thread_send_message(TID, request(Id, Request))).

%!  alt_wait(+Id) is det.
%
%   Wait for request Id to be completed

alt_wait(Id) :-
    queue(Q),
    thread_get_message(Q, updates(Id, Updates)),
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
