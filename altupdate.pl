/*  Part of SWI-Prolog

    Author:        Jan Wielemaker
    E-mail:        jan@swi-prolog.org
    WWW:           http://www.swi-prolog.org
    Copyright (c)  2022, SWI-Prolog Solutions b.v.
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions
    are met:

    1. Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright
       notice, this list of conditions and the following disclaimer in
       the documentation and/or other materials provided with the
       distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

:- module(altupdate,
          [ alt_create_workers/2, % ++Pool, +Goals
            alt_close/1,          % ++Pool
            alt_request/3,        % ++Pool, +Request, -Id
            alt_wait/4            % ++Pool, +Id, -Request, +Options
          ]).
:- use_module(library(apply)).
:- use_module(library(increval)).
:- use_module(library(debug)).
:- use_module(library(lists)).
:- use_module(library(option)).
:- use_module(library(prolog_code)).
:- use_module(library(error)).

:- meta_predicate
    alt_create_workers(+, :).

/** <module> Concurrent evaluation of alternative strategies
*/

:- dynamic
    worker/3,		% Pool, Goal, Thread
    queue/2,            % Pool, Queue
    done/1.             % Id

%!  alt_create_workers(+Pool, :Goals)
%
%   Create a set of worker threads for processing requests. Each of the
%   threads will execute call(Goal,Request)

alt_create_workers(Pool, M:Goals) :-
    must_be(ground, Pool),
    message_queue_create(Q),
    asserta(queue(Pool, Q)),
    foldl(alt_create_worker(Pool, M), Goals, 1, _).

%!  alt_close(+Pool)
%
%   Terminate the workers.

alt_close(Pool) :-
    must_be(ground, Pool),
    retractall(queue(Pool, _)),
    forall(worker(Pool, _, TID),
           thread_send_message(TID, quit)),
    forall(worker(Pool, _, TID),
           thread_join(TID)).

:- meta_predicate
    alt_create_worker(0, +, +),
    request(0, -),
    run(1, +).

alt_create_worker(Pool, M, Goal, Id, Nid) :-
    alt_create_worker(M:Goal, Pool, Id),
    Nid is Id+1.

alt_create_worker(Goal, Pool, Id) :-
    Goal = _:G,
    pi_head(PI, G),
    format(atom(Alias), 'alt_~d_~w', [Id, PI]),
    thread_create(alt_worker(Pool, Goal), TID, [alias(Alias)]),
    asserta(worker(Pool, Goal, TID)).

%!  alt_worker(+Pool, :Goal)
%
%   Implement the worker loop.

alt_worker(Pool, Goal) :-
    thread_get_message(Msg),
    (   Msg == quit
    ->  true
    ;   dispatch(Pool, Msg, Goal),
        alt_worker(Pool, Goal)
    ).

%!  dispatch(+Pool, +Message, :Goal) is det.

dispatch(Pool, request(Id, Request), Goal) =>
    thread_self(Me),
    queue(Pool, Q),
    (   catch(transaction(request(run(Goal, Request), Changes),
                          only_first(Id),
                          altupdate_mutex),
              Exception,
              true)
    ->  (   var(Exception)
        ->  debug(alt, 'Won!  Committing ~p', [Changes]),
            send_changes(Pool, Me, Id, Request, Changes)
        ;   debug(alt, 'Exception: ~p', [Exception]),
            thread_send_message(Q, result(Id, Me, exception(Exception)))
        )
    ;   debug(alt, 'Failed', []),
        thread_send_message(Q, result(Id, Me, false))
    ).
dispatch(_, updates(_Id, Updates), _Goal) =>
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

%!  send_changes(+Pool, +Me, +Id, +Request, +Updates) is det.
%
%   Send updates to the other workers such  that they can invalidate the
%   relevant tables. This also sets a  message ready for alt_wait/3 with
%   the instantiated Request and the Updates to update their tables.

send_changes(Pool, Me, Id, Request, Updates) :-
    forall(worker(Pool, _, TID),
           (   TID == Me
           ->  true
           ;   thread_signal(TID, lost_from(Me)),
               thread_send_message(TID, updates(Id, Updates))
           )),
    queue(Pool, Q),
    thread_send_message(Q, result(Id, Me, success(Request, Updates))).

%!  lost_from(+Winner).
%!  cancelled.
%
%   These are asynchronously called  through   thread_signal.  As we are
%   running in the  target  thread  we   first  validate  we  are  still
%   executing the target goal and not some   other  part of the dispatch
%   cycle.

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

%!  alt_request(+Pool, +Request, -Id) is det.
%
%   Post a request on the pool. This predicate succeeds immediately. Use
%   alt_wait/1 to wait for the request to finish.

alt_request(Pool, Request, _Id) :-
    must_be(ground, Pool),
    \+ worker(Pool, _,_),
    !,
    existence_error(Request, worker_pool).
alt_request(Pool, Request, Id) :-
    flag(alt_request_id, Id, Id+1),
    forall(worker(Pool, _, TID),
           thread_send_message(TID, request(Id, Request))).

%!  alt_wait(+Pool, +Id, -Request, +Options) is semidet.
%
%   Wait for request Id to be completed. Unify Request with the possibly
%   instantiated request. Options are   passed  to thread_get_message/3,
%   where notably timeout(+Seconds) and deadline(+AbdTime) are useful.
%
%   This predicate may fail for two reasons
%
%     1. All workers failed or produced an exception
%     2. Timeout was reached.  If the timeout is reached we first
%        cancel all remaining workers and than wait for all of them
%        to complete.  Note that it is possible we get a success
%        anyway because that was already submitted

alt_wait(Pool, Id, Request, Options) :-
    must_be(ground, Pool),
    select_option(timeout(TMO), Options, Options1),
    !,
    get_time(Now),
    Deadline is Now+TMO,
    findall(T, worker(Pool, _, T), Workers),
    alt_wait(Pool, Id, Workers, Request, [deadline(Deadline)|Options1]).
alt_wait(Pool, Id, Request, Options) :-
    findall(T, worker(Pool, _, T), Workers),
    alt_wait(Pool, Id, Workers, Request, Options).

alt_wait(_, _, [], _, _) =>
    fail.
alt_wait(Pool, Id, Workers, Request, Options) =>
    queue(Pool, Q),
    (   thread_get_message(Q, result(Id, From, Result), Options)
    ->  (   Result = success(Request0, Updates)
        ->  updates(Updates),
            Request = Request0
        ;   delete(Workers, From, Workers1),
            alt_wait(Pool, Id, Workers1, Request, Options)
        )
    ;   forall(member(TID, Workers),
               thread_signal(TID, cancelled)),
        alt_wait(Pool, Id, Workers, Request, [])
    ).


%!  updates(+Updates) is det.
%
%   Invalidate the tables  that  are  affected   by  the  given  list of
%   actions.

updates(Updates) :-
    maplist(update, Updates).

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
