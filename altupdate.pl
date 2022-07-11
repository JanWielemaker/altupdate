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
          [ alt_create_pool/2,    % ++Pool, +Options
            alt_close/1,          % ++Pool
            alt_submit/2,         % ++Pool, :Goal
            alt_wait/3,           % ++Pool, -Goal, +Options
            alt_peek/2,           % ++Pool, -Goal
            alt_cancel/1,         % ++Pool
            alt_property/2        % ?Pool, :Property
          ]).
:- use_module(library(apply)).
:- use_module(library(increval)).
:- use_module(library(debug)).
:- use_module(library(option)).
:- use_module(library(error)).
:- use_module(library(aggregate)).

:- meta_predicate
    alt_submit(+, 0),
    alt_submit(+, 0, -),
    alt_wait(+, 0, +),
    alt_wait(+, +, 0, +),
    alt_peek(+, 0),
    alt_peek(+, 0),
    alt_property(?, :).

/** <module> Concurrent evaluation of alternative strategies

This library deals with  trying  alternative   strategies  to  satisfy a
request by changing the state of a   set of dynamic predicates. In other
words:

  - Give a state represented as a set of dynamic predicates
  - Advance the state.  Each step involves a _synchronization point_
  - To get from one _synchronization poin_ to the next, we
    - Submit goals using alt_submit/2.  Multiple goals may be
      submitted.  The Goals are handled concurrently by a set of
      threads (the _workers_).  If there are more submitted goals
      than workers, the goals are queued and started in the order
      in which they are queued.
    - At any point in time, the user may
      - Submit new goals using alt_submit/2.
      - Use alt_peek/2 to see whether some goal succeeded or all
        failed.
      - Use alt_wait/3 to wait for completion (forever or for a
        limited time).
      - Use alt_cancel/1 to cancel this attempt.

In addition, we satisfy the following requirements:

  - The strategies may modify the state during their search.  They
    do not see each other modifications.  The accepted solution
    becomes visible _atomically_.
  - The dynamic predicates can be _incremental_ and be connected
    using _incremental_ _private_ tables.
  - Each _worker_ maintains consistency with the global view
    before starting a new request.
*/

:- dynamic
    pool/1,             % Pool
    worker/2,		% Pool, Thread
    active/2,           % Pool, Pid
    done/3,             % Pool, Pid, Goal
    queued/3,           % Pool, Id, Goal
    status/3,           % Pool, Id, Status
    needs_update/2.     % Pool, TID

%!  alt_create_pool(++Pool, +Options)
%
%   Create a set of worker  threads   for  processing  requests. Options
%   processed:
%
%     - threads(N)
%       Use a worker pool with N workers.  Default is the Prolog flag
%       `cpu_count`.

alt_create_pool(Pool, _Options) :-
    pool(Pool),
    !,
    permission_error(create, alt_pool, Pool).
alt_create_pool(Pool, Options) :-
    must_be(ground, Pool),
    (   option(threads(Threads), Options)
    ->  true
    ;   current_prolog_flag(cpu_count, Threads)
    ),
    assert(pool(Pool)),
    forall(between(1, Threads, _),
           alt_add_worker(Pool)).

%!  alt_add_worker(++Pool) is det.
%
%   Add a new worker to Pool.

alt_add_worker(Pool) :-
    must_be(ground, Pool),
    thread_create(alt_worker(Pool), TID, []),
    asserta(worker(Pool, TID)).

%!  alt_close(+Pool)
%
%   Terminate the workers.

alt_close(Pool) :-
    must_be(ground, Pool),
    alt_cancel(Pool),
    retractall(pool(Pool)),
    retractall(active(Pool, _)),
    retractall(needs_update(_,_)),
    retractall(queued(Pool, _, _)),
    retractall(status(Pool, _, _)),
    retractall(done(Pool, _, _)),
    forall(worker(Pool, TID),
           thread_send_message(TID, quit)),
    forall(retract(worker(Pool, TID)),
           thread_join(TID)).

:- meta_predicate
    request(0, -),
    run(0).

%!  alt_worker(++Pool)
%
%   Implement the worker loop. A worker   listens  for new queued goals,
%   request to update (invalidate) tables and a request to quit.

alt_worker(Pool) :-
    thread_wait(worker_request(Pool, Request),
                [ wait_preds([ +(queued/3),
                               +(needs_update/2),
                               -(pool/1)
                             ])
                ]),
    (   Request == quit
    ->  true
    ;   dispatch(Pool, Request),
        alt_worker(Pool)
    ).

worker_request(Pool, Request), retract(queued(Pool, Goal, Id)) =>
    \+ done(Pool, _, _),
    Request = run(Goal, Id),
    thread_self(On),
    asserta(status(Pool, Id, running(Goal, On))).
worker_request(Pool, Request), needs_update =>
    (   status(Pool, _, success(_Goal, Updates))
    ->  Request = updates(Updates)
    ;   Request = updates([])
    ).
worker_request(Pool, Request), \+ pool(Pool) =>
    Request = quit.
worker_request(_, _) =>
    fail.

%!  dispatch(+Pool, +Request) is det.
%
%   Dispatch a request to a worker. The two possible requests are (1) to
%   run a goal and (2) to invalidate   tables  because some other thread
%   has updated the database.

dispatch(Pool, run(Goal, Id)) =>
    thread_self(Me),
    (   catch(transaction(request(run(Goal), Changes),
                          only_first(Pool, Id),
                          altupdate_mutex),
              Exception,
              true)
    ->  (   var(Exception), Changes \== failed
        ->  debug(alt, 'Won!  Committing ~p', [Changes]),
            send_changes(Pool, Me, Id, Goal, Changes)
        ;   var(Exception)
        ->  debug(alt, 'Lost (too late)', []),
            set_status(Pool, Id, lost(Goal))
        ;   Exception = lost_from(_)
        ->  debug(alt, 'Lost (aborted)', []),
            set_status(Pool, Id, lost(Goal))
        ;   Exception = cancelled
        ->  debug(alt, 'Cancelled', []),
            set_status(Pool, Id, cancelled(Goal))
        ;   debug(alt, 'Exception: ~p', [Exception]),
            set_status(Pool, Id, error(Goal, Exception))
        )
    ;   debug(alt, 'Failed', []),
        set_status(Pool, Id, failed(Goal))
    ).
dispatch(Pool, updates(Updates)) =>
    updates(Updates),
    thread_self(TID),
    retract(needs_update(Pool, TID)).

request(Goal, Changes) :-
    call(Goal),
    !,
    transaction_updates(Changes).
request(_, failed).

run(Goal) :-
    call(Goal),
    !.
run(_) :-
    fail.

only_first(Pool, Id) :-
    active(Pool, Pid),
    \+ done(Pool, Pid, _),
    assert(done(Pool, Pid, Id)).

set_status(Pool, Id, Status) :-
    retractall(status(Pool, Id, _)),
    asserta(status(Pool, Id, Status)).

%!  send_changes(+Pool, +Me, +Id, +Goal, +Updates) is det.
%
%   Send updates to the other workers such  that they can invalidate the
%   relevant tables. This also sets a  message ready for alt_wait/3 with
%   the instantiated Request and the Updates to update their tables.

send_changes(Pool, Me, Id, Goal, Updates) :-
    set_status(Pool, Id, success(Goal, Updates)),
    forall(worker(Pool, TID),
           (   TID == Me
           ->  true
           ;   status(Pool, _Id, running(_, TID))
           ->  thread_signal(TID, lost_from(Me)),
               asserta(needs_update(Pool, TID))
           ;   asserta(needs_update(Pool, TID))
           )).

needs_update :-
    thread_self(TID),
    needs_update(_Pool, TID).

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
    prolog_frame_attribute(Frame, parent_goal, run(_)).

%!  alt_submit(+Pool, :Goal) is det.
%
%   Post a Goal on  Pool.  This   predicate  succeeds  immediately.  Use
%   alt_peek/2 to check the satus or alt_wait/3  to wait for the request
%   to finish.

alt_submit(Pool, Goal) :-
    alt_submit(Pool, Goal, _Id).

alt_submit(Pool, Goal, _Id) :-
    must_be(ground, Pool),
    \+ worker(Pool, _),
    !,
    existence_error(Goal, worker_pool).
alt_submit(Pool, Goal, Pid) :-
    flag(alt_request_goal_id, Id, Id+1),
    (   active(Pool, Pid)
    ->  true
    ;   flag(alt_request_update_id, Pid, Pid+1),
        asserta(active(Pool, Pid))
    ),
    assertz(queued(Pool, Goal, Id)).

%!  alt_wait(+Pool, -Goal, +Options) is semidet.
%
%   Wait on Pool for one of the   submitted goals to succeed. Unify Goal
%   with the possibly instantiated Goal that   won the race. Options are
%   passed  to  thread_wait/3,  where    notably  timeout(+Seconds)  and
%   deadline(+AbdTime) are useful.
%
%   If all submitted goals have failed   the predicate succeeds, binding
%   Goal to `false`. If a specified   timeout  is reached this predicate
%   fails. The pending tasks are remain active and thus new goals may be
%   submitted or we may decide to keep   waiting or call alt_cancel/1 to
%   terminate the ongoing task.
%
%   @error permission_error(wait, pool, Pool) if no goals were posted to
%   Pool.

alt_wait(Pool, Goal, Options) :-
    alt_wait(Pool, _Pid, Goal, Options).

alt_wait(Pool, Pid, Goal, Options) :-
    must_be(ground, Pool),
    select_option(timeout(TMO), Options, Options1),
    !,
    get_time(Now),
    Deadline is Now+TMO,
    alt_wait_(Pool, Pid, Goal,  [deadline(Deadline)|Options1]).
alt_wait(Pool, Pid, Goal, Options) :-
    alt_wait_(Pool, Pid, Goal, Options).

alt_wait_(Pool, Pid, Goal, Options) :-
    active(Pool, Pid),
    !,
    thread_wait(alt_peek_(Pool, Pid, Goal, Updates),
                [ wait_preds([ +(status/3)
                             ])
                | Options
                ]),
    alt_sync(Pool, Pid, Updates).
alt_wait_(Pool, _Pid, _Goal, _Options) :-
    permission_error(wait, pool, Pool).

%!  alt_peek(++Pool, :Goal) is semidet.
%
%   True when Pool was  successfully  completed   by  Goal.  If all pool
%   workers have failed, alt_peek/3 unifies Goal with `false`. If one or
%   more workers are still working alt_peek/3 fails.
%
%   @error permission_error(peek, pool, Pool) if no goals were posted to
%   Pool.


alt_peek(Pool, Goal) :-
    alt_peek(Pool, _Pid, Goal).

alt_peek(Pool, Pid, Goal) :-
    alt_peek_(Pool, Pid, Goal, Updates),
    !,
    alt_sync(Pool, Pid, Updates).
alt_peek(Pool, Pid, Goal) :-
    active(Pool, Pid),
    !,
    (   alt_property(Pool, running)
    ->  fail
    ;   alt_sync(Pool, Pid),
        Goal = _:false
    ).
alt_peek(Pool, _Pid, _Goal) :-
    permission_error(peek, pool, Pool).

alt_peek_(Pool, Pid, Goal, Updates) :-
    done(Pool, Pid, Id),
    status(Pool, Id, success(Goal, Updates)).

%!  alt_cancel(+Pool) is det.
%
%   Cancel all activities on Pool and wait for synchronization.

alt_cancel(Pool) :-
    active(Pool, Pid),
    !,
    must_be(ground, Pool),
    assert(done(Pool, Pid, cancelled)),
    forall(status(Pool, _Id, running(_, TID)),
           thread_signal(TID, cancelled)),
    alt_sync(Pool, Pid).
alt_cancel(_).

%!  alt_sync(+Pool, +Pid) is det.
%
%   Wait for all threads to complete and handle the updates.

alt_sync(Pool, Pid, Updates) :-
    updates(Updates),
    alt_sync(Pool, Pid).

alt_sync(Pool, Pid) :-
    thread_wait(\+ needs_update(Pool, _),
                [ wait_preds([ -(needs_update/2)
                             ])
                ]),
    retractall(active(Pool, Pid)),
    retractall(done(Pool, Pid, _Id)),
    retractall(status(Pool, _, _)),
    retractall(queued(Pool, _, _)).

%!  alt_property(?Pool, :Property) is nondet.
%
%   True when Property is a property of Pool.  Defined properties:
%
%     - active
%       True when jobs are submitted and not yet accepted using
%       a successful alt_peek/3 or alt_wait/4.
%     - running
%       True when at least one worker is still running or a goal
%       is scheduled to run.
%     - running(Goal, Threads)
%       True when Goal is still running on Thread.  May succeed
%       multiple times.
%     - status(Status)
%       True when a triggered goal has Status.  Status is one of
%       - running(Goal, Thread)
%         Goal is still running
%       - success(Goal, Updates)
%         Goal succeeded and won the race.  Updates are the
%         updates to the dynamic predicates from the transaction.
%       - failed(Goal)
%         Goal failed
%       - error(Goal, Exception)
%         Goal raised Exception
%       - lost(Goal)
%         Goal was terminated by the winning thread.
%       - cancelled(Goal)
%         Goal was cancelled by alt_cancel/1.

alt_property(Pool, M:Property) :-
    pool(Pool),
    alt_property(Property, Pool, M).

alt_property(workers(N), Pool, _) :-
    aggregate_all(count, worker(Pool, _), N).
alt_property(active, Pool, _) :-
    active(Pool, _Pid).
alt_property(running, Pool, _) :-
    (   status(Pool, _, running(_Goal, _Tid))
    ->  true
    ;   queued(Pool, _, _)
    ).
alt_property(running(Goal, Thread), Pool, M) :-
    status(Pool, _, running(QGoal, Thread)),
    unqualify(QGoal, M, Goal).
alt_property(status(Status), Pool, M) :-
    status(Pool, _, Status0),
    unqualify_status(Status0, M, Status).

unqualify_status(running(QGoal, Thread), M, Status) =>
    unqualify(QGoal, M, Goal),
    Status = running(Goal, Thread).
unqualify_status(success(QGoal, Updates), M, Status) =>
    unqualify(QGoal, M, Goal),
    Status = success(Goal, Updates).
unqualify_status(failed(QGoal), M, Status) =>
    unqualify(QGoal, M, Goal),
    Status = failed(Goal).
unqualify_status(cancelled(QGoal), M, Status) =>
    unqualify(QGoal, M, Goal),
    Status = cancelled(Goal).
unqualify_status(error(QGoal, Exception), M, Status) =>
    unqualify(QGoal, M, Goal),
    Status = error(Goal, Exception).
unqualify_status(Status0, _, Status) =>
    Status = Status0.

unqualify(M:Goal, M, G) =>
    G = Goal.
unqualify(Goal, _, G) =>
    G = Goal.

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
