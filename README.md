# Try alternative updates strategies concurrently

## Problem statement

  - Given a set of incremental dynamic predicates and dependent incremental tabled
    predicates.
  - On a new request there are N possible strategies for handling the request and
    update the state accordingly.
    - Handling a request implies making changes to the state (dynamic predicates)
      and evaluate the consequences (what-if reasoning).
  - Obviously, a good starting point is to have N threads, each trying to handle
    the request using a different strategy.  These threads must do the reasoning
    inside a _transaction_ to remain independent.   This works well with private
    incremental tabling.

One simply working solution is to kill the unsuccessful workers. Now we
may continue with the winner or we must ensure that committed changes
are properly reflected in the "main" worker's table.

> Note that incremental private tabling does not deal with the possibility that
> some other thread makes changes to the incremental dynamic predicates.  I.e.,
> an assert or retract __only__ invalidates the threads own private tables.
> _Shared_ tabling fixes this.   We cannot use this together with transactions
> and have each thread using its own tables though.

There are two ways to sync our private tables after another thread has
changed the dynamic database:

  - The changing that can use transaction_updates/1 to get a summary of
    the updates.  We can use that to invalidate our own tables.
  - We could copy the affected changes from the consistent thread.  This
    requires new primitives.

If computing the tables is expensive and only some of them are outdated
during the computation it is not attractive to kill the unsuccessful
workers. we rather keep them around and update them using one of the
same strategies as above.


