# Asynchronous Code Guidelines

Asynchronous code can be tricky to reason about. To contain this complexity, we
want to ensure the following properties:

## Cancellation awareness

All asynchronous code needs to handle cancellation. That is: It must be possible
to abort the running task from the outside by cancelling the task. Let's take
`folly::coro::Mutex` as a negative example. Its locking methods are not
cancellable. As a result, cancelling a function that uses a mutex does not
immediately do anything. We are stuck until locking eventually succeeds (which
might also never happen). Even if we eventually succeed, the coroutine continues
execution, and unless we happen to execute a cancellation-aware function, we
will never be informed that the coroutine was already cancelled a long time ago.

## Cancellation safety

If a coroutine is canceled, it must leave the system in a consistent state. That
is: It must uphold all invariants and safety requirements expected by other
components of the system. For example, a mutex locked across a cancellation
point needs to be unlocked when the coroutine is cancelled. If the coroutine
spawned another task, it must ensure that there are either no lifetime
dependencies or that the associated task also completes.

## Exception safety

The same must hold true for exceptions, which in particular includes panics. In
a sense, this describes a superset of the previous paragraph, as cancellation is
typically propagated through the `folly::OperationCancelled` exception.

## Cancellation consistency

When an asynchronous operation is canceled, it ideally leaves the system in a
state as-if the operation never happened. This is for example critical if a
reading operation on a queue is cancelled. If the dequeuing function can still
reach a cancellation point after performing the actual dequeuing, then
cancelling the task means that the item is lost. Composition of
cancellation-consistent components is not necessarily cancellation-consistent.
Also, whether something is cancellation consistent depends on the overall
context. For example, our previous example might still be safe if we know that
the cancelled task is the only one reading from the queue and that no other task
will attempt to read from the queue in the future.

## Fairness

When multiple concurrent tasks are involved, it is typically desirable for all
tasks to be able to make progress (if they can), ideally with equal pace. This
implies that coroutines that shell out work to a set of other components should
typically not heavily prefer a single other component. Furthermore, access to
shared resources should typically be granted in a FIFO fashion, such that longer
waiting tasks are preferred.

## Partial Termination

Communication between multiple tasks should be set up in a way where, if one
task ends (due to cancellation or exception, or simply because of programmer
error), the task that is depending on the communication is notified. For
example, a channel between a set of tasks should make the readers aware if all
writers have been dropped, and the writers aware if all readers have been
dropped. This is important to ensure that partial termination does not lead to
buffers filling up, or even deadlocks in the case of bounded channels.
