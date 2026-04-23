# Operator Review Questionnaire

Please review this PR, which ports an operator from the old
pipline executor to the new neo-based executor.

Pay special attention to the following questions:

# General

- Are we using separate threads or executors to bridge external
  systems into the folly actor system? If yes, summarize why that
  is necessary.
  For calls into external libraries, spawn a subagent to investigate
  the external library documentation and link each call site with
  the reference docs for its blocking behavior.
  
- Do we support snapshot handling for this operator? Why or why not?
  If we do, is snapshot size bounded?
  
- Do we support backpressure handling?

- Are there any operators that were already ported to the new executor
  framework that need to solve the same 

# Backwards Compatibility

- Could any existing pipeline use the new operator as a drop-in
  replacement, or were there changes in interface or behavior?
  If so, what are the differences?

# Shutdown Behavior

- What is the shutdown path for an orderly shutdown:
  a) When the source has indicated no more data will be coming?
  b) When a downstream operator signals completion (only applies to non-sink operators)
  
- What is the shutdown path for an extraordinary shutdown,
  e.g. when the user sends a CTRL-C signal to the node?

In addition to the answers for the questions above, create a
table that shows for every object created by the operator who is responsible
for destroying it and where/when that happens.

# Source Operators

- Walk through the operation in high-traffic and low-traffic mode:
  -  In high-traffic mode, are there any unbounded buffers that could
     kill our memory? Any blocking calls that

  - In low-traffic mode, are there any unbounded waits that could kill
    throughput?
    In particular, answer this: Imagine a source that only sends one single
    event, what is the maximum amount of time it can take in the worst
    case until that event is delivered downstream?

# Sink Operators

- For a sink operator, the `prepare_snapshot()` callback should only return
  after all currently pending events are processed, to ensure correctness
  when the snapshot "flows back" along the pipeline. Is that already
  implemented correctly?
    
