---
description: Work on a non-trivial task in the tenzir repo
model: claude-opus-4-1-20250805
allowed-tools: Write(CLAUDE-TODOS.md)
---

Your task is to implement the following:

<task>
$ARGS
</task>

To do that, proceed as follows.

Then:
  - Create a plan with discrete implementation steps, and checkboxes to mark
    them as completed. Write it into CLAUDE-TODOS.md.
    - For tasks that add new TQL operators or function calls, structure the
      implemenation plan so that integration tests are written first, before
      implementing the main feature. Instruct the subagent that writes the
      tests to only write but not execute the tests, since they will fail anyways.
    - For other tasks, add the integration tests after the feature is implemented.

*IMPORTANT*: Ask the user for explicit confirmation of the plan before proceeding.

Sequentially spawn a subagent to work on each task.
Each agent should:
  - Implement the assigned step of the plan.
  - Compile the code
  - Run the relevant tests
  - Update TODOS.md with the progress and any relevant learnings.

Finally:
  - Run all integration and unit tests