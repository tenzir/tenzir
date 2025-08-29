---
description: Work on a non-trivial task in the tenzir repo
allowed-tools: Edit(CLAUDE-TODOS.md)
---

Your task is to implement the following:

<task>
$ARGS
</task>

To do that, proceed as follows.

Then:
  - Create a plan with discrete implementation steps, and checkboxes to mark
    them as completed. Write it into `CLAUDE-TODOS.md`.
  - For tasks that add new TQL operators or functions, first generate documentation,
    and have it approved before generating the implementation.

*IMPORTANT*: Ask the user for explicit confirmation of the plan before proceeding.

Sequentially spawn a subagent to work on each task.
Each agent should:
  - Implement the assigned step of the plan.
  - If the agent is writing C++ code, instruct it to read `./.claude/contexts/cpp-writing.md`
    before writing code.
  - Compile the code
  - Run the relevant tests
  - Tick of the completed task in `CLAUDE-TODOS.md` and update it with any relevant learnings.

Finally:
  - Run all integration and unit tests
