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

Work on the next unfinished task in `CLAUDE-TODOS.md`.
  - Implement the assigned step of the plan.
  - If the task includes writing C++ code:
     - Read `./.claude/contexts/cpp-writing.md` before writing any code.
     - Compile the code before marking the task as finished.
  - If the task includes writing TQL code:
     - Instruct it to read the tenzir docs TQL guide before writing any code.
  - Run the relevant tests
  - Tick of the completed task in `CLAUDE-TODOS.md` and update it with any relevant learnings.

After the task is done, ask the user for approval to go to the next task.
Suggest using /clear to reset the context window.

Finally:
  - Run all integration and unit tests and declare the task as being done.
