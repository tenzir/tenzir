---
sidebar_position: 1
---

# Manage a pipeline

A pipeline can be in one of the following **states** after you [run
it][run-pipelines]:

- **Created**: the pipeline has just been [deployed][run-pipelines].
- **Running**: the pipeline is actively processing data.
- **Completed**: there is no more data to process.
- **Failed**: an error occurred.
- **Paused**: the user interrupted execution, keeping in-memory state.
- **Stopped**: the user interrupted execution, resetting all in-memory state.

The [app](https://app.tenzir.com/) or [API](/api) allow you to manage the
pipeline lifecycles.

[run-pipelines]: ../run-pipelines/README.md

## Change the state of a pipeline

In the [app](https://app.tenzir.com/overview), an icon visualizes the current
pipeline state. Change a state as follows:

1. Click the checkbox on the left next to the pipeline, or the checkbox in the
   column header to select all pipelines.
2. Click the button corresponding to the desired action, i.e., *Start*, *Pause*,
   *Stop*, or *Delete*.
3. Confirm your selection.

The screenshot below highlights the controls:

![Pipeline Management Controls](app-controls.png)

For the [API](/api), use the following endpoints based on the desired actions:
- Start, pause, and stop:
  [`/pipeline/update`](/api#/paths/~1pipeline~1update/)
- Delete: [`/pipeline/delete`](/api#/paths/~1pipeline~1delete/)

## Understand pipeline state transitions

The diagram below illustrates the various states, where circles correspond to
states and arrows to state transitions:

![Pipeline States](pipeline-states.excalidraw.svg)

The grey buttons indicate the actions you, as a user, can take to transition
into a different state. The orange arrows are transitions that take place
automatically based on system events.
