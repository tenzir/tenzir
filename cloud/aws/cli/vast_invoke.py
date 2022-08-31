"""Extended PyInvoke Tasks

This module contains extensions that enable proper handling of the PTY by the
entrypoint script and Docker. We want to attach a PTY only when it is necessary.
To enable this, we need to enable each task describes whether it requires a PTY
or not.

To achieve this, this module adds the capability to run any command in a "PTY
checking" dry run mode:
- this dry run mode is activated by specifying the VASTCLOUD_CHECK_PTY
environment variable.
- for each task in the command, the dry run mode prints 0 if not PTY is required
and 1 otherwise. For a command running 3 tasks, the output might be "100" if only
the first tasks requires a PTY.
- the VASTCLOUD_NO_PTY force-disables the use of PTY, effectively making the dry
mode return 0s only
"""
import invoke
import flags


def update(config):
    """Configs updates that should always apply"""
    # increase input read speed, at the expense of CPU useage
    # the read should be blocking but
    config.runners["local"].input_sleep = 0.0001
    # forward trace flags
    if flags.TRACE:
        config.run.env[flags.TRACE_FLAG_VAR] = "1"


class _Task(invoke.Task):
    """PyInvoke Task that supports PTY checking calls"""

    def __call__(self, *args, **kwargs):
        if flags.CHECK_PTY:
            return "0"
        else:
            # specify no PTY for recursive calls to the CLI
            args[0].config.run.env[flags.NO_PTY_FLAG_VAR] = "1"
            update(args[0].config)
            return super().__call__(*args, **kwargs)


class _PTYTask(invoke.Task):
    """PyInvoke Task specific to commands that need PTY. Also supports PTY
    checking calls"""

    def __call__(self, *args, **kwargs):
        if flags.CHECK_PTY:
            print("1", end="")
            return ""
        else:
            args[0].config.run.pty = True
            update(args[0].config)
            return super().__call__(*args, **kwargs)


def task(*args, **kwargs):
    """PyInvoke task decorator that supports PTY checking calls"""
    return invoke.task(*args, **kwargs, klass=_Task)


def pty_task(*args, **kwargs):
    """PyInvoke task decorator specific to commands that need PTY. Also supports
    PTY checking calls"""
    task_class = _Task if flags.NO_PTY else _PTYTask
    return invoke.task(*args, **kwargs, klass=task_class)


# Re-export important classes to keep only one interface to PyInvoke

Failure = invoke.Failure
Exit = invoke.Exit
Context = invoke.Context
