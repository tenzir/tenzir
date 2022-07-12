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
"""
import invoke
import os


def is_check_pty_call():
    return "VASTCLOUD_CHECK_PTY" in os.environ


class _Task(invoke.Task):
    """PyInvoke Task that supports PTY checking calls"""

    def __call__(self, *args, **kwargs):
        if is_check_pty_call():
            return "0"
        else:
            return super().__call__(*args, **kwargs)


class _PTYTask(invoke.Task):
    """PyInvoke Task specific to commands that need PTY. Also supports PTY
    checking calls"""

    def __call__(self, *args, **kwargs):
        if is_check_pty_call():
            print("1", end="")
            return ""
        else:
            try:
                args[0].config.run.pty = True
            except:
                print("Failed to set PTY config")
            return super().__call__(*args, **kwargs)


def task(*args, **kwargs):
    """PyInvoke task decorator that supports PTY checking calls"""
    return invoke.task(*args, **kwargs, klass=_Task)


def pty_task(*args, **kwargs):
    """PyInvoke task decorator specific to commands that need PTY. Also supports
    PTY checking calls"""
    return invoke.task(*args, **kwargs, klass=_PTYTask)


# Re-export important classes to keep only one interface to PyInvoke

Exit = invoke.Exit
Context = invoke.Context
