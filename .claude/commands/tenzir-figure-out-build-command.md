Your task is to prepare the tenzir build command for this worktree, according
to these wishes by the user:

<instructions>
${ARGS}
</instructions>

Add the correct build invocation and the correct invocation to
run the built tenzir binary to `CLAUDE.local.md`. If there are previous
entries, update them with the new settings.
Never commit this change, because `CLAUDE.local.md` is not tracked by git.

# Examples

User: Create a native build for developing on the google secops plugin. Also enable platform integration.
Agent:

User: Prepare a nix build based on v5.14 with the custom caf branch topic/investigate-memory-leak.

# Additonal Context (read carefully)

@./.claude/contexts/build-system.md