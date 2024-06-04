Configured and non-configured contexts with the same name will not cause
non-deterministic behavior upon loading anymore. Non-configured contexts that
share the same name with configured contexts will have a random suffix added to
prevent potential data loss.
