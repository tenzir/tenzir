VAST will now properly regenerate any corrupted oversized
partition files it encounters on startup, provided that
the corresponding store files are available. These files
could be produced by versions up to and including
VAST 2.2, when using configurations with an increased
maximum partition size.
