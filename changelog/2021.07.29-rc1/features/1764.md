To aid debugging, plugin versions are now unique. They consist of three optional
parts: The CMake project version of the plugin, the Git revision of the last
commit that touched the plugin, and a `dirty` suffix for uncommited changes to
the plugin. Plugin developers no longer need to specify the version manually in
the plugin entrypoint.
