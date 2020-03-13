Auxiliary Third-Party Software
==============================

This directory contains the third-party software VAST uses. We manage it via
`git subtree` or `git submodule`.

Repositories
------------

### submodules
- [CAF](https://github.com/actor-framework/actor-framework) https://github.com/actor-framework/actor-framework
- [broker](https://github.com/zeek/broker) https://github.com/zeek/broker

### subtrees
- [lz4](https://github.com/lz4/lz4): git@github.com:lz4/lz4.git
- [robin-map](https://github.com/Tessil/robin-map/): git@github.com:Tessil/robin-map.git
- [xxHash](https://github.com/Cyan4973/xxHash): git@github.com:Cyan4973/xxHash.git

Adding a New Repository
-----------------------

To add a new repository **foo** at location `REMOTE` (e.g.,
`git@github.com:.../foo.git`), perform the following steps:

1. Add a new mapping above that specifies the remote location

### submodules
2. Add the repository as a submodule:

       git submodule add REMOTE aux/foo
       git -C aux/foo checkout COMMIT
       git commit -a -m 'Add 3rd-party library foo'

### subtrees
2. Add the repository as a subtree:

       git subtree add --prefix=aux/foo --squash REMOTE master

Synchronize an Existing Repository
----------------------------------

To update an existing repository **foo**, perform the following steps:

### submodules
1. Run `git -C aux/foo fetch` to synchronize with the remote
2. Check out the commit that should be tracked:

       git -C aux/foo checkout COMMIT

3. Go to the top-level directory of the VAST repository
4. Create a commit for the updated submodule reference

       git commit -a -m 'Update aux/foo'

### subtrees
1. Locate the remote location for foo (e.g., REMOTE)
2. Go to the top-level directory of the VAST repository
3. Pull from the remote repository:

       git subtree pull --prefix aux/foo REMOTE master --squash

4. Commit your changes:

       git commit -a -m 'Update aux/foo'

### Both

5. Compile VAST, run the unit tests, and perform potentially necessary
   adaptations resulting from the update
