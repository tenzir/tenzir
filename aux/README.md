Auxiliary Third-Party Software
==============================

This directory contains the third-party software VAST uses. We manage it via
`git subtree`.

Repositories
------------

- **date**: `git@github.com:HowardHinnant/date.git`

Adding a New Repository
-----------------------

To add a new repository **foo** at location `REMOTE` (e.g.,
`git@github.com:.../foo.git`), perform the following steps:

1. Add a new mapping above that specifies the remote location
2. Add the repository as a subtree:

       git remote add aux/foo REMOTE
       git subtree add --prefix=aux/foo --squash REMOTE master
       git commit -a -m 'Add 3rd-party library foo'
       git push

Synchronize an Existing Repository
----------------------------------

To update an existing repository **foo**, perform the following steps:

1. Locate the remote location for foo (e.g., REMOTE)
2. Go to the top-level directory of the VAST repository
3. Pull from the remote repository:

       git subtree pull --prefix aux/foo REMOTE master --squash

4. Commit your changes:

       git commit -a -m 'Update aux/foo'

5. Compile VAST, run the unit tests, and perform potentially necessary
   adaptations resulting from the update
