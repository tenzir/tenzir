This document specifies the coding style of the source code. In general, the
style resembles the [Boost](http://www.boost.org) style syntactically and the
[Google style](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml)
semantically.


Header
======

- Header filenames end in `.h` and implementation filenames in `.cc`.

- All header files should use #define guards to prevent multiple inclusion. The
  format of the symbol name should be `VAST_<PATH>_<TO>_<FILE>_H`.

- Don't use `#include` when a forward declarations suffices. It can make sense to
  outsource forward declarations into a separate file per module. The file name
  should be `<MODULE>/fwd.h`.

- Include order is from low-level to high-level headers, e.g.,

        #include <memory>

        #include <boost/operators.hpp>

        #include "vast/logger.h"
        #include "vast/util/profiler.h"

  Within each section the order should be alphabetical. VAST includes should
  always be in doublequotes and relative to the source directory, whereas
  system-wide includes in angle brackets.

  In the implementation (`*.cc`) file, the top-level include always consist of
  the corresponding header file. Aftewards, the above order applies again. For
  example,

        #include "meta/event.h"

        #include <functional>

- When declaring a function, the order of parameters is: inputs, then outputs.

Classes
=======

- Prefer types with value semantics over reference semantics.

- Use the [rule of zero or rule of
  five](http://en.cppreference.com/w/cpp/language/rule_of_three).

Naming
======

- Types and variables should be nouns, while functions performing an action
  should be "command" verbs. Getter and setter functions should be nouns. We do
  not use an explicit `get_` or `set_` prefix.

- All macros should start with `VAST_` to avoid potential clashes with external
  libraries.

- Names of *(i)* classes/structs, *(ii)* functions, and *(iii)* enums should be
  lower case and delimited by underscores.

- The name of implementation namespaces is `detail`, e.g.,

        vast::detail::some_non_exposed_helper

Comments
========

- Doxygen comments (which are stripped during the generation of the
  documentation) start with `///`.

- Use `@cmd` rather than `\cmd` for Doxygen commands.

- Use `//` or `/*` and `*/` to define basic comments that should not be
  swallowed by Doxygen.
