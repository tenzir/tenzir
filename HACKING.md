This document specifies the coding style used throughout VAST. In general, the
style draws inspiration from the STL, the [Google
style](http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml), and
[CAF
style](https://github.com/actor-framework/actor-framework/blob/master/HACKING.md).

General
=======

- Use 2 spaces per indentation level, no tabs, 80 characters max per line.

- Namespaces and access modifiers (e.g., `public`) do not increase the
  indentation level.

- `*` and `&` bind to the *type*, e.g., `const std::string& arg`.

- Always use `auto` to declare a variable unless you cannot initialize it
  immediately or if you actually want a type conversion. In the latter case,
  provide a comment why this conversion is necessary.

- Never use unwrapped, manual resource management such as `new` and `delete`.

- Prefer `using T = X` in favor of `typedef X T`.

- Keywords are always followed by a whitespace: `if (...)`, `template <...>`,
  `while (...)`, etc.


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

- When providing a move constructor and move-assignment operator, declare them
  as `noexcept`.

Naming
======

- Class names, constants, and function names are lowercase with underscores.

- Template parameter types should be written in CamelCase.

- Types and variables should be nouns, while functions performing an action
  should be "command" verbs. Getter and setter functions should be nouns. We do
  not use an explicit `get_` or `set_` prefix. Classes used to implement
  metaprogramming functions also should use verbs, e.g., `remove_const`.

- All library macros should start with `VAST_` to avoid potential clashes with
  external libraries.

- Names of *(i)* classes/structs, *(ii)* functions, and *(iii)* enums should be
  lower case and delimited by underscores.

- The name of implementation namespaces is `detail`, e.g.,

        vast::detail::some_non_exposed_helper

- Member variables should have an underscore (`_`) as suffix, unless they
  are part of the public interface.

Template Metaprogramming
========================

- Break `using name = ...` statements always directly after `=` if it
  does not fit in one line.

- Use one level of indentation per "open" template and place the closing `>`,
  `>::type` or `>::value` on its own line. For example:
  ```cpp
  
  using optional_result_type = 
    typename std::conditional<
      std::is_same<result_type, void>::value,
      bool,
      optional<result_type>
    >::type;
  ```

- When dealing with "ordinary" templates, use indentation based on the position
  of the opening `<`:
  ```cpp

  using type = quite_a_long_template_which_needs_a_break<std::string,
                                                         double>;
  ```


Comments
========

- Doxygen comments (which are stripped during the generation of the
  documentation) start with `///`.

- Use `@cmd` rather than `\cmd` for Doxygen commands.

- Use `//` or `/*` and `*/` to define basic comments that should not be
  swallowed by Doxygen.
