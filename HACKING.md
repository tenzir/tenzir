This document specifies the coding style for VAST. The style is based on
STL, [Google style][google-style], and [CAF style][caf-style] guidelines.

[google-style]: http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
[caf-style]: https://github.com/actor-framework/actor-framework/blob/master/HACKING.md

General
=======

- 2 spaces per indentation level.

- No tabs, ever.

- 80 characters max per line.

- Minimize veritcal whitespace within functions. Use comments to separate
  logical code blocks.

- Namespaces and access modifiers (e.g., `public`) do not increase the
  indentation level.

- `*` and `&` bind to the *type*, e.g., `const std::string& arg`.

- Always use `auto` to declare a variable unless you cannot initialize it
  immediately or if you actually want a type conversion. In the latter case,
  provide a comment why this conversion is necessary.

- Never use unwrapped, manual resource management such as `new` and `delete`.

- Use `using T = X` in favor of `typedef X T`.

- Keywords are always followed by a whitespace: `if (...)`, `template <...>`,
  `while (...)`, etc.

- Never use C-style casts.

- Do not use the `inline` keyword unless to avoid duplicate symbols. The
  compiler does a better job at figuring out what functions should be inlined.

Header
======

- Header filenames end in `.h` and implementation filenames in `.cc`.

- All header files should use #define guards to prevent multiple inclusion. The
  format of the symbol name should be `VAST_<PATH>_<TO>_<FILE>_H`.

- Don't use `#include` when a forward declarations suffices. It can make sense to
  outsource forward declarations into a separate file per module. The file name
  should be `<MODULE>/fwd.h`.

- Include order is from low-level to high-level headers, e.g.,

        #include <cassert>

        #include <memory>

        #include <3rd/party.hpp>

        #include "vast/logger.h"
        #include "vast/util/profiler.h"

  Within each section the order should be alphabetical. VAST includes should
  always be in doublequotes and relative to the source directory, whereas
  system-wide includes in angle brackets.

- As in the STL, the order of parameters when declaring a function is: inputs,
  then outputs. API coherence and symmetry trumps this rule, e.g., when the
  first argument of related functions model the same concept.

Classes
=======

- Use the order `public`, `proctected`, `private` for functions and members in
  classes.

- The order of member functions within a class is: constructors, operators,
  mutating members, accessors.

- Friends first: put friend declaration immediate after opening the class.

- Use structs for state-less classes or when the API is the struct's state.

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

- Put non-API implementation into namespace `detail`.

- Member variables have an underscore (`_`) as suffix, unless they constitute
  the public interface.

- Put static non-const variables in an anonymous namespace.

Template Metaprogramming
========================

- Break `using name = ...` statements always directly after `=` if they do not
  fit in one line.

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
  of the last opening `<`:
  ```cpp
  using type = quite_a_long_template_which_needs_a_break<std::string,
                                                         double>;
  ```

Comments
========

- Doxygen comments start with `///`.

- Use Markdown instead of Doxygen formatters.

- Use `@cmd` rather than `\cmd`.

- Use `//` or `/*` and `*/` to define basic comments that should not be
  swallowed by Doxygen.
