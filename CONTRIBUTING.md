This document specifies the coding style for VAST. The style is based on
STL, [Google style][google-style], and [CAF style][caf-style] guidelines.

[google-style]: http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
[caf-style]: https://github.com/actor-framework/actor-framework/blob/master/CONTRIBUTING.md

Git Workflow
============

VAST's git workflow encompasses the following key aspects. (For general git
style guidelines, see https://github.com/agis-/git-style-guide.)

- The `master` branch reflects the latest state of development and should
  always compile.

- Simple bugfixes consisting of a single commit can be pushed to `master`.

- For new features and non-trivial fixes, use *topic branches* that branch off
  `master` with a naming convention of `topic/short-description`. After
  completing work in a topic branch, check the following steps to prepare
  for a merge back into `master`:

  + Squash your commits such that each commit reflects a self-contained change
  + Create a pull request to `master` on github
  + Wait for the results of continuous integration tools and fix any reported
    issues
  + Ask a maintainer to review your work when your changes merge cleanly
  + Address the feedback articulated during the review
  + A maintainer will merge the topic branch into `master` after it passes the
    code review

Commit Messages
---------------

- The first line succinctly summarizes the changes in no more than 50
  characters. It is capitalized and written in and imperative present tense:
  e.g., "Fix bug" as opposed to "Fixes bug" or "Fixed bug".

- The first line does not contain a dot at the end. (Think of it as the header
  of the following description).

- The second line is empty.

- Optional long descriptions as full sentences begin on the third line,
  indented at 72 characters per line.

Coding Style
============

General
-------

- Use 2 spaces per indentation level.

- No tabs, ever.

- No C-style casts, ever.

- 80 characters max per line.

- Minimize vertical whitespace within functions. Use comments to separate
  logical code blocks.

- Namespaces and access modifiers (e.g., `public`) do not increase the
  indentation level.

- The `const` keyword follows after type, e.g., `T const&` as opposed to
  `const T&`.

- `*` and `&` bind to the *type*, e.g., `T const& arg`.

- Always use `auto` to declare a variable unless you cannot initialize it
  immediately or if you actually want a type conversion. In the latter case,
  provide a comment why this conversion is necessary.

- Never use unwrapped, manual resource management such as `new` and `delete`.

- Never use `typedef`; always write `using T = X` in favor of `typedef X T`.

- Keywords are always followed by a whitespace: `if (...)`, `template <...>`,
  `while (...)`, etc.

- Leave a whitespace after `!` to make negations easily recognizable.

  ```cpp
  if (! sunny())
    stay_home()
  ```

- Opening braces belong onto the same line:

  ```cpp
  struct foo {
    void f() {
    }
  };
  ```

- Do not use the `inline` keyword unless to avoid duplicate symbols. The
  compiler does a better job at figuring out what functions should be inlined.

Header
------

- Header filenames end in `.h` and implementation filenames in `.cc`.

- All header files should use #define guards to prevent multiple inclusion. The
  format of the symbol name should be `VAST_<PATH>_<TO>_<FILE>_H`.

- Don't use `#include` when a forward declarations suffices. It can make sense
  to outsource forward declarations into a separate file per module. The file
  name should be `<MODULE>/fwd.h`.

- Include order is from low-level to high-level headers, e.g.,

  ```cpp
  #include <sys/types.h>

  #include <memory>

  #include <3rd/party.hpp>

  #include "vast/logger.h"
  ```

  Within each section the order should be alphabetical. VAST includes should
  always be in doublequotes and relative to the source directory, whereas
  system-wide includes in angle brackets.

- As in the standard library, the order of parameters when declaring a function
  is: inputs, then outputs. API coherence and symmetry trumps this rule, e.g.,
  when the first argument of related functions model the same concept.

Classes
-------

- Use the order `public`, `proctected`, `private` for functions and members in
  classes.

- Mark single-argument constructors as `explicit` to avoid implicit
  conversions.

- The order of member functions within a class is: constructors, operators,
  mutating members, accessors.

- Friends first: put friend declaration immediate after opening the class.

- Put declarations (and/or definitions) of assignment operators right after the
  contrstructors, and all other operators at the bottom of the public section.

- Use structs for state-less classes or when the API is the struct's state.

- Prefer types with value semantics over reference semantics.

- Use the [rule of zero or rule of
  five](http://en.cppreference.com/w/cpp/language/rule_of_three).

- When providing a move constructor and move-assignment operator, declare them
  as `noexcept`.

Naming
------

- Class names, constants, and function names are lowercase with underscores.

- Template parameter types should be written in CamelCase.

- Types and variables should be nouns, while functions performing an action
  should be "command" verbs. Getter and setter functions should be nouns. We do
  not use an explicit `get_` or `set_` prefix. Classes used to implement
  metaprogramming functions also should use verbs, e.g., `remove_const`.

- All library macros should start with `VAST_` to avoid potential clashes with
  external libraries.

- Names of (i) classes/structs, (ii) functions, and (iii) enums should be
  lower case and delimited by underscores.

- Put non-API implementation into namespace `detail`.

- Member variables have an underscore (`_`) as suffix, unless they constitute
  the public interface. Getters and setters use the same member name without
  the suffix.

- Put static non-const variables in an anonymous namespace.

Breaking
--------

- Break constructor initializers after the comma, use two spaces for
  indentation, and place each initializer on its own line (unless you don't
  need to break at all):

  ```cpp
  my_class::my_class()
    : my_base_class{some_function()},
      greeting_{"Hello there! This is my_class!"},
      some_bool_flag_{false} {
    // ok
  }

  other_class::other_class() : name_{"tommy"}, buddy_{"michael"} {
    // ok
  }
  ```

- Break function arguments after the comma for both declaration and invocation:

  ```cpp
  a_rather_long_return_type f(std::string const& x,
                              std::string const& y) {
    // ...
  }
  ```

  If that turns out intractable, break directly after the opening parenthesis:

  ```cpp
  template <typename T>
  black_hole_space_time_warp f(
    typename T::gravitational_field_manager const& manager,
    typename T::antimatter_clustear const& cluster) {
    // ...
  }
  ```

- Break template parameters without indentation:

  ```cpp
  template <class T>
  auto identity(T x) {
    return x;
  }
  ```

- Break trailining return types without indentation if they cannot fit on the
  same line:

  ```cpp
  template <class T>
  auto compute_upper_bound_on_compressed_data(T x)
  -> std::enable_if_t<std::is_integral::value, T> {
    return detail::bound(x);
  }
  ```

- Break before binary and ternary operators:

  ```cpp
  if (today_is_a_sunny_day()
      && it_is_not_too_hot_to_go_swimming()) {
    // ...
  }
  ```

Template Metaprogramming
------------------------

- Use the `typename` keyword only to access dependent types. For general
  template parameters, use `class` instead:

  ```cpp
  template <class T>
  struct foo {
    using type = typename T::type;
  };
  ```

- Use `T` for generic, unconstrained template parameters and `x` for generic
  function arguments. Suffix both with `s` for template parameter packs:

  ```cpp
  template <class T, class... Ts>
  auto f(T x, Ts... xs) {
    // ...
  }
  ```

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
--------

- Doxygen comments start with `///`.

- Use Markdown instead of Doxygen formatters.

- Use `@cmd` rather than `\cmd`.

- Use `//` or `/*` and `*/` to define basic comments that should not be
  swallowed by Doxygen.
