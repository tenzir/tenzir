#!/bin/sh

## Usage
# git ls-files -- '*.hpp' '*.cpp' ':!:*logger.?pp' | parallel ./logfmt.sh

# This converter doesn't "handle, commas in strings" correctly or nested calls
# with multiple arguments correctly, the compiler will tell :).

# Manual preparatory steps:
# 1. Eliminate single line logging lambdas
#   [&]{ VAST_*(...);}
#   should become:
#   [&]{
#     VAST_*(...);
#   }
#   -> commit (ignore the style check)
# 2. FMT doesn't like `this` as argument. Most of those are in the `format`
#    directory. `this` should be replaced with an appropriate literal, for
#    example `VAST_*(this, "does thing");` becomes
#    `VAST_*("json.reader does thing);` in `format/json.{c,h}pp`
#   -> commit (still ignore the style check)
# 3. Rebase on latest master!!

# 4. Run the script and clang-format, commit, and do code review.
# 5. After approval
#    -> reset to state (3)
#    -> rebase again
#    -> run the script
#    -> fix CI
#    -> merge to master

sed -ri '
/^\s+VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE)/ {
  ### Normalizations
  # If the statement does not end with the line join the next, repeat until
  # done.
  :join
  /;$/ b done
  N
  # If 1 ends with a string literal and 2 begins with one, join them.
  s/"\s*\n\s*"//
  s/\n\s*/ /
  b join
  :done

  # Rewrite "(self" to "(*self"
  s/(VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE))\(self/\1(*self/
  s/(VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE))\(st.self/\1(*st.self/
  s/(VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE))\(state.self/\1(*state.self/
  s/(VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE))\(state_.self/\1(*state_.self/
  s/(VAST_(ERROR|WARNING|INFO|VERBOSE|DEBUG|TRACE))\(driver_.state.self/\1(*driver_.state.self/

  # Remove _ANON suffixes from log statements.
  s/(VAST_[^_]+)_ANON/\1/

  # Replace character literals with string literals.
  s/, '\''(\\?.)'\''/, "\1"/g

  # Replace VAST_ARG("n", x, y) with "n(", x, y, ")".
  s/VAST_ARG\("([^"]+)", ([^,]+), ([^)]+)\)/"\1(", \2, \3, ")"/g
  # Replace VAST_ARG("n", x) with "n(", x, ")".
  s/VAST_ARG\("([^"]+)", ([^)]+)\)/"\1(", \2, ")"/g
  # Replace VAST_ARG(x) with "x (", x, ")".
  s/VAST_ARG\(([^)]+)\)/"\1(", \1, ")"/g

  # Add ", " before the first argument.
  s/\(/(, /
  # Copy the hold area.
  h

  ### This part prepares the format string.
  #########################################
  # Remove the ); at the end.
  s/\);//
  # Replace normal arguments with {}, keep string literals intact.
  s/, +[^,"]+/ {}/g
  # Remove remaining commas and quotes.
  s/,//g
  s/"//g
  # Add a quote after the opening brace.
  s/\( /("/
  # Append a quote at the end.
  s/$/"/

  ### This part prepares the arguments to the format string.
  ##########################################################
  # Exchange hold and pattern area.
  x
  # Remove everything up to the opening brace.
  s/^[^(]+\(//
  # Remove string literal arguments.
  s/, +"[^"]*"//g
  # Exchange hold and pattern area again to get the left side into PA.
  x
  # Move the content from the hold area and append it to the pattern area.
  G
  s/\n//
}' $1
