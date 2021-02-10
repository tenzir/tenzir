/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/detail/backtrace.hpp"

#include "vast/config.hpp"

#if !VAST_ENABLE_BACKTRACE
void backtrace(){};
#else
#  if __has_include(<execinfo.h>)
#    include <execinfo.h>
#    include <unistd.h>

namespace vast::detail {

void backtrace() {
  void* vast_array[10];
  auto vast_bt_size = ::backtrace(vast_array, 10);
  ::backtrace_symbols_fd(vast_array, vast_bt_size, STDERR_FILENO);
}

} // namespace vast::detail
#  elif __has_include(<libunwind.h>)
#    define UNW_LOCAL_ONLY
#    include <cstdio>
#    include <cstdlib>
#    include <cxxabi.h>
#    include <libunwind.h>

namespace vast::detail {

void backtrace() {
  unw_cursor_t cursor;
  unw_context_t context;

  // Initialize cursor to current frame for local unwinding.
  unw_getcontext(&context);
  unw_init_local(&cursor, &context);

  // Unwind frames one by one, going up the frame stack.
  while (unw_step(&cursor) > 0) {
    unw_word_t offset, pc;
    unw_get_reg(&cursor, UNW_REG_IP, &pc);
    if (pc == 0) {
      break;
    }
    std::fprintf(stderr, "0x%lx:", pc);

    char sym[256];
    if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
      char* nameptr = sym;
      int status;
      char* demangled = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
      if (status == 0) {
        nameptr = demangled;
      }
      std::fprintf(stderr, " (%s+0x%lx)\n", nameptr, offset);
      std::free(demangled);
    } else {
      std::fprintf(stderr, " -- error: unable to obtain symbol name for this "
                           "frame\n");
    }
  }
}

} // namespace vast::detail
#  else
#    error "backtrace enabled but neither execinfo nor unwind are available"
#  endif
#endif
