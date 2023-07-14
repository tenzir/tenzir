//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/backtrace.hpp"

#include "tenzir/config.hpp"

#if !TENZIR_ENABLE_BACKTRACE

namespace tenzir::detail {

void backtrace() {
}

} // namespace tenzir::detail

#else // TENZIR_ENABLE_BACKTRACE

#  if TENZIR_ENABLE_LIBUNWIND
#    define UNW_LOCAL_ONLY
#    include <cstdio>
#    include <cstdlib>
#    include <cxxabi.h>
#    include <libunwind.h>

namespace tenzir::detail {

// The following code has been adapted from
// https://eli.thegreenplace.net/2015/programmatic-access-to-the-call-stack-in-c
// According to https://eli.thegreenplace.net/pages/about, it is generously
// offered under the unlicense by Eli Bendersky.
void backtrace() {
  constexpr unsigned symbol_buffer_size = 8196;
  unw_cursor_t cursor;
  unw_context_t context;

  // Initialize cursor to current frame for local unwinding.
  unw_getcontext(&context);
  unw_init_local(&cursor, &context);

  // Unwind frames one by one, going up the frame stack.
  while (unw_step(&cursor) > 0) {
    unw_word_t offset, pc;
    unw_get_reg(&cursor, UNW_REG_IP, &pc);
    if (pc == 0)
      break;
    std::fprintf(stderr, "0x%lx:", pc);
    char sym[symbol_buffer_size];
    if (unw_get_proc_name(&cursor, sym, sizeof(sym), &offset) == 0) {
      char* nameptr = sym;
      int status;
      char* demangled = abi::__cxa_demangle(sym, nullptr, nullptr, &status);
      if (status == 0)
        nameptr = demangled;
      std::fprintf(stderr, " (%s+0x%lx)\n", nameptr, offset);
      std::free(demangled);
    } else {
      std::fprintf(stderr, " -- error: unable to obtain symbol name for this "
                           "frame\n");
    }
  }
}

} // namespace tenzir::detail

#  elif TENZIR_ENABLE_LIBBACKTRACE && __has_include(<backtrace.h>)

#    include <backtrace.h>

namespace tenzir::detail {

void backtrace() {
  auto state = backtrace_create_state(nullptr, false, nullptr, nullptr);
  backtrace_print(state, 1, stderr);
}

} // namespace tenzir::detail

#  elif TENZIR_ENABLE_LIBEXECINFO && __has_include(<execinfo.h>)

#    include <execinfo.h>
#    include <unistd.h>

namespace tenzir::detail {

void backtrace() {
  void* tenzir_array[10];
  auto tenzir_bt_size = ::backtrace(tenzir_array, 10);
  ::backtrace_symbols_fd(tenzir_array, tenzir_bt_size, STDERR_FILENO);
}

} // namespace tenzir::detail

#  else

#    error                                                                     \
      "backtrace enabled but neither libunwind, libbacktrace, nor libexecinfo are available"

#  endif

#endif // TENZIR_ENABLE_BACKTRACE
