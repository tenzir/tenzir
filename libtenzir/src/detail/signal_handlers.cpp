//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <backtrace.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cxxabi.h>
#include <ucontext.h>
#include <unistd.h>

namespace {

// Async-signal-safe write wrapper.
void safe_write(const char* str) {
  if (str) {
    auto len = __builtin_strlen(str);
    (void)write(STDERR_FILENO, str, len);
  }
}

void safe_write(const char* str, size_t len) {
  (void)write(STDERR_FILENO, str, len);
}

// Global backtrace state, initialized once at startup.
backtrace_state* bt_state = nullptr;

void bt_error_callback(void* /*data*/, const char* msg, int /*errnum*/) {
  safe_write("  backtrace error: ");
  safe_write(msg);
  safe_write("\n");
}

// Try to demangle a C++ symbol. Returns the demangled name on success,
// or nullptr if demangling fails or the symbol is not mangled.
// The caller must free() the returned pointer.
[[maybe_unused]]
char* try_demangle(const char* mangled) {
  if (not mangled || mangled[0] != '_' || mangled[1] != 'Z') {
    return nullptr;
  }
  int status = 0;
  char* demangled = abi::__cxa_demangle(mangled, nullptr, nullptr, &status);
  return (status == 0) ? demangled : nullptr;
}

// Helper to format and print a single frame.
void print_frame(uintptr_t pc, const char* filename, int lineno,
                 const char* function) {
  char buf[1024];
  int n;
  // Try to demangle the function name.
  char* demangled = try_demangle(function);
  const char* func_name = demangled ? demangled : function;
  if (filename && func_name) {
    n = snprintf(buf, sizeof(buf), "  %s:%d %s\n", filename, lineno, func_name);
  } else if (func_name) {
    n = snprintf(buf, sizeof(buf), "  0x%lx %s\n",
                 static_cast<unsigned long>(pc), func_name);
  } else {
    n = snprintf(buf, sizeof(buf), "  0x%lx\n", static_cast<unsigned long>(pc));
  }
  // Ensure we don't write past the buffer (snprintf returns what it would
  // have written if there was enough space).
  if (n > 0) {
    size_t len = static_cast<size_t>(n);
    if (len >= sizeof(buf)) {
      len = sizeof(buf) - 1;
    }
    safe_write(buf, len);
  }
  free(demangled);
}

// Callback for crash location - only prints the first (innermost) frame.
int bt_crash_callback(void* data, uintptr_t pc, const char* filename,
                      int lineno, const char* function) {
  auto* printed = static_cast<bool*>(data);
  if (*printed) {
    return 0; // Skip subsequent inlined frames for crash location
  }
  *printed = true;
  print_frame(pc, filename, lineno, function);
  return 0;
}

// Callback for backtrace - prints all frames including inlined ones.
int bt_full_callback(void* /*data*/, uintptr_t pc, const char* filename,
                     int lineno, const char* function) {
  print_frame(pc, filename, lineno, function);
  return 0;
}

// Signal handler for fatal signals (SIGSEGV, SIGABRT).
// Uses SA_SIGINFO to get detailed signal information and extracts the
// instruction pointer and frame pointer from the signal context to produce
// an accurate backtrace of where the crash occurred.
extern "C" void fatal_signal_handler(int sig, siginfo_t* si, void* vctx) {
  auto* ctx = static_cast<ucontext_t*>(vctx);
#if defined(__x86_64__)
  auto pc = static_cast<uintptr_t>(ctx->uc_mcontext.gregs[REG_RIP]);
  auto bp = reinterpret_cast<uintptr_t*>(ctx->uc_mcontext.gregs[REG_RBP]);
#elif defined(__aarch64__)
  auto pc = static_cast<uintptr_t>(ctx->uc_mcontext.pc);
  auto bp = reinterpret_cast<uintptr_t*>(ctx->uc_mcontext.regs[29]); // x29 = FP
#else
  uintptr_t pc = 0;
  uintptr_t* bp = nullptr;
#endif

  // Print signal info header.
  char buf[256];
  int n
    = snprintf(buf, sizeof(buf),
               "\n*** Fatal signal %d (si_code=%d) ***\n"
               "Fault address: %p\n"
               "Instruction pointer: 0x%lx\n",
               sig, si->si_code, si->si_addr, static_cast<unsigned long>(pc));
  if (n > 0) {
    size_t len = static_cast<size_t>(n);
    if (len >= sizeof(buf)) {
      len = sizeof(buf) - 1;
    }
    safe_write(buf, len);
  }

  // Try to resolve the faulting PC to a source location.
  // Use bt_crash_callback to only print the innermost frame (not all inlined).
  if (bt_state && pc != 0) {
    safe_write("Crash location:\n");
    bool printed = false;
    backtrace_pcinfo(bt_state, pc, bt_crash_callback, bt_error_callback,
                     &printed);
  }

  // Walk the stack manually using the frame pointer from the signal context.
  // This gives us the actual call stack at the time of the crash, not the
  // signal handler's stack.
  if (bt_state && bp != nullptr) {
    safe_write("\nBacktrace:\n");
    constexpr int max_frames = 64;
    for (int i = 0; i < max_frames && bp != nullptr; ++i) {
      // On x86_64 with frame pointers:
      // bp[0] = previous frame pointer
      // bp[1] = return address
      auto return_addr = bp[1];
      if (return_addr == 0) {
        break;
      }
      backtrace_pcinfo(bt_state, return_addr, bt_full_callback,
                       bt_error_callback, nullptr);
      // Move to previous frame
      auto* next_bp = reinterpret_cast<uintptr_t*>(bp[0]);
      // Sanity check: frame pointer should increase (stack grows down)
      if (next_bp <= bp) {
        break;
      }
      bp = next_bp;
    }
  }
  // Return. With SA_RESETHAND, the kernel will immediately re-signal with
  // default action, producing a core with the original fault context.
}

// Use constructor attribute with high priority (lower number = earlier)
// to install signal handlers before any other static initializers run.
// Priority 101 is the earliest user-available priority (1-100 are reserved).
__attribute__((constructor(101))) void early_install_signal_handlers() {
  // Initialize libbacktrace state for stacktrace generation.
  bt_state = backtrace_create_state(nullptr, 0, bt_error_callback, nullptr);

  // Set up alternate signal stack to handle stack overflow scenarios.
  stack_t ss{};
  ss.ss_sp = malloc(SIGSTKSZ);
  ss.ss_size = SIGSTKSZ;
  sigaltstack(&ss, nullptr);

  struct sigaction sa{};
  sa.sa_sigaction = fatal_signal_handler;
  sa.sa_flags = SA_SIGINFO | SA_ONSTACK | SA_RESETHAND;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGSEGV, &sa, nullptr);
  sigaction(SIGABRT, &sa, nullptr);
}

} // namespace

// This function exists solely to ensure the object file is not discarded
// during static linking. The signal handlers are installed automatically
// via the constructor attribute above.
void signal_handlers_anchor() {
  // Intentionally empty.
}
