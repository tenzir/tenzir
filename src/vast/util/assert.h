#ifndef VAST_UTIL_ASSERT_H
#define VAST_UTIL_ASSERT_H

#include "vast/config.h"

#ifndef VAST_ENABLE_ASSERTIONS
# define VAST_ASSERT(expr) static_cast<void>(expr)
#elif defined(VAST_WINDOWS)
#  include <cstdio>
#  include <cstdlib>
#  define VAST_ASSERT(expr)                                                 \
   if (static_cast<bool>(expr) == false)                                    \
   {                                                                        \
     ::printf("%s:%u: assertion failed '%s'\n", __FILE__, __LINE__, #expr); \
     ::abort();                                                             \
   }                                                                        \
   static_cast<void>(0)
#else // defined(VAST_LINUX) || defined(VAST_MACOS) || defined(VAST_BSD)
#  include <execinfo.h>
#  include <cstdio>
#  include <cstdlib>
#  define VAST_ASSERT(expr)                                                 \
   if (static_cast<bool>(expr) == false)                                    \
   {                                                                        \
     ::printf("%s:%u: assertion failed '%s'\n", __FILE__, __LINE__, #expr); \
     void* vast_array[10];                                                  \
     auto vast_bt_size = ::backtrace(vast_array, 10);                       \
     ::backtrace_symbols_fd(vast_array, vast_bt_size, 2);                   \
     ::abort();                                                             \
   }                                                                        \
   static_cast<void>(0)
#endif

#endif
