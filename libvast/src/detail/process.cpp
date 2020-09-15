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

#include "vast/detail/process.hpp"

#include "vast/detail/settings.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>

#include <dlfcn.h>

#if VAST_LINUX
#  include "vast/concept/parseable/core/operators.hpp"
#  include "vast/concept/parseable/string/any.hpp"
#  include "vast/concept/parseable/string/char_class.hpp"
#  include "vast/concept/parseable/vast/si.hpp"
#  include "vast/detail/line_range.hpp"

#  include <fstream>
#  include <limits.h>
#  include <unistd.h>

#  include <sys/stat.h>
#elif VAST_MACOS
#  include <unistd.h>

#  include <mach/mach_init.h>
#  include <mach/task.h>
#  include <sys/types.h>
#endif

#if VAST_POSIX && !VAST_LINUX
#  include <sys/resource.h>
#  include <sys/sysctl.h>
#  include <sys/time.h>
#  include <sys/types.h>
#endif

#if VAST_LINUX

namespace vast::detail {

static caf::settings get_status_proc() {
  using namespace parser_literals;
  auto is = std::ifstream{"/proc/self/status"};
  auto lines = detail::line_range{is};
  caf::settings result;
  auto skip = ignore(+parsers::any);
  auto ws = ignore(+parsers::space);
  auto kvp = [&](const char* k, const std::string_view human_friendly, auto v) {
    using T = decltype(v);
    return (k >> ws >> si_parser<T>{} >> skip)->*[=, &result](T x) {
      result[human_friendly] = x;
    };
  };
  auto rss = kvp("VmRSS:", "current-memory-usage", size_t{});
  auto size = kvp("VmHWM:", "peak-memory-usage", size_t{});
  auto swap = kvp("VmSwap:", "swap-space-usage", size_t{});
  auto p = rss | size | swap | skip;
  while (true) {
    lines.next();
    if (lines.done())
      break;
    auto line = lines.get();
    if (!p(line))
      VAST_WARNING_ANON("failed to parse /proc/self/status:", line);
  }
  return result;
}

} // namespace vast::detail
#endif

#if VAST_MACOS

namespace vast::detail {

static caf::settings get_settings_mach() {
  caf::settings result;
  task_t task = MACH_PORT_NULL;
  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
    return {};
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
  task_info(task, TASK_BASIC_INFO, (task_info_t) &t_info, &t_info_count);
  result["current-memory-usage"] = t_info.resident_size;
  return result;
}

} // namespace vast::detail
#endif

#if VAST_POSIX && !VAST_LINUX

namespace vast::detail {

static caf::settings get_status_rusage() {
  caf::settings result;
  struct rusage ru;
  if (getrusage(RUSAGE_SELF, &ru) != 0) {
    VAST_WARNING_ANON(__func__,
                      "failed to obtain rusage:", std::strerror(errno));
    return result;
  }
  result["peak-memory-usage"] = ru.ru_maxrss;
  return result;
}

} // namespace vast::detail
#endif

namespace vast::detail {

namespace {

caf::expected<path> objectpath_dynamic(const void* addr) {
  Dl_info info;
  if (!dladdr(addr, &info))
    return make_error(ec::unspecified, "failed to execute dladdr()");
  if (!info.dli_fname)
    return make_error(ec::unspecified, "addr not in an mmapped region");
  // FIXME: On Linux, if addr is inside the main executable,
  // dli_fname seems to be the same argv[0] instead of the full path.
  // We should detect and return an error in that case.
  return info.dli_fname;
}

caf::expected<path> objectpath_static() {
#ifdef VAST_LINUX
  struct stat sb;
  auto self = "/proc/self/exe";
  if (lstat(self, &sb) == -1)
    return make_error(ec::unspecified, "lstat() returned with error");
  auto size = sb.st_size ? sb.st_size + 1 : PATH_MAX;
  std::vector<char> buf(size);
  if (readlink(self, buf.data(), size) == -1)
    return make_error(ec::unspecified, "readlink() returned with error");
  return path{buf.data()};
#else
  return make_error(ec::unimplemented);
#endif
}

} // namespace

caf::expected<path> objectpath(const void* addr) {
  if (addr == nullptr)
    addr = reinterpret_cast<const void*>(objectpath_dynamic);
  if (auto result = objectpath_dynamic(addr))
    return result;
  return objectpath_static();
}

caf::settings get_status() {
#if VAST_LINUX
  return get_status_proc();
#elif VAST_MACOS
  auto result = get_status_rusage();
  merge_settings(get_settings_mach(), result);
  return result;
#elif VAST_POSIX
  return get_status_rusage();
#else
  VAST_DEBUG_ANON("getting process information not supported");
  // Not implemented.
  return caf::settings{};
#endif
}

} // namespace vast::detail
