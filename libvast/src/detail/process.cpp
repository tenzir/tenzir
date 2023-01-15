//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/process.hpp"

#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/status.hpp"

#include <caf/expected.hpp>

#include <dlfcn.h>

#if VAST_LINUX
#  include "vast/concept/parseable/core/operators.hpp"
#  include "vast/concept/parseable/string/any.hpp"
#  include "vast/concept/parseable/string/char_class.hpp"
#  include "vast/concept/parseable/vast/si.hpp"
#  include "vast/detail/line_range.hpp"

#  include <sys/stat.h>

#  include <fstream>
#  include <limits.h>
#  include <unistd.h>
#elif VAST_MACOS
#  include <mach/mach_init.h>
#  include <mach/task.h>
#  include <sys/types.h>

#  include <unistd.h>
#endif

#if VAST_POSIX && !VAST_LINUX
#  include <sys/resource.h>
#  include <sys/sysctl.h>
#  include <sys/time.h>
#  include <sys/types.h>
#endif

#if VAST_LINUX

namespace vast::detail {

static record get_status_proc() {
  using namespace parser_literals;
  auto is = std::ifstream{"/proc/self/status"};
  auto lines = detail::line_range{is};
  record result;
  auto skip = ignore(+parsers::any);
  auto ws = ignore(+parsers::space);
  auto kvp = [&](const char* k, const std::string_view human_friendly) {
    // The output says "kB", but the unit is actually "kiB".
    return (k >> ws >> parsers::u64 >> ws >> "kB")->*[=, &result](uint64_t x) {
      result[human_friendly] = uint64_t{x * 1024};
    };
  };
  auto rss = kvp("VmRSS:", "current-memory-usage");
  auto size = kvp("VmHWM:", "peak-memory-usage");
  auto swap = kvp("VmSwap:", "swap-space-usage");
  auto p = rss | size | swap | skip;
  while (true) {
    lines.next();
    if (lines.done())
      break;
    auto line = lines.get();
    if (!p(line))
      VAST_WARN("failed to parse /proc/self/status: {}", line);
  }
  return result;
}

} // namespace vast::detail
#endif

#if VAST_MACOS

namespace vast::detail {

static record get_settings_mach() {
  record result;
  task_t task = MACH_PORT_NULL;
  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
    return {};
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
  task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
  // http://web.mit.edu/darwin/src/modules/xnu/osfmk/man/task_basic_info.html
  // says the resident set size is counted in pages, so we multiply accordingly.
  result["current-memory-usage"] = uint64_t{t_info.resident_size * PAGE_SIZE};
  return result;
}

} // namespace vast::detail
#endif

#if VAST_POSIX && !VAST_LINUX

namespace vast::detail {

static record get_status_rusage() {
  record result;
  struct rusage ru;
  if (getrusage(RUSAGE_SELF, &ru) != 0) {
    VAST_WARN("{} failed to obtain rusage: {}", __func__, std::strerror(errno));
    return result;
  }
  result["peak-memory-usage"] = uint64_t{ru.ru_maxrss * 1024ull};
  return result;
}

} // namespace vast::detail
#endif

namespace vast::detail {

namespace {

caf::expected<std::filesystem::path> objectpath_dynamic(const void* addr) {
  Dl_info info;
  if (!dladdr(addr, &info))
    return caf::make_error(ec::unspecified, "failed to execute dladdr()");
  if (!info.dli_fname)
    return caf::make_error(ec::unspecified, "addr not in an mmapped region");
  // FIXME: On Linux, if addr is inside the main executable,
  // dli_fname seems to be the same argv[0] instead of the full path.
  // We should detect and return an error in that case.
  return info.dli_fname;
}

caf::expected<std::filesystem::path> objectpath_static() {
#if VAST_LINUX
  struct stat sb;
  auto self = "/proc/self/exe";
  if (lstat(self, &sb) == -1)
    return caf::make_error(ec::unspecified, "lstat() returned with error");
  auto size = sb.st_size ? sb.st_size + 1 : PATH_MAX;
  std::vector<char> buf(size);
  if (readlink(self, buf.data(), size) == -1)
    return caf::make_error(ec::unspecified, "readlink() returned with error");
  return std::filesystem::path{buf.data()};
#else
  return caf::make_error(ec::unimplemented);
#endif
}

} // namespace

caf::expected<std::filesystem::path> objectpath(const void* addr) {
  if (addr == nullptr)
    addr = reinterpret_cast<const void*>(objectpath_dynamic);
  if (auto result = objectpath_dynamic(addr))
    return result;
  return objectpath_static();
}

record get_status() {
#if VAST_LINUX
  return get_status_proc();
#elif VAST_MACOS
  auto result = get_status_rusage();
  merge(get_settings_mach(), result, policy::merge_lists::no);
  return result;
#elif VAST_POSIX
  return get_status_rusage();
#else
  VAST_DEBUG("getting process information not supported");
  // Not implemented.
  return record{};
#endif
}

caf::expected<std::string> execute_blocking(const std::string& command) {
  using namespace std::string_literals;
  std::string result;
  std::array<char, 4096> buffer; // Try to read one full page at a time.
  auto* out = ::popen(command.c_str(), "r");
  if (!out)
    return caf::make_error(ec::system_error,
                           "popen() failed: "s + ::strerror(errno));
  size_t nread = 0;
  do {
    nread = ::fread(buffer.data(), 1, buffer.size(), out);
    result += std::string_view{buffer.data(), nread};
  } while (nread == buffer.size());
  auto error = ::ferror(out);
  if (::pclose(out) < 0)
    return caf::make_error(ec::system_error,
                           "pclose() failed: "s + ::strerror(errno));
  if (error)
    return caf::make_error(ec::system_error, "fread() failed");
  return result;
}

} // namespace vast::detail
