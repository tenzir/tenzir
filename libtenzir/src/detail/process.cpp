//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/process.hpp"

#include "tenzir/detail/posix.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/expected.hpp>

#include <dlfcn.h>
#include <string_view>

#if TENZIR_LINUX
#  include <sys/stat.h>

#  include <limits.h>
#  include <unistd.h>
#endif

namespace tenzir::detail {

namespace {

caf::expected<std::filesystem::path> objectpath_dynamic(const void* addr) {
  Dl_info info;
  if (not dladdr(addr, &info)) {
    return caf::make_error(ec::unspecified, "failed to execute dladdr()");
  }
  if (not info.dli_fname) {
    return caf::make_error(ec::unspecified, "addr not in an mmapped region");
  }
  // FIXME: On Linux, if addr is inside the main executable,
  // dli_fname seems to be the same argv[0] instead of the full path.
  // We should detect and return an error in that case.
  return info.dli_fname;
}

caf::expected<std::filesystem::path> objectpath_static() {
#if TENZIR_LINUX
  struct stat sb;
  auto self = "/proc/self/exe";
  if (lstat(self, &sb) == -1) {
    return caf::make_error(ec::unspecified, "lstat() returned with error");
  }
  auto size = sb.st_size ? sb.st_size + 1 : PATH_MAX;
  std::vector<char> buf(size);
  if (readlink(self, buf.data(), size) == -1) {
    return caf::make_error(ec::unspecified, "readlink() returned with error");
  }
  return std::filesystem::path{buf.data()};
#else
  return caf::make_error(ec::unimplemented);
#endif
}

} // namespace

caf::expected<std::filesystem::path> objectpath(const void* addr) {
  if (addr == nullptr) {
    addr = reinterpret_cast<const void*>(objectpath_dynamic);
  }
  if (auto result = objectpath_dynamic(addr)) {
    return result;
  }
  return objectpath_static();
}

caf::expected<std::string> execute_blocking(const std::string& command) {
  using namespace std::string_literals;
  std::string result;
  std::array<char, 4096> buffer; // Try to read one full page at a time.
  auto* out = ::popen(command.c_str(), "r");
  if (not out) {
    return caf::make_error(ec::system_error,
                           "popen() failed: "s + detail::describe_errno());
  }
  size_t nread = 0;
  do {
    nread = ::fread(buffer.data(), 1, buffer.size(), out);
    result += std::string_view{buffer.data(), nread};
  } while (nread == buffer.size());
  auto error = ::ferror(out);
  if (::pclose(out) < 0) {
    return caf::make_error(ec::system_error,
                           "pclose() failed: "s + detail::describe_errno());
  }
  if (error) {
    return caf::make_error(ec::system_error, "fread() failed");
  }
  return result;
}

} // namespace tenzir::detail
