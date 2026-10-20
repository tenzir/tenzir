//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/env.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/file.hpp>
#include <tenzir/uuid.hpp>

#include <fmt/std.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <sys/stat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <system_error>
#include <utility>

namespace tenzir {
namespace {

auto file_error(std::string_view operation, std::filesystem::path const& path,
                int error = errno) -> std::string {
  return fmt::format("failed to {} {}: {}", operation, path,
                     std::error_code{error, std::generic_category()}.message());
}

auto open_file(std::filesystem::path const& path, struct stat& metadata,
               FileSource source) -> Result<folly::File, std::string> {
  // Regular reads inspect and read the same descriptor. Nonblocking open also
  // handles a FIFO replacing a regular file without waiting for a writer.
  // Explicit stream reads instead preserve blocking producer/EOF semantics.
  auto flags = O_RDONLY | O_CLOEXEC;
  if (source != FileSource::stream) {
    flags |= O_NONBLOCK;
  }
  auto fd = folly::openNoInt(path.c_str(), flags);
  if (fd == -1) {
    return Err{file_error("open", path)};
  }
  auto file = folly::File{fd, true};
  if (source == FileSource::stream) {
    return file;
  }
  if (::fstat(fd, &metadata) != 0) {
    return Err{file_error("inspect", path)};
  }
  if (not S_ISREG(metadata.st_mode)) {
    return Err{fmt::format("cannot read non-regular file {}", path)};
  }
  return file;
}

auto read_some(folly::File const& file, std::filesystem::path const& path,
               std::span<std::byte> buffer) -> Result<size_t, std::string> {
  // macOS rejects read requests larger than INT_MAX. Retry interrupted reads
  // in one place for both the EOF and exact-prefix overloads.
  auto size = std::min(buffer.size(), size_t{std::numeric_limits<int>::max()});
  auto count = folly::readNoInt(file.fd(), buffer.data(), size);
  if (count < 0) {
    return Err{file_error("read", path)};
  }
  return static_cast<size_t>(count);
}

auto create_parent(std::filesystem::path const& path)
  -> Result<void, std::string> {
  if (path.has_parent_path()) {
    auto error = std::error_code{};
    std::filesystem::create_directories(path.parent_path(), error);
    if (error) {
      return Err{fmt::format("failed to create directory {}: {}",
                             path.parent_path(), error.message())};
    }
  }
  return {};
}

auto write_contents(folly::File& file, std::filesystem::path const& path,
                    std::span<std::byte const> bytes)
  -> Result<void, std::string> {
  while (not bytes.empty()) {
    auto size = std::min(bytes.size(), size_t{std::numeric_limits<int>::max()});
    auto count = folly::writeFull(file.fd(), bytes.data(), size);
    if (count < 0) {
      return Err{file_error("write", path)};
    }
    if (static_cast<size_t>(count) != size) {
      return Err{fmt::format("incomplete write of {}", path)};
    }
    bytes = bytes.subspan(size);
  }
  if (not file.closeNoThrow()) {
    return Err{file_error("close", path)};
  }
  return {};
}

} // namespace

auto read_file(std::filesystem::path const& path, FileReadOptions options)
  -> Result<std::vector<std::byte>, std::string> {
  struct stat metadata{};
  TRY(auto file, open_file(path, metadata, options.source));
  auto result = std::vector<std::byte>{};
  auto limit = std::min(options.max_bytes.value_or(result.max_size()),
                        result.max_size());
  auto buffer = std::array<std::byte, 64 * 1024>{};
  // Treat size only as a hint: pseudo-files can report zero and ordinary files
  // can grow or shrink. Never allocate an unchecked stat-sized buffer.
  if (metadata.st_size > 0) {
    auto reserve_size = std::min(limit, buffer.size());
    if (std::cmp_less(metadata.st_size, reserve_size)) {
      reserve_size = static_cast<size_t>(metadata.st_size);
    }
    result.reserve(reserve_size);
  }
  while (true) {
    auto remaining = limit - result.size();
    // Probe one byte past the limit to distinguish exact-size input from
    // oversized input, even when the limit is zero. The addition is bounded
    // by the scratch buffer size and cannot overflow for SIZE_MAX limits.
    auto size = remaining < buffer.size() ? remaining + 1 : buffer.size();
    TRY(auto count, read_some(file, path, std::span{buffer}.first(size)));
    if (count == 0) {
      return result;
    }
    if (count > remaining) {
      return Err{
        fmt::format("file {} exceeds read limit of {} bytes", path, limit)};
    }
    result.insert(result.end(), buffer.begin(), buffer.begin() + count);
  }
}

auto read_file(std::filesystem::path const& path,
               std::span<std::byte> destination, FileSource source)
  -> Result<void, std::string> {
  struct stat metadata{};
  TRY(auto file, open_file(path, metadata, source));
  while (not destination.empty()) {
    TRY(auto count, read_some(file, path, destination));
    if (count == 0) {
      return Err{fmt::format("incomplete read of {}", path)};
    }
    destination = destination.subspan(count);
  }
  return {};
}

auto write_file(std::filesystem::path const& path,
                std::span<std::byte const> bytes) -> Result<void, std::string> {
  TRY(create_parent(path));
  auto fd = folly::openNoInt(path.c_str(),
                             O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd == -1) {
    return Err{file_error("open", path)};
  }
  auto file = folly::File{fd, true};
  return write_contents(file, path, bytes);
}

auto replace_file(std::filesystem::path const& path,
                  std::span<std::byte const> bytes)
  -> Result<void, std::string> {
  TRY(create_parent(path));
  // O_EXCL prevents following a pre-existing temporary symlink. Unlike
  // Folly's atomic writer, which fchmods after mkstemp, open honors the process
  // umask just like write_file. A unique sibling also supports concurrent
  // writers.
  auto temporary
    = path.parent_path() / fmt::format(".tenzir-{}.tmp", uuid::random());
  auto fd = folly::openNoInt(temporary.c_str(),
                             O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd == -1) {
    return Err{file_error("create temporary for", path)};
  }
  auto file = folly::File{fd, true};
  auto cleanup = detail::scope_guard{[&]() noexcept {
    auto error = std::error_code{};
    std::filesystem::remove(temporary, error);
  }};
  // Check close before publication, including delayed write errors. No fsync:
  // atomic publication is not a promise of crash durability.
  TRY(write_contents(file, path, bytes));
  auto error = std::error_code{};
  std::filesystem::rename(temporary, path, error);
  if (error) {
    return Err{
      fmt::format("failed to atomically write {}: {}", path, error.message())};
  }
  cleanup.disable();
  return {};
}

auto expand_home(std::string path) -> std::string {
  if (path.empty() or path[0] != '~') {
    return path;
  }
  if (path.size() == 1 or path[1] == '/') {
    auto home = detail::getenv("HOME");
    if (home) {
      path.replace(0, 1, *home);
    }
  }
  return path;
}

} // namespace tenzir
