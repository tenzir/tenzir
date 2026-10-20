//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <cstddef>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace tenzir {

/// Selects whether input must be a regular file or may be a stream.
enum class FileSource {
  regular,
  stream,
};

struct FileReadOptions {
  FileSource source = FileSource::regular;
  /// Maximum accepted byte count. Exceeding it is an error, not truncation.
  Option<size_t> max_bytes = None{};
};

/// Reads a local file to EOF through one handle, including regular pseudo-files
/// whose reported size is zero. File size is only a bounded reserve hint.
/// Concurrent in-place truncation can produce a shorter successful read, and
/// growth can contribute additional bytes; this is not a snapshot guarantee.
/// Follows symlinks. Regular input is validated on the opened descriptor and
/// rejects non-regular files without waiting for FIFO writers. Explicit stream
/// input uses a blocking open and may wait for a producer or EOF. These helpers
/// perform blocking I/O and do not promise prompt cancellation. The path `-`
/// is always a literal filename, not standard input.
auto read_file(std::filesystem::path const& path, FileReadOptions options = {})
  -> Result<std::vector<std::byte>, std::string>;

/// Fills exactly the requested prefix through one handle. Early EOF is an
/// error; trailing bytes are allowed and need not reach EOF. The same source
/// validation and blocking-open rules apply as for the owning overload. An
/// empty destination still opens and validates the input.
auto read_file(std::filesystem::path const& path,
               std::span<std::byte> destination,
               FileSource source = FileSource::regular)
  -> Result<void, std::string>;

/// Creates parent directories and writes a local file, truncating any previous
/// contents. Follows symlinks. Errors may leave a partially written file. The
/// path `-` is a literal filename, not standard output. Stream destinations
/// such as FIFOs retain synchronous, blocking write behavior.
auto write_file(std::filesystem::path const& path,
                std::span<std::byte const> bytes) -> Result<void, std::string>;

/// Creates parent directories, writes a unique sibling temporary file, closes
/// it, then atomically renames it over the destination. Replaces a destination
/// symlink rather than its target. Failure cleans up the temporary file and
/// leaves the destination unchanged. This does not guarantee crash durability.
auto replace_file(std::filesystem::path const& path,
                  std::span<std::byte const> bytes)
  -> Result<void, std::string>;

/// Tries to expand paths that start with a `~`. Returns the original input
/// string if no expansion occurs.
auto expand_home(std::string path) -> std::string;

} // namespace tenzir
