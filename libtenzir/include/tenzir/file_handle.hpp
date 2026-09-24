//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/time.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace arrow::io {
class RandomAccessFile;
} // namespace arrow::io

namespace tenzir {

/// A seekable file, as an element type next to bytes and events.
///
/// File sources hand one handle per file to a subpipeline that accepts it,
/// instead of streaming the file's bytes. Readers of formats that need random
/// access, such as Parquet with its footer, then fetch only what they need.
///
/// The identity fields describe the file that `file` refers to. Readers that
/// checkpoint their position within the file compare them after a restore.
struct FileHandle {
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::string path;
  Option<time> mtime;
  int64_t size = 0;
};

} // namespace tenzir
