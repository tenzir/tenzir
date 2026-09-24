//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/config.h>
#include <arrow/util/future.h>

#include <memory>
#include <utility>

namespace tenzir {

/// A `RandomAccessFile` that forwards to another file.
///
/// Decorators derive from this class and override the reads they want to
/// intercept. `arrow::io::RandomAccessFile` has grown several overloads of
/// `ReadAt` and `ReadAsync` whose default implementations call each other in a
/// version-dependent order; Arrow 25 in particular routes `ReadAsync(position,
/// nbytes)` through a new `allow_short_read` overload, so a decorator that
/// overrides only the older ones is bypassed. This class funnels every overload
/// into five canonical ones, which are the only ones a decorator needs to
/// override to see every read:
///
/// - `Read(nbytes, out)` and `Read(nbytes)` for sequential reads,
/// - `ReadAt(position, nbytes, out)` and `ReadAt(position, nbytes)` for
///   synchronous positional reads, and
/// - `ReadAsync(ctx, position, nbytes)` for asynchronous positional reads.
class ForwardingFile : public arrow::io::RandomAccessFile {
public:
  explicit ForwardingFile(std::shared_ptr<arrow::io::RandomAccessFile> inner)
    : inner_{std::move(inner)} {
  }

  // -- FileInterface ----------------------------------------------------------

  auto Close() -> arrow::Status override {
    return inner_->Close();
  }

  auto CloseAsync() -> arrow::Future<> override {
    return inner_->CloseAsync();
  }

  auto Abort() -> arrow::Status override {
    return inner_->Abort();
  }

  auto Tell() const -> arrow::Result<int64_t> override {
    return inner_->Tell();
  }

  auto closed() const -> bool override {
    return inner_->closed();
  }

  // -- Seekable ---------------------------------------------------------------

  auto Seek(int64_t position) -> arrow::Status override {
    return inner_->Seek(position);
  }

  // -- Readable and InputStream -----------------------------------------------

  auto Read(int64_t nbytes, void* out) -> arrow::Result<int64_t> override {
    return inner_->Read(nbytes, out);
  }

  auto Read(int64_t nbytes)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> override {
    return inner_->Read(nbytes);
  }

  auto io_context() const -> const arrow::io::IOContext& override {
    return inner_->io_context();
  }

  auto supports_zero_copy() const -> bool override {
    return inner_->supports_zero_copy();
  }

  // -- RandomAccessFile -------------------------------------------------------

  auto GetSize() -> arrow::Result<int64_t> override {
    return inner_->GetSize();
  }

  auto ReadAt(int64_t position, int64_t nbytes, void* out)
    -> arrow::Result<int64_t> override {
    return inner_->ReadAt(position, nbytes, out);
  }

  auto ReadAt(int64_t position, int64_t nbytes)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> override {
    return inner_->ReadAt(position, nbytes);
  }

  auto
  ReadAsync(const arrow::io::IOContext& ctx, int64_t position, int64_t nbytes)
    -> arrow::Future<std::shared_ptr<arrow::Buffer>> override {
    return inner_->ReadAsync(ctx, position, nbytes);
  }

#if ARROW_VERSION_MAJOR >= 25
  // The `allow_short_read` overloads funnel into the canonical ones above, the
  // way Arrow's own default implementations do, so that a decorator sees them.

  auto ReadAt(int64_t position, int64_t nbytes, bool allow_short_read,
              void* out) -> arrow::Result<int64_t> override {
    ARROW_ASSIGN_OR_RAISE(auto read, ReadAt(position, nbytes, out));
    return check_short_read(read, nbytes, allow_short_read);
  }

  auto ReadAt(int64_t position, int64_t nbytes, bool allow_short_read)
    -> arrow::Result<std::shared_ptr<arrow::Buffer>> override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(position, nbytes));
    if (buffer) {
      ARROW_RETURN_NOT_OK(
        check_short_read(buffer->size(), nbytes, allow_short_read));
    }
    return buffer;
  }

  auto ReadAsync(const arrow::io::IOContext& ctx, int64_t position,
                 int64_t nbytes, bool allow_short_read)
    -> arrow::Future<std::shared_ptr<arrow::Buffer>> override {
    return ReadAsync(ctx, position, nbytes)
      .Then(
        [nbytes, allow_short_read](const std::shared_ptr<arrow::Buffer>& buffer)
          -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
          if (buffer) {
            ARROW_RETURN_NOT_OK(
              check_short_read(buffer->size(), nbytes, allow_short_read));
          }
          return buffer;
        });
  }
#endif

  auto WillNeed(const std::vector<arrow::io::ReadRange>& ranges)
    -> arrow::Status override {
    return inner_->WillNeed(ranges);
  }

protected:
  auto inner() const -> arrow::io::RandomAccessFile& {
    return *inner_;
  }

private:
  static auto
  check_short_read(int64_t actual, int64_t requested, bool allow_short_read)
    -> arrow::Result<int64_t> {
    if (not allow_short_read and actual != requested) {
      return arrow::Status::IOError("File too short: expected to be able to "
                                    "read ",
                                    requested, " bytes, got ", actual);
    }
    return actual;
  }

  std::shared_ptr<arrow::io::RandomAccessFile> inner_;
};

} // namespace tenzir
