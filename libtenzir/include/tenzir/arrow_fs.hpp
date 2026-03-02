//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/glob.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/tql2/ast.hpp"

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/util/future.h>
#include <arrow/util/uri.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Task.h>
#include <folly/futures/Future.h>

#include <array>
#include <deque>
#include <unordered_set>

namespace tenzir {

/// Common arguments for Arrow filesystem-based operators.
struct ArrowFsArgs {
  located<secret> url;
  Option<location> watch;
  Option<location> remove;
  Option<ast::lambda_expr> rename;
  Option<duration> max_age;
  located<ir::pipeline> pipe;

  /// Registers the common ArrowFsArgs fields on a Describer.
  template <class Args, class... Impls>
    requires std::derived_from<Args, ArrowFsArgs>
  static auto describe_to(Describer<Args, Impls...>& d) -> void {
    d.template positional<located<secret>>("url", &ArrowFsArgs::url);
    d.named("watch", &ArrowFsArgs::watch);
    auto remove_arg = d.named("remove", &ArrowFsArgs::remove);
    auto rename_arg
      = d.template named<ast::lambda_expr>("rename", &ArrowFsArgs::rename);
    auto max_age_arg
      = d.template named<duration>("max_age", &ArrowFsArgs::max_age);
    auto pipe_arg = d.pipeline(&ArrowFsArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto remove_loc = ctx.get_location(remove_arg);
      auto rename_loc = ctx.get_location(rename_arg);
      if (remove_loc and rename_loc) {
        diagnostic::error("cannot use both `remove` and `rename`")
          .primary(*remove_loc)
          .primary(*rename_loc)
          .emit(ctx);
      }
      if (auto max_age = ctx.get(max_age_arg)) {
        if (*max_age <= duration::zero()) {
          diagnostic::error("`max_age` must be a positive duration")
            .primary(*ctx.get_location(max_age_arg))
            .emit(ctx);
        }
      }
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (*output and (*output)->template is<chunk_ptr>()) {
        diagnostic::error("pipeline must not return bytes")
          .primary(pipe)
          .emit(ctx);
      }
      return {};
    });
  }

  /// Registers the common ArrowFsArgs fields with an extra validator.
  template <class Args, class... Impls, class F>
    requires std::derived_from<Args, ArrowFsArgs>
             and std::invocable<F, DescribeCtx&>
  static auto describe_to(Describer<Args, Impls...>& d, F extra) -> void {
    d.template positional<located<secret>>("url", &ArrowFsArgs::url);
    d.named("watch", &ArrowFsArgs::watch);
    auto remove_arg = d.named("remove", &ArrowFsArgs::remove);
    auto rename_arg
      = d.template named<ast::lambda_expr>("rename", &ArrowFsArgs::rename);
    auto max_age_arg
      = d.template named<duration>("max_age", &ArrowFsArgs::max_age);
    auto pipe_arg = d.pipeline(&ArrowFsArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto remove_loc = ctx.get_location(remove_arg);
      auto rename_loc = ctx.get_location(rename_arg);
      if (remove_loc and rename_loc) {
        diagnostic::error("cannot use both `remove` and `rename`")
          .primary(*remove_loc)
          .primary(*rename_loc)
          .emit(ctx);
      }
      if (auto max_age = ctx.get(max_age_arg)) {
        if (*max_age <= duration::zero()) {
          diagnostic::error("`max_age` must be a positive duration")
            .primary(*ctx.get_location(max_age_arg))
            .emit(ctx);
        }
      }
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (*output and (*output)->template is<chunk_ptr>()) {
        diagnostic::error("pipeline must not return bytes")
          .primary(pipe)
          .emit(ctx);
      }
      extra(ctx);
      return {};
    });
  }
};

/// Convert an Arrow Future to a folly Task.
template <class T>
auto arrow_future_to_task(arrow::Future<T> future) -> Task<arrow::Result<T>> {
  auto [promise, sf] = folly::makePromiseContract<arrow::Result<T>>();
  future.AddCallback(
    [promise = std::move(promise)](arrow::Result<T> const& result) mutable {
      promise.setValue(result);
    });
  co_return co_await std::move(sf);
}

inline auto arrow_future_to_task(arrow::Future<arrow::internal::Empty> future)
  -> Task<arrow::Status> {
  auto [promise, sf] = folly::makePromiseContract<arrow::Status>();
  future.AddCallback(
    [promise = std::move(promise)](arrow::Status const& status) mutable {
      promise.setValue(status);
    });
  co_return co_await std::move(sf);
}

/// Persisted state for a file that is pending or being actively processed.
struct TrackedFile {
  std::string path;
  int64_t mtime_ns = 0;
  int64_t offset = 0;
  uint64_t job_id = 0;
  std::shared_ptr<arrow::io::RandomAccessFile> file;

  friend auto inspect(auto& f, TrackedFile& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("offset", x.offset),
                              f.field("mtime_ns", x.mtime_ns),
                              f.field("job_id", x.job_id));
  }
};

/// Signals that a directory scan has completed with the discovered files.
struct ScanComplete {
  std::vector<arrow::fs::FileInfo> files;
};

/// Result of opening a file for reading in a processing slot.
struct FileOpen {
  uint64_t job_id;
  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> file;
};

/// Result of reading a chunk from an active file.
struct ReadProgress {
  uint64_t job_id;
  arrow::Result<std::shared_ptr<arrow::Buffer>> result;
};

/// Signals that a subpipeline has finished and its slot can be freed.
struct SubFinished {
  uint64_t job_id;
};

using AwaitResult = variant<ScanComplete, FileOpen, ReadProgress, SubFinished>;

using FileSystemPtr = std::shared_ptr<arrow::fs::FileSystem>;

/// Splits a path at the first `/` into two parts.
auto split_at_first_slash(std::string_view path)
  -> std::pair<std::string, std::string>;

/// Serializable representation of a previously seen file.
struct SeenFile {
  std::string path;
  int64_t mtime_ns = 0;
  int64_t size = 0;

  SeenFile() = default;

  SeenFile(arrow::fs::FileInfo const& file)
    : path{file.path()},
      mtime_ns{file.mtime().time_since_epoch().count()},
      size{file.size()} {
  }

  friend auto operator==(SeenFile const&, SeenFile const&) -> bool = default;

  friend auto inspect(auto& f, SeenFile& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("mtime_ns", x.mtime_ns),
                              f.field("size", x.size));
  }
};

struct SeenFileHasher {
  auto operator()(SeenFile const& file) const -> size_t {
    return hash(file.path, file.mtime_ns, file.size);
  }
};

using SeenFileSet = std::unordered_set<SeenFile, SeenFileHasher>;

/// Base class for Arrow filesystem-based source operators.
///
/// Derived classes must implement:
/// - `make_filesystem()`
/// - `remove_file()`
/// - `resolve_url()`
///
/// The base class handles:
///   - File discovery (with glob matching)
///   - Watch mode (periodic re-scanning)
///   - Job queue management (concurrent file processing)
///   - Subpipeline spawning per file
///   - File cleanup (remove/rename after processing)
class ArrowFsOperator : public Operator<void, table_slice> {
public:
  explicit ArrowFsOperator(ArrowFsArgs args) : base_args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> final;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> final;
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> final;
  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> final;
  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> final;
  auto state() -> OperatorState final;
  auto snapshot(Serde& serde) -> void final;
  auto post_commit(OpCtx& ctx) -> Task<void> final;

protected:
  struct MakeFilesystemResult {
    FileSystemPtr fs;
    std::string path;
  };

  // Create the filesystem from the resolved URI.
  virtual auto
  make_filesystem(arrow::util::Uri const& uri, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> = 0;

  /// SDK specific calls to remove files that bypass Arrow's directory markers.
  virtual auto remove_file(std::string const& path,
                           diagnostic_handler& dh) const -> Task<void> = 0;

  /// Resolves the URL secret and returns the parsed URI.
  virtual auto resolve_url(OpCtx& ctx)
    -> Task<failure_or<arrow::util::Uri>> = 0;

  auto filesystem() const -> std::shared_ptr<arrow::fs::FileSystem> const& {
    return fs_;
  }

private:
  static constexpr auto watch_pause = std::chrono::seconds{10};
  static constexpr size_t max_jobs = 10;
  static constexpr size_t read_size = 10uz * 1024 * 1024;

  auto cleanup_file(std::string path, diagnostic_handler& dh) const
    -> Task<void>;
  auto cleanup_files(diagnostic_handler& dh) -> Task<void>;
  auto restore(OpCtx& ctx) -> Task<void>;
  auto spawn_scan_task(OpCtx& ctx) -> void;
  auto is_globbing() const -> bool;
  auto find_free_slot() const -> Option<size_t>;
  auto find_slot_by_job(uint64_t job_id) const -> Option<size_t>;
  auto start_job_in_slot(size_t slot, OpCtx& ctx) -> void;

  template <class F>
    requires std::is_invocable_r_v<Task<AwaitResult>, F>
  auto enqueue_task(OpCtx& ctx, F f) -> void {
    ctx.spawn_task([this, f = std::move(f)]() mutable -> Task<void> {
      co_await results_->enqueue(co_await std::invoke(std::move(f)));
    });
  }

  ArrowFsArgs base_args_;
  FileSystemPtr fs_;
  glob glob_;
  std::string root_path_;

  bool scan_complete_ = false;
  SeenFileSet previous_;
  SeenFileSet current_;
  std::deque<TrackedFile> pending_;
  std::array<Option<TrackedFile>, max_jobs> processing_{};
  uint64_t next_job_id_ = 0;
  std::vector<std::string> cleanup_pending_;
  mutable std::unique_ptr<folly::coro::BoundedQueue<AwaitResult>> results_
    = std::make_unique<folly::coro::BoundedQueue<AwaitResult>>(max_jobs + 1);
};

} // namespace tenzir
