//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/fs_url_template.hpp"
#include "tenzir/glob.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/let_id.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/tql2/ast.hpp"

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/util/future.h>
#include <arrow/util/uri.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/futures/Future.h>

#include <array>
#include <deque>
#include <unordered_map>
#include <unordered_set>

namespace tenzir {

/// Common arguments for Arrow filesystem-based operators.
struct FromArrowFsArgs {
  located<secret> url;
  Option<location> watch;
  Option<location> remove;
  Option<ast::lambda_expr> rename;
  Option<duration> max_age;
  located<ir::pipeline> pipe;
  let_id file_info;

  /// Registers the common ArrowFsArgs fields on a Describer, optionally
  /// running an extra validator as part of the combined `validate` closure.
  template <class Args, class... Impls, class F = decltype([](DescribeCtx&) {})>
    requires std::derived_from<Args, FromArrowFsArgs>
             and std::invocable<F, DescribeCtx&>
  static auto describe_to(Describer<Args, Impls...>& d, F extra = {}) -> void {
    d.template positional<located<secret>>("url", &FromArrowFsArgs::url);
    d.named("watch", &FromArrowFsArgs::watch);
    auto remove_arg = d.named("remove", &FromArrowFsArgs::remove);
    auto rename_arg
      = d.template named<ast::lambda_expr>("rename", &FromArrowFsArgs::rename);
    auto max_age_arg
      = d.template named<duration>("max_age", &FromArrowFsArgs::max_age);
    auto pipe_arg = d.pipeline(&FromArrowFsArgs::pipe,
                               {{"file", &FromArrowFsArgs::file_info}});
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
  Option<time> mtime;
  int64_t offset = 0;
  uint64_t job_id = 0;
  std::shared_ptr<arrow::io::RandomAccessFile> file;

  friend auto inspect(auto& f, TrackedFile& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("offset", x.offset),
                              f.field("mtime", x.mtime),
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

/// Converts an Arrow filesystem TimePoint to an Option<time>.
/// Returns None for Arrow's kNoTime sentinel.
inline auto to_option_time(arrow::fs::TimePoint tp) -> Option<time> {
  if (tp == arrow::fs::kNoTime) {
    return {};
  }
  return tp;
}

/// Serializable representation of a previously seen file.
struct SeenFile {
  std::string path;
  Option<time> mtime;
  int64_t size = 0;

  SeenFile() = default;

  SeenFile(arrow::fs::FileInfo const& file)
    : path{file.path()},
      mtime{to_option_time(file.mtime())},
      size{file.size()} {
  }

  friend auto operator==(SeenFile const&, SeenFile const&) -> bool = default;

  friend auto inspect(auto& f, SeenFile& x) -> bool {
    return f.object(x).fields(f.field("path", x.path),
                              f.field("mtime", x.mtime),
                              f.field("size", x.size));
  }
};

struct SeenFileHasher {
  auto operator()(SeenFile const& file) const -> size_t {
    return hash(file.path, data{file.mtime}, file.size);
  }
};

using SeenFileSet = std::unordered_set<SeenFile, SeenFileHasher>;

struct MakeFilesystemResult {
  FileSystemPtr fs;
  std::string path;
};

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
class FromArrowFsOperator : public Operator<void, table_slice> {
public:
  explicit FromArrowFsOperator(FromArrowFsArgs args)
    : base_args_{std::move(args)} {
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
  // Create the filesystem from the resolved URI.
  virtual auto
  make_filesystem(arrow::util::Uri const& uri, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>>
    = 0;

  /// SDK specific calls to remove files that bypass Arrow's directory markers.
  virtual auto
  remove_file(std::string const& path, diagnostic_handler& dh) const
    -> Task<void>
    = 0;

  /// Resolves the URL secret and returns the parsed URI.
  virtual auto resolve_url(OpCtx& ctx) -> Task<failure_or<arrow::util::Uri>>
    = 0;

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

  FromArrowFsArgs base_args_;
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
  mutable Box<folly::coro::BoundedQueue<AwaitResult>> results_{
    std::in_place,
    max_jobs + 1,
  };
};

/// Common arguments for Arrow filesystem-based sink operators.
struct ToArrowFsArgs {
  located<secret> url;
  uint64_t max_size = uint64_t{100} * 1024 * 1024;
  duration timeout = std::chrono::minutes{5};
  located<ir::pipeline> pipe;
  Option<ast::expression> partition_by;

  /// Registers the common ToArrowFsArgs fields on a Describer, optionally
  /// running an extra validator as part of the combined `validate` closure.
  template <class Args, class... Impls, class F = decltype([](DescribeCtx&) {})>
    requires std::derived_from<Args, ToArrowFsArgs>
             and std::invocable<F, DescribeCtx&>
  static auto describe_to(Describer<Args, Impls...>& d, F extra = {}) -> void {
    d.template positional<located<secret>>("url", &ToArrowFsArgs::url);
    auto max_size_arg = d.template named_optional<uint64_t>(
      "max_size", &ToArrowFsArgs::max_size);
    auto timeout_arg
      = d.template named_optional<duration>("timeout", &ToArrowFsArgs::timeout);
    auto partition_arg = d.template named<ast::expression>(
      "partition_by", &ToArrowFsArgs::partition_by, "list<field>");
    auto pipe_arg = d.pipeline(&ToArrowFsArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_size = ctx.get(max_size_arg)) {
        if (*max_size == 0) {
          diagnostic::error("`max_size` must be a positive number")
            .primary(*ctx.get_location(max_size_arg))
            .emit(ctx);
        }
      }
      if (auto timeout = ctx.get(timeout_arg)) {
        if (*timeout <= duration::zero()) {
          diagnostic::error("`timeout` must be a positive duration")
            .primary(*ctx.get_location(timeout_arg))
            .emit(ctx);
        }
      }
      if (auto partition_by = ctx.get(partition_arg)) {
        if (auto l = try_as<ast::list>(*partition_by)) {
          for (auto const& item : l->items) {
            match(
              item,
              [&](const ast::spread&) {
                diagnostic::error(
                  "spread expressions are not allowed in `partition_by`")
                  .emit(ctx);
              },
              [&](const ast::expression& expr) {
                if (not ast::field_path::try_from(expr)) {
                  diagnostic::error(
                    "`partition_by` list items must be field selectors")
                    .primary(expr)
                    .emit(ctx);
                }
              });
          }
        } else if (not ast::field_path::try_from(*partition_by)) {
          diagnostic::error("`partition_by` must be a field selector or a "
                            "list of field selectors")
            .primary(*ctx.get_location(partition_arg))
            .emit(ctx);
        }
      }
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->template is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes").primary(pipe).emit(ctx);
      }
      extra(ctx);
      return {};
    });
  }
};

/// Base class for Arrow filesystem-based sink operators.
///
/// Required overrides:
/// - `resolve_url()`     — resolve the URL secret to a raw string (may
///                         contain `**` and `{uuid}` placeholders).
/// - `make_filesystem()` — build the Arrow filesystem from the sanitized
///                         URL and return it together with the object
///                         path extracted by `*Options::FromUri`.
///
/// Optional overrides:
/// - `setup_args()` — populate `args` fields that need an `OpCtx` (e.g.
///                    a built-in `pipe`, or a `url` synthesised from
///                    user-facing arguments). Called first in `start()`.
/// - `preprocess()` — transform each incoming slice before partition
///                    routing: inject synthetic columns, coerce types,
///                    or drop rows by returning an empty slice.
///
/// The base class handles:
///   - URL template parsing and placeholder sanitization via `FsUrlTemplate`.
///   - Partition-aware routing of table slices to per-partition sub-pipelines.
///   - Size- and age-based partition rotation, signalled through an
///     internal control queue.
///   - Stream lifecycle (open on first chunk, close on sub teardown,
///     flush on snapshot).
///
/// Checkpoint / restart semantics:
///   - `prepare_snapshot` flushes every open stream so bytes that have
///     been written are durable, but per-partition state is *not*
///     serialised.
///   - On restart the operator begins with zero partitions. A stream that
///     was open at checkpoint time is re-created on the next push;
///     without a `{uuid}` placeholder or a time-varying `partition_by`
///     value, this overwrites the previous file. `FsUrlTemplate::set_path`
///     already warns users when no `{uuid}` is present in the path.
///   - For multipart-upload backends (S3 / ABS / GCS) an in-flight upload
///     from before the checkpoint is orphaned on the server; rely on the
///     provider's multipart-abort lifecycle rule to clean it up.
class ToArrowFsOperator : public Operator<table_slice, void> {
public:
  explicit ToArrowFsOperator(ToArrowFsArgs args) : base_args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> final;
  auto process(table_slice input, OpCtx& ctx) -> Task<void> final;
  auto process_sub(SubKeyView key, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> final;
  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> final;
  auto await_task(diagnostic_handler& dh) const -> Task<Any> final;
  auto process_task(Any result, OpCtx& ctx) -> Task<void> final;
  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> final;
  auto prepare_snapshot(OpCtx& ctx) -> Task<void> final;
  auto state() -> OperatorState final;

protected:
  /// Resolves the URL secret and returns the raw, secret-expanded URL string.
  /// The string may contain template placeholders (e.g. **, {uuid}).
  virtual auto resolve_url(OpCtx& ctx) -> Task<failure_or<std::string>> = 0;

  /// Creates the Arrow filesystem from the sanitized URL (template
  /// placeholders replaced by URI-safe tokens) and returns the filesystem
  /// plus the path extracted by *Options::FromUri (which will contain safe
  /// tokens — the base class restores real placeholders via
  /// FsUrlTemplate::set_path).
  virtual auto make_filesystem(std::string const& url, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>>
    = 0;

  /// Called before partition routing for every incoming slice. Derived
  /// classes can inject synthetic columns (e.g. for partitioning on a
  /// derived value), coerce types, or drop rows by returning an empty
  /// slice. Default implementation is an identity transform.
  virtual auto preprocess(table_slice input, OpCtx& ctx) -> Task<table_slice>;

  /// Called at the top of `start()` before anything else. Lets derived
  /// classes populate `args` fields that need an `OpCtx` (e.g. a built-in
  /// `pipe` that requires a `compile_ctx`, or a `url` synthesised from
  /// other user-facing args). Default is a no-op.
  virtual auto setup_args(ToArrowFsArgs& args, OpCtx& ctx)
    -> Task<failure_or<void>>;

  auto filesystem() const -> FileSystemPtr const& {
    return fs_;
  }

private:
  struct Partition {
    data key;
    std::shared_ptr<arrow::io::OutputStream> stream;
    size_t stream_bytes = 0;
    time created = time::clock::now();
    bool is_rotating = false;
  };

  struct RotateRequested {
    int64_t sub_key;
  };
  using Message = variant<RotateRequested>;

  /// State shared between the operator's main loop (`process`, `finalize`,
  /// `prepare_snapshot`) and subpipeline drivers (`process_sub`,
  /// `finish_sub`). Guarded by `state_`. Arrow filesystem calls run via
  /// `spawn_blocking` *outside* the guard, so I/O across partitions is
  /// not serialised — only map mutations are.
  struct State {
    /// Sub-key → partition state. Inserted in `process()`, erased in
    /// `finish_sub`.
    std::unordered_map<int64_t, Partition> partitions;
    /// Partition key → currently-active sub-key. Erased by `process_sub`
    /// when rotation fires and by `finish_sub` on sub teardown, so the
    /// next push for the same key spawns a fresh sub. Keyed by `data`
    /// so that `std::hash<data>` can hash through a cheap view without
    /// copying the underlying list.
    std::unordered_map<data, int64_t> key_to_sub;
  };

  /// Briefly locks `state_` and returns a reference to partition `sk`, or
  /// `None` if it was already erased. The returned reference remains valid
  /// for the rest of the caller: `std::unordered_map` references survive
  /// inserts/rehashes, and the partition is only erased by `finish_sub(sk)`,
  /// which is serialised against its owning sub's driver.
  auto find_partition(int64_t sk) -> Task<Option<Partition&>>;

  auto open_stream(Partition& part, diagnostic_handler& dh) const
    -> Task<failure_or<void>>;
  auto close_stream(Partition& part, diagnostic_handler& dh) const
    -> Task<void>;

  ToArrowFsArgs base_args_;
  FileSystemPtr fs_;
  FsUrlTemplate template_;
  int64_t next_sub_key_ = 0;

  Mutex<State> state_{State{}};

  /// Mirrors `state_->partitions.size()` for reads from `state()`, which is
  /// synchronous and cannot co_await. All updaters (`process`, `finish_sub`)
  /// and the reader (`state`, `finalize`) run serially on the operator's
  /// executor, so a plain counter suffices.
  size_t partition_count_ = 0;

  // Control-plane queue. `process_sub` enqueues `RotateRequested` when a
  // partition crosses the rotation threshold; `await_task` dequeues and
  // `process_task` dispatches. No separate notification primitive is
  // needed — the queue itself wakes the executor.
  mutable Box<folly::coro::UnboundedQueue<Message>> control_queue_{
    std::in_place,
  };

  bool finalized_ = false;
  MetricsCounter bytes_written_counter_;
};

} // namespace tenzir
