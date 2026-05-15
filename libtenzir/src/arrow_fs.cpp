//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_fs.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/fs_url_template.hpp"
#include "tenzir/glob.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view3.hpp"

#include <arrow/array/array_primitive.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/bit_util.h>
#include <folly/CancellationToken.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/futures/detail/Types.h>

#include <filesystem>
#include <ranges>

namespace tenzir {

auto split_at_first_slash(std::string_view path)
  -> std::pair<std::string, std::string> {
  auto slash = path.find('/');
  if (slash == std::string_view::npos) {
    return {std::string{path}, ""};
  }
  return {
    std::string{path.substr(0, slash)},
    std::string{path.substr(slash + 1)},
  };
}

namespace {

constexpr auto extract_root_path(glob const& glob_, std::string const& expanded)
  -> std::string {
  if (not glob_.empty()) {
    if (const auto* prefix = try_as<std::string>(glob_[0])) {
      if (glob_.size() == 1) {
        auto result = *prefix;
        if (expanded.ends_with('/') and not result.ends_with('/')) {
          result += '/';
        }
        return result;
      }
      const auto slash = prefix->rfind("/");
      if (slash != std::string::npos) {
        return prefix->substr(0, slash + 1);
      }
    }
  }
  return "/";
}

} // namespace

auto FromArrowFsOperator::start(OpCtx& ctx) -> Task<void> {
  co_await OperatorBase::start(ctx);
  auto resolved = co_await resolve_url(ctx);
  if (not resolved) {
    co_return;
  }
  auto& uri = *resolved;
  auto fs = co_await make_filesystem(uri, ctx.dh());
  if (not fs) {
    co_return;
  }
  fs_ = std::move(fs->fs);
  TENZIR_ASSERT(fs_);
  glob_ = parse_glob(fs->path);
  root_path_ = extract_root_path(glob_, fs->path);
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_arrow_fs"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_arrow_fs"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::events);
  co_await restore(ctx);
  auto ndh = null_diagnostic_handler{};
  co_await cleanup_files(ndh);
  if (base_args_.watch or not scan_complete_) {
    spawn_scan_task(ctx);
  }
}

auto FromArrowFsOperator::await_task(diagnostic_handler&) const -> Task<Any> {
  co_return co_await results_->dequeue();
}

auto FromArrowFsOperator::process_task(Any result, Push<table_slice>&,
                                       OpCtx& ctx) -> Task<void> {
  auto msg = result.as<AwaitResult>();
  co_await co_match(
    msg,
    [this, &ctx](ScanComplete& scan) -> Task<void> {
      for (auto& file : scan.files) {
        auto inserted = current_.emplace(file).second;
        if (not inserted or previous_.contains(file)) {
          continue;
        }
        pending_.push_back(TrackedFile{
          .path = file.path(),
          .mtime = to_option_time(file.mtime()),
          .file = nullptr,
        });
      }
      scan_complete_ = true;
      // NOTE: `previous_` can grow without bounds if neither `rename` nor
      // `remove` are used with `watch=true`.
      std::swap(previous_, current_);
      current_.clear();
      while (not pending_.empty()) {
        if (auto slot = find_free_slot()) {
          start_job_in_slot(*slot, ctx);
        } else {
          break;
        }
      }
      co_return;
    },
    [this, &ctx](FileOpen& open) -> Task<void> {
      auto slot = find_slot_by_job(open.job_id);
      TENZIR_ASSERT(slot);
      auto& file_state = *processing_[*slot];
      if (not open.file.ok()) {
        diagnostic::error("failed to open `{}`", file_state.path)
          .primary(base_args_.url)
          .note(open.file.status().ToStringWithoutContextLines())
          .compose([&](auto x) {
            return is_globbing() ? std::move(x).severity(severity::warning)
                                 : std::move(x);
          })
          .emit(ctx);
        processing_[*slot].reset();
        start_job_in_slot(*slot, ctx);
        co_return;
      }
      file_state.file = open.file.MoveValueUnsafe();
      auto pipe = base_args_.pipe.inner;
      auto env = substitute_ctx::env_t{
        {
          base_args_.file_info,
          record{
            {"path", file_state.path},
            {"mtime", file_state.mtime},
          },
        },
      };
      auto sub_result = pipe.substitute({ctx, &env}, true);
      if (not sub_result) {
        processing_[*slot].reset();
        start_job_in_slot(*slot, ctx);
        co_return;
      }
      co_await ctx.spawn_sub<chunk_ptr>(open.job_id, std::move(pipe));
      // Queue the first read.
      enqueue_task(
        ctx,
        [job_id = open.job_id, file = file_state.file,
         offset = file_state.offset] -> Task<AwaitResult> {
          co_return ReadProgress{
            job_id,
            co_await arrow_future_to_task(file->ReadAsync(offset, read_size)),
          };
        });
    },
    [this, &ctx](ReadProgress& read) -> Task<void> {
      // The subpipeline can be torn down while a read is in-flight.
      // If it no longer exists, this read is stale.
      auto sub = ctx.get_sub(read.job_id);
      if (not sub) {
        co_return;
      }
      auto& pipe = as<SubHandle<chunk_ptr>>(*sub);
      auto slot = find_slot_by_job(read.job_id);
      TENZIR_ASSERT(slot);
      auto& file_state = *processing_[*slot];
      if (not read.result.ok()) {
        diagnostic::error("failed to read from `{}`", file_state.path)
          .primary(base_args_.url)
          .note(read.result.status().ToStringWithoutContextLines())
          .compose([&](auto x) {
            return is_globbing() ? std::move(x).severity(severity::warning)
                                 : std::move(x);
          })
          .emit(ctx);
        co_await pipe.close();
        co_return;
      }
      auto buffer = read.result.MoveValueUnsafe();
      if (not buffer or buffer->size() == 0) {
        co_await pipe.close();
        co_return;
      }
      auto bytes = buffer->size();
      auto push_result = co_await pipe.push(chunk::make(std::move(buffer)));
      if (not push_result) {
        co_await pipe.close();
        co_return;
      }
      bytes_read_counter_.add(bytes);
      file_state.offset += detail::narrow<int64_t>(bytes);
      if (detail::narrow<size_t>(bytes) < read_size) {
        co_await pipe.close();
        co_return;
      }
      enqueue_task(
        ctx,
        [job_id = read.job_id, file = file_state.file,
         offset = file_state.offset] -> Task<AwaitResult> {
          co_return ReadProgress{
            job_id,
            co_await arrow_future_to_task(file->ReadAsync(offset, read_size)),
          };
        });
    },
    [this, &ctx](SubFinished& sub) -> Task<void> {
      auto slot = find_slot_by_job(sub.job_id);
      TENZIR_ASSERT(slot);
      auto path = std::move(processing_[*slot]->path);
      processing_[*slot].reset();
      start_job_in_slot(*slot, ctx);
      if (ctx.checkpoint_settings()) {
        cleanup_pending_.push_back(std::move(path));
      } else {
        co_await cleanup_file(std::move(path), ctx.dh());
      }
    });
}

auto FromArrowFsOperator::process_sub(SubKeyView key, table_slice slice,
                                      Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> {
  TENZIR_UNUSED(key, ctx);
  auto const rows = slice.rows();
  co_await push(std::move(slice));
  events_read_counter_.add(rows);
}

auto FromArrowFsOperator::finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
  -> Task<void> {
  co_await results_->enqueue(SubFinished{as<uint64_t>(key)});
}

auto FromArrowFsOperator::finalize(Push<table_slice>&, OpCtx& ctx)
  -> Task<FinalizeBehavior> {
  co_await cleanup_files(ctx.dh());
  co_return FinalizeBehavior::done;
}

auto FromArrowFsOperator::state() -> OperatorState {
  if (base_args_.watch) {
    return OperatorState::normal;
  }
  if (not scan_complete_) {
    return OperatorState::normal;
  }
  if (not pending_.empty()) {
    return OperatorState::normal;
  }
  for (auto& slot : processing_) {
    if (slot) {
      return OperatorState::normal;
    }
  }
  return OperatorState::done;
}

auto FromArrowFsOperator::snapshot(Serde& serde) -> void {
  serde("scan_complete_", scan_complete_);
  serde("pending_", pending_);
  serde("processing_", processing_);
  serde("next_job_id_", next_job_id_);
  serde("cleanup_pending_", cleanup_pending_);
  serde("previous_", previous_);
}

auto FromArrowFsOperator::cleanup_file(std::string path,
                                       diagnostic_handler& dh) const
  -> Task<void> {
  if (base_args_.remove) {
    co_await remove_file(path, dh);
    co_return;
  }
  if (base_args_.rename) {
    auto result = eval(*base_args_.rename, path, dh);
    auto* new_path = try_as<std::string>(result);
    if (not new_path) {
      diagnostic::warning("expected `string`, got `{}`",
                          type::infer(result).value_or(type{}).kind())
        .primary(*base_args_.rename)
        .emit(dh);
      co_return;
    }
    auto final_path = std::filesystem::path{*new_path};
    if (new_path->ends_with('/')) {
      auto filename = std::filesystem::path{path}.filename();
      final_path /= filename;
    }
    auto status
      = co_await spawn_blocking([fs = fs_, src_path = path, final_path] {
          auto parent_path = final_path.parent_path();
          if (not parent_path.empty() and parent_path != ".") {
            if (auto s = fs->CreateDir(parent_path, true); not s.ok()) {
              return s;
            }
          }
          // TODO: Verify that below is equivalent to fs->Move()
          return fs->CopyFile(src_path, final_path);
        });
    if (not status.ok()) {
      diagnostic::warning("failed to rename `{}` to `{}`", path, final_path)
        .primary(*base_args_.rename)
        .note(status.ToStringWithoutContextLines())
        .emit(dh);
      co_return;
    }
    co_await remove_file(path, dh);
    co_return;
  }
}

auto FromArrowFsOperator::cleanup_files(diagnostic_handler& dh) -> Task<void> {
  // PERF: We could probably batch these calls.
  auto tasks = std::vector<Task<void>>{};
  for (auto& path : cleanup_pending_) {
    tasks.push_back(cleanup_file(std::move(path), dh));
  }
  co_await folly::coro::collectAllRange(std::move(tasks));
  cleanup_pending_.clear();
}

auto FromArrowFsOperator::post_commit(OpCtx& ctx) -> Task<void> {
  co_await cleanup_files(ctx.dh());
}

auto FromArrowFsOperator::restore(OpCtx& ctx) -> Task<void> {
  // TODO: Parallelize this.
  for (const auto& [index, slot] : detail::enumerate(processing_)) {
    if (not slot) {
      continue;
    }
    auto& state = *slot;
    // The path can be empty if the file was put to be cleaned-up but the slot
    // was not reset.
    if (state.path.empty()) {
      slot.reset();
      continue;
    }
    // Verify file exists and mtime matches.
    auto info_future = fs_->GetFileInfoAsync({state.path});
    auto info_result = co_await arrow_future_to_task(std::move(info_future));
    if (not info_result.ok()) {
      slot.reset();
      continue;
    }
    auto info = info_result.MoveValueUnsafe();
    if (info.empty() or not info[0].IsFile()) {
      slot.reset();
      continue;
    }
    auto& file_info = info[0];
    auto file_mtime = to_option_time(file_info.mtime());
    if (file_mtime != state.mtime) {
      diagnostic::warning("file `{}` was modified since last checkpoint",
                          state.path)
        .primary(base_args_.url)
        .emit(ctx);
      if (state.offset < file_info.size()) {
        // Assume the file was appended to.
        state.mtime = file_mtime;
      } else {
        if (base_args_.watch) {
          pending_.push_back(TrackedFile{
            .path = state.path,
            .mtime = file_mtime,
            .file = nullptr,
          });
        }
        slot.reset();
        continue;
      }
    }
    // Reopen file.
    auto open_future = fs_->OpenInputFileAsync(state.path);
    auto open_result = co_await arrow_future_to_task(std::move(open_future));
    if (not open_result.ok()) {
      slot.reset();
      continue;
    }
    state.file = open_result.MoveValueUnsafe();
    previous_.emplace(file_info);
  }
  // Restore pending files
  auto restored = std::deque<TrackedFile>{};
  for (auto& file : pending_) {
    auto info_future = fs_->GetFileInfoAsync({file.path});
    auto info_result = co_await arrow_future_to_task(std::move(info_future));
    if (not info_result.ok()) {
      continue;
    }
    auto info = info_result.MoveValueUnsafe();
    if (info.empty() or not info[0].IsFile()) {
      continue;
    }
    previous_.emplace(info[0]);
    restored.push_back(std::move(file));
  }
  pending_ = std::move(restored);
}

auto FromArrowFsOperator::spawn_scan_task(OpCtx& ctx) -> void {
  ctx.spawn_task([this, &dh = ctx.dh()]() -> Task<void> {
    while (true) {
      auto start = std::chrono::steady_clock::now();
      auto files = std::vector<arrow::fs::FileInfo>{};
      auto root_result
        = co_await arrow_future_to_task(fs_->GetFileInfoAsync({root_path_}));
      if (not root_result.ok()) {
        diagnostic::error("failed to scan `{}`", root_path_)
          .primary(base_args_.url)
          .note(root_result.status().ToStringWithoutContextLines())
          .emit(dh);
      } else {
        TENZIR_ASSERT(root_result->size() == 1);
        auto root_info = std::move((*root_result)[0]);
        switch (root_info.type()) {
          case arrow::fs::FileType::NotFound:
            if (not base_args_.watch) {
              diagnostic::error("`{}` does not exist", root_path_)
                .primary(base_args_.url)
                .emit(dh);
              co_return;
            }
            break;
          case arrow::fs::FileType::Unknown:
            diagnostic::error("`{}` is unknown", root_path_)
              .primary(base_args_.url)
              .emit(dh);
            co_return;
          case arrow::fs::FileType::File:
            if (matches(root_info.path(), glob_)) {
              files.push_back(std::move(root_info));
              break;
            }
            if (not base_args_.watch) {
              diagnostic::error("`{}` is a file, not a directory", root_path_)
                .primary(base_args_.url)
                .emit(dh);
              co_return;
            }
            break;
          case arrow::fs::FileType::Directory: {
            auto sel = arrow::fs::FileSelector{};
            sel.base_dir = root_path_;
            sel.recursive = true;
            auto gen = fs_->GetFileInfoGenerator(sel);
            while (true) {
              auto batch = co_await arrow_future_to_task(gen());
              if (not batch.ok()) {
                diagnostic::error("failed to scan `{}`", root_path_)
                  .primary(base_args_.url)
                  .note(batch.status().ToStringWithoutContextLines())
                  .emit(dh);
                co_return;
              }
              if (batch->empty()) {
                break;
              }
              for (auto& file : *batch) {
                if (file.IsFile()) {
                  files.push_back(std::move(file));
                  continue;
                }
                // Clean up existing directory markers when `remove=true`.
                // Directory markers are 0-sized objects with keys ending in '/'.
                if (base_args_.remove and file.IsDirectory()) {
                  co_await remove_file(file.path() + '/', dh);
                }
              }
            }
            break;
          }
        }
      }
      // Apply glob and max_age filters.
      auto now = time::clock::now();
      std::erase_if(files, [&](const arrow::fs::FileInfo& file) {
        if (not matches(file.path(), glob_)) {
          return true;
        }
        if (base_args_.max_age) {
          if (file.mtime() != arrow::fs::kNoTime
              and now - file.mtime() >= *base_args_.max_age) {
            return true;
          }
        }
        return false;
      });
      co_await results_->enqueue(ScanComplete{std::move(files)});
      if (not base_args_.watch) {
        co_return;
      }
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed < watch_pause) {
        auto dur = std::chrono::duration_cast<folly::HighResDuration>(
          watch_pause - elapsed);
        co_await folly::coro::sleep(dur);
      }
    }
  });
}

auto FromArrowFsOperator::start_job_in_slot(size_t slot, OpCtx& ctx) -> void {
  if (pending_.empty()) {
    return;
  }
  processing_[slot] = std::move(pending_.front());
  processing_[slot]->job_id = ++next_job_id_;
  pending_.pop_front();
  enqueue_task(ctx,
               [this, job_id = processing_[slot]->job_id,
                path = processing_[slot]->path] mutable -> Task<AwaitResult> {
                 auto result = co_await arrow_future_to_task(
                   fs_->OpenInputFileAsync(path));
                 co_return FileOpen{job_id, std::move(result)};
               });
}

auto FromArrowFsOperator::find_free_slot() const -> Option<size_t> {
  for (auto i = size_t{0}; i < max_jobs; ++i) {
    if (not processing_[i]) {
      return i;
    }
  }
  return std::nullopt;
}

auto FromArrowFsOperator::find_slot_by_job(uint64_t job_id) const
  -> Option<size_t> {
  for (auto i = size_t{0}; i < max_jobs; ++i) {
    if (processing_[i] and processing_[i]->job_id == job_id) {
      return i;
    }
  }
  return std::nullopt;
}

auto FromArrowFsOperator::is_globbing() const -> bool {
  return glob_.size() != 1 or not is<std::string>(glob_[0]);
}

// =============================================================================
// ToArrowFsOperator
// =============================================================================

auto ToArrowFsOperator::setup_args(ToArrowFsArgs&, OpCtx&)
  -> Task<failure_or<void>> {
  co_return {};
}

auto ToArrowFsOperator::start(OpCtx& ctx) -> Task<void> {
  if (not co_await setup_args(base_args_, ctx)) {
    co_return;
  }
  auto resolved = co_await resolve_url(ctx);
  if (not resolved) {
    co_return;
  }
  // Parse the URL template: extract partition fields, validate structure,
  // and sanitize placeholders for URI parsing.
  auto tmpl
    = FsUrlTemplate::parse(std::move(*resolved), base_args_.partition_by,
                           base_args_.url.source, ctx.dh());
  if (not tmpl) {
    co_return;
  }
  template_ = std::move(*tmpl);
  // Create the filesystem from the sanitized URL (safe tokens in place of
  // placeholders).  The returned path will contain those safe tokens.
  auto fs = co_await make_filesystem(template_.sanitized_url(), ctx.dh());
  if (not fs) {
    co_return;
  }
  fs_ = std::move(fs->fs);
  // Restore real placeholders in the path extracted by make_filesystem.
  // `set_path` finalises `has_uuid()` based on the actual path portion and
  // emits any `{uuid}`-placement diagnostics.
  // TODO: Consider using separate types here to differentiate.
  template_.set_path(std::move(fs->path), base_args_.url.source, ctx.dh());
  bytes_written_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_arrow_fs"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_written_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_arrow_fs"},
                       MetricsDirection::write, MetricsVisibility::external_,
                       MetricsUnit::events);
}

auto ToArrowFsOperator::preprocess(table_slice input, OpCtx&)
  -> Task<table_slice> {
  co_return input;
}

auto ToArrowFsOperator::process(table_slice input, OpCtx& ctx) -> Task<void> {
  input = co_await preprocess(std::move(input), ctx);
  if (input.rows() == 0) {
    co_return;
  }
  auto const fields = template_.partition_fields();
  auto by = std::vector<series>{};
  by.reserve(fields.size());
  for (auto const& field : fields) {
    by.emplace_back(eval(field, input, ctx.dh()));
  }
  auto const rows = detail::narrow<int64_t>(input.rows());
  auto boundaries = std::set<int64_t>{rows};
  for (auto& s : by) {
    for (auto i = int64_t{0}; i + 1 < rows; ++i) {
      if (s.at(i) != s.at(i + 1)) {
        boundaries.insert(i + 1);
      }
    }
  }
  // Hash-bucket runs into one boolean mask per distinct key. Within-partition
  // row order is preserved; cross-partition order is unobservable (each
  // partition writes to its own stream).
  auto masks = std::unordered_map<data, std::shared_ptr<arrow::Buffer>>{};
  auto run_start = int64_t{0};
  for (auto boundary : boundaries) {
    auto key = list{};
    key.reserve(by.size());
    for (auto& s : by) {
      key.push_back(materialize(s.at(run_start)));
    }
    auto [it, inserted] = masks.try_emplace(std::move(key));
    if (inserted) {
      it->second = check(arrow::AllocateEmptyBitmap(rows, arrow_memory_pool()));
    }
    arrow::bit_util::SetBitsTo(it->second->mutable_data(), run_start,
                               boundary - run_start, true);
    run_start = boundary;
  }
  // One push per bucket. `process_sub` erases `key_to_sub` under the
  // guard when rotation fires, so the next iteration's lookup naturally
  // spawns a fresh sub without us having to drain anything in between.
  for (auto& [key, bitmap] : masks) {
    auto sub_key = int64_t{};
    {
      auto guard = co_await state_.lock();
      auto& kts = guard->key_to_sub;
      if (auto it = kts.find(key); it == kts.end()) {
        sub_key = next_sub_key_++;
        auto part = Partition{};
        part.key = key;
        auto [pit, _] = guard->partitions.emplace(sub_key, std::move(part));
        ++partition_count_;
        kts.emplace(key, sub_key);
        // Hold the lock across `spawn_sub` so concurrent `process_sub`
        // callers see the partition in the map before the sub can emit.
        co_await ctx.spawn_sub<table_slice>(sub_key, base_args_.pipe.inner);
        // Drive deadline-based rotation independently of incoming data.
        if (template_.has_uuid()) {
          auto cancel_token = pit->second.cancel_timeout.getToken();
          ctx.spawn_task(
            [this, sk = sub_key,
             cancel_token = std::move(cancel_token)] -> Task<void> {
              auto token = folly::cancellation_token_merge(
                co_await folly::coro::co_current_cancellation_token,
                cancel_token);
              co_await folly::coro::co_withCancellation(
                token, sleep_for(base_args_.timeout));
              co_await rotate(sk);
            });
        }
      } else {
        sub_key = it->second;
      }
    }
    auto sub = ctx.get_sub(sub_key);
    if (not sub) {
      diagnostic::error("subpipeline closed unexpectedly")
        .primary(base_args_.pipe)
        .emit(ctx.dh());
      co_return;
    }
    auto slice = filter(input, arrow::BooleanArray{rows, bitmap});
    auto const slice_rows = slice.rows();
    // `push` runs without the guard. If it suspends on a full input
    // channel and we still hold the guard, `process_sub` on the draining
    // side cannot acquire the guard to look up the partition, and
    // nothing drains — deadlock.
    auto result
      = co_await as<SubHandle<table_slice>>(*sub).push(std::move(slice));
    if (not result) {
      diagnostic::error("subpipeline closed unexpectedly")
        .primary(base_args_.pipe)
        .emit(ctx.dh());
      co_return;
    }
    events_written_counter_.add(slice_rows);
  }
}

auto ToArrowFsOperator::await_task(diagnostic_handler&) const -> Task<Any> {
  co_return co_await control_queue_->dequeue();
}

auto ToArrowFsOperator::process_task(Any result, OpCtx& ctx) -> Task<void> {
  auto msg = std::move(result).as<Message>();
  co_await co_match(msg, [&](RotateRequested& r) -> Task<void> {
    if (auto sub = ctx.get_sub(r.sub_key)) {
      co_await as<SubHandle<table_slice>>(*sub).close();
    }
  });
}

auto ToArrowFsOperator::process_sub(SubKeyView key, chunk_ptr chunk, OpCtx& ctx)
  -> Task<void> {
  auto sk = as<int64_t>(key);
  if (not chunk or chunk->size() == 0) {
    co_return;
  }
  auto part = co_await find_partition(sk);
  TENZIR_ASSERT(part, "partition must exist: inserted before spawn_sub, "
                      "erased only in finish_sub");
  if (not part->stream) {
    if (not co_await open_stream(*part, ctx.dh())) {
      co_return;
    }
  }
  auto status = co_await spawn_blocking(
    [stream = part->stream, buf = as_arrow_buffer(chunk)] {
      return stream->Write(buf);
    });
  if (not status.ok()) {
    diagnostic::error("failed to write to output stream: {}", status.ToString())
      .primary(base_args_.url)
      .emit(ctx.dh());
    co_return;
  }
  bytes_written_counter_.add(chunk->size());
  part->stream_bytes += chunk->size();
  const auto can_rotate = template_.has_uuid() and not part->is_rotating;
  const auto over_max_size = part->stream_bytes >= base_args_.max_size;
  if (can_rotate and over_max_size) {
    co_await rotate(sk);
  }
}

auto ToArrowFsOperator::rotate(int64_t sk) -> Task<void> {
  auto guard = co_await state_.lock();
  auto it = guard->partitions.find(sk);
  if (it == guard->partitions.end() or it->second.is_rotating) {
    co_return;
  }
  it->second.is_rotating = true;
  if (auto kit = guard->key_to_sub.find(it->second.key);
      kit != guard->key_to_sub.end() and kit->second == sk) {
    guard->key_to_sub.erase(kit);
  }
  control_queue_->enqueue(RotateRequested{sk});
}

auto ToArrowFsOperator::finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> {
  auto sk = as<int64_t>(key);
  auto part = co_await find_partition(sk);
  TENZIR_ASSERT(part, "partition must exist: finish_sub is the sole eraser");
  part->cancel_timeout.requestCancellation();
  co_await close_stream(*part, ctx.dh());
  auto guard = co_await state_.lock();
  // Drop the mapping if it still points at this sub (rotation may have
  // cleared it already).
  if (auto kit = guard->key_to_sub.find(part->key);
      kit != guard->key_to_sub.end() and kit->second == sk) {
    guard->key_to_sub.erase(kit);
  }
  guard->partitions.erase(sk);
  --partition_count_;
}

auto ToArrowFsOperator::state() -> OperatorState {
  if (finalized_ and partition_count_ == 0) {
    return OperatorState::done;
  }
  return OperatorState::normal;
}

auto ToArrowFsOperator::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  finalized_ = true;
  auto subs = std::vector<int64_t>{};
  {
    auto guard = co_await state_.lock();
    subs.reserve(guard->key_to_sub.size());
    for (auto& [_, sub_key] : guard->key_to_sub) {
      subs.push_back(sub_key);
    }
    guard->key_to_sub.clear();
  }
  auto close_tasks = std::vector<Task<void>>{};
  close_tasks.reserve(subs.size());
  for (auto sub_key : subs) {
    if (auto sub = ctx.get_sub(sub_key)) {
      close_tasks.push_back(as<SubHandle<table_slice>>(*sub).close());
    }
  }
  co_await folly::coro::collectAllRange(std::move(close_tasks));
  if (partition_count_ == 0) {
    co_return FinalizeBehavior::done;
  }
  co_return FinalizeBehavior::continue_;
}

auto ToArrowFsOperator::prepare_snapshot(OpCtx& ctx) -> Task<void> {
  auto tasks = std::vector<Task<void>>{};
  {
    auto guard = co_await state_.lock();
    tasks.reserve(guard->partitions.size());
    for (auto& [sub_key, part] : guard->partitions) {
      if (not part.stream) {
        continue;
      }
      tasks.push_back(folly::coro::co_invoke(
        [this, &ctx, stream = part.stream]() mutable -> Task<void> {
          auto status = co_await spawn_blocking([stream = std::move(stream)] {
            return stream->Flush();
          });
          if (not status.ok()) {
            diagnostic::error("failed to flush output stream: {}",
                              status.ToString())
              .primary(base_args_.url)
              .emit(ctx.dh());
          }
        }));
    }
  }
  co_await folly::coro::collectAllRange(std::move(tasks));
}

auto ToArrowFsOperator::find_partition(int64_t sk) -> Task<Option<Partition&>> {
  auto guard = co_await state_.lock();
  auto it = guard->partitions.find(sk);
  if (it == guard->partitions.end()) {
    co_return std::nullopt;
  }
  co_return it->second;
}

auto ToArrowFsOperator::open_stream(Partition& part,
                                    diagnostic_handler& dh) const
  -> Task<failure_or<void>> {
  auto path = template_.fill_path(part.key);
  auto [parent, _] = arrow::fs::internal::GetAbstractPathParent(path);
  if (not parent.empty() and fs_->type_name() == "local") {
    auto dir_status = co_await spawn_blocking([fs = fs_, parent] {
      return fs->CreateDir(parent, /*recursive=*/true);
    });
    if (not dir_status.ok()) {
      diagnostic::error("failed to create directory `{}`: {}", parent,
                        dir_status.ToString())
        .primary(base_args_.url)
        .emit(dh);
      co_return failure::promise();
    }
  }
  auto result = co_await spawn_blocking([fs = fs_, path] {
    return fs->OpenOutputStream(path);
  });
  if (not result.ok()) {
    diagnostic::error("failed to open output stream `{}`: {}", path,
                      result.status().ToString())
      .primary(base_args_.url)
      .emit(dh);
    co_return failure::promise();
  }
  part.stream = result.MoveValueUnsafe();
  part.stream_bytes = 0;
  part.created = time::clock::now();
  co_return {};
}

auto ToArrowFsOperator::close_stream(Partition& part,
                                     diagnostic_handler& dh) const
  -> Task<void> {
  if (not part.stream) {
    co_return;
  }
  auto status
    = co_await spawn_blocking([stream = std::exchange(part.stream, nullptr)] {
        return stream->Close();
      });
  if (not status.ok()) {
    diagnostic::warning("failed to close output stream: {}", status.ToString())
      .primary(base_args_.url)
      .emit(dh);
  }
}

} // namespace tenzir
