//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_fs.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/glob.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"

#include <arrow/filesystem/filesystem.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/futures/detail/Types.h>

namespace tenzir {

auto split_at_first_slash(std::string_view path)
  -> std::pair<std::string, std::string> {
  auto slash = path.find('/');
  if (slash == std::string_view::npos) {
    return {std::string{path}, ""};
  }
  return {std::string{path.substr(0, slash)},
          std::string{path.substr(slash + 1)}};
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

auto ArrowFsOperator::start(OpCtx& ctx) -> Task<void> {
  co_await OperatorBase::start(ctx);
  auto resolved = co_await resolve_url(ctx);
  if (resolved.is_error()) {
    co_return;
  }
  auto& uri = *resolved;
  auto fs = co_await make_filesystem(uri, ctx.dh());
  if (fs.is_error()) {
    co_return;
  }
  fs_ = std::move(fs->fs);
  TENZIR_ASSERT(fs_);
  glob_ = parse_glob(fs->path);
  root_path_ = extract_root_path(glob_, fs->path);
  co_await restore(ctx);
  auto ndh = null_diagnostic_handler{};
  co_await cleanup_files(ndh);
  if (base_args_.watch or not scan_complete_) {
    spawn_scan_task(ctx);
  }
}

auto ArrowFsOperator::await_task(diagnostic_handler&) const -> Task<Any> {
  co_return co_await results_->dequeue();
}

auto ArrowFsOperator::process_task(Any result, Push<table_slice>&, OpCtx& ctx)
  -> Task<void> {
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
      if (push_result.is_err()) {
        co_await pipe.close();
        co_return;
      }
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
      cleanup_pending_.push_back(std::move(processing_[*slot]->path));
      processing_[*slot].reset();
      start_job_in_slot(*slot, ctx);
      co_return;
    });
}

auto ArrowFsOperator::finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
  -> Task<void> {
  co_await results_->enqueue(SubFinished{as<uint64_t>(key)});
}

auto ArrowFsOperator::finalize(Push<table_slice>&, OpCtx& ctx)
  -> Task<FinalizeBehavior> {
  co_await cleanup_files(ctx.dh());
  co_return FinalizeBehavior::done;
}

auto ArrowFsOperator::state() -> OperatorState {
  if (base_args_.watch) {
    return OperatorState::unspecified;
  }
  if (not scan_complete_) {
    return OperatorState::unspecified;
  }
  if (not pending_.empty()) {
    return OperatorState::unspecified;
  }
  for (auto& slot : processing_) {
    if (slot) {
      return OperatorState::unspecified;
    }
  }
  return OperatorState::done;
}

auto ArrowFsOperator::snapshot(Serde& serde) -> void {
  serde("scan_complete_", scan_complete_);
  serde("pending_", pending_);
  serde("processing_", processing_);
  serde("next_job_id_", next_job_id_);
  serde("cleanup_pending_", cleanup_pending_);
  serde("previous_", previous_);
}

auto ArrowFsOperator::cleanup_file(std::string path,
                                   diagnostic_handler& dh) const -> Task<void> {
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
    auto status = co_await spawn_blocking([&, src_path = path, final_path] {
      auto parent_path = final_path.parent_path();
      if (not parent_path.empty() and parent_path != ".") {
        if (auto s = fs_->CreateDir(parent_path, true); not s.ok()) {
          return s;
        }
      }
      // TODO: Verify that below is equivalent to fs_->Move()
      return fs_->CopyFile(src_path, final_path);
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

auto ArrowFsOperator::cleanup_files(diagnostic_handler& dh) -> Task<void> {
  // PERF: We could probably batch these calls.
  auto tasks = std::vector<Task<void>>{};
  for (auto& path : cleanup_pending_) {
    tasks.push_back(cleanup_file(std::move(path), dh));
  }
  co_await folly::coro::collectAllRange(std::move(tasks));
  cleanup_pending_.clear();
}

auto ArrowFsOperator::post_commit(OpCtx& ctx) -> Task<void> {
  co_await cleanup_files(ctx.dh());
}

auto ArrowFsOperator::restore(OpCtx& ctx) -> Task<void> {
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

auto ArrowFsOperator::spawn_scan_task(OpCtx& ctx) -> void {
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

auto ArrowFsOperator::start_job_in_slot(size_t slot, OpCtx& ctx) -> void {
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

auto ArrowFsOperator::find_free_slot() const -> Option<size_t> {
  for (auto i = size_t{0}; i < max_jobs; ++i) {
    if (not processing_[i]) {
      return i;
    }
  }
  return std::nullopt;
}

auto ArrowFsOperator::find_slot_by_job(uint64_t job_id) const
  -> Option<size_t> {
  for (auto i = size_t{0}; i < max_jobs; ++i) {
    if (processing_[i] and processing_[i]->job_id == job_id) {
      return i;
    }
  }
  return std::nullopt;
}

auto ArrowFsOperator::is_globbing() const -> bool {
  return glob_.size() != 1 or not is<std::string>(glob_[0]);
}

} // namespace tenzir
