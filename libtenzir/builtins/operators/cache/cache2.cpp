//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cache2.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/oneshot.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/result.hpp>

#include <caf/actor_registry.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::cache::cache2 {

namespace {

using clock = std::chrono::steady_clock;
using read_result = Result<Option<nova::Events>, std::string>;

// NOTE: The sort order is relied upon for cache eviction.
enum class cache_state : uint8_t {
  failed,
  closed,
  open,
};

struct WriteResult {
  Result<bool, std::string> outcome;
  uint64_t bytes_added;
  Option<nova::Events> appended;
};

struct ReaderState {
  mutable bool cancelled = false;
};

struct PendingRead {
  Arc<ReaderState> reader;
  Arc<Oneshot<read_result>> result;
};

class Cache {
public:
  Cache(located<uint64_t> max_events, uint64_t max_bytes, duration read_timeout,
        duration write_timeout)
    : max_events_{max_events},
      max_bytes_{max_bytes},
      read_timeout_{read_timeout},
      write_timeout_{write_timeout} {
  }

  Cache(const Cache&) = delete;
  auto operator=(const Cache&) -> Cache& = delete;

  auto write(nova::Events events, diagnostic_handler& dh) -> Task<WriteResult> {
    auto waiters = std::vector<Arc<Oneshot<read_result>>>{};
    auto appended = Option<nova::Events>{None{}};
    auto exceeded_events = false;
    auto exceeded_bytes = false;
    auto accepted = true;
    auto bytes_added = uint64_t{0};
    {
      auto guard = std::scoped_lock{mutex_};
      if (state_ == cache_state::failed) {
        TENZIR_ASSERT(failure_);
        co_return WriteResult{Err{*failure_}, 0, None{}};
      }
      if (state_ == cache_state::closed) {
        co_return WriteResult{Err{"cache is already closed"}, 0, None{}};
      }
      if (not deadline_ and write_timeout_ > duration::zero()) {
        deadline_ = clock::now() + write_timeout_;
      }
      auto event_count = detail::narrow<uint64_t>(events.active_count());
      if (event_count == 0) {
        co_return WriteResult{true, 0, None{}};
      }
      if (event_count > max_events_.inner - event_count_) {
        events.mask = std::move(events.mask)
                        .keep_first(detail::narrow<nova::storage::Index>(
                          max_events_.inner - event_count_));
        event_count = detail::narrow<uint64_t>(events.active_count());
        exceeded_events = true;
        accepted = false;
      }
      if (event_count > 0) {
        const auto bytes = events.approx_bytes();
        if (byte_size_ + bytes > max_bytes_ and not exceeded_events) {
          exceeded_bytes = true;
          accepted = false;
        } else {
          const auto offset = events_.size();
          event_count_ += event_count;
          byte_size_ += bytes;
          bytes_added = detail::narrow<uint64_t>(bytes);
          events_.push_back(std::move(events));
          appended = events_.back();
          if (auto it = pending_.find(offset); it != pending_.end()) {
            std::ranges::transform(it->second, std::back_inserter(waiters),
                                   [](PendingRead& read) {
                                     return std::move(read.result);
                                   });
            pending_.erase(it);
          }
        }
      }
    }
    if (exceeded_events) {
      diagnostic::warning("cache exceeded capacity of {} events",
                          max_events_.inner)
        .primary(max_events_.source)
        .emit(dh);
    }
    if (exceeded_bytes) {
      diagnostic::warning("cache exceeded total capacity of {} MiB",
                          max_bytes_ / (1 << 20))
        .hint("consider increasing `tenzir.cache.capacity` option")
        .emit(dh);
    }
    TENZIR_ASSERT(waiters.empty() or appended);
    for (auto& waiter : waiters) {
      std::ignore = waiter->send(read_result{Option{*appended}});
    }
    co_return WriteResult{accepted, bytes_added, std::move(appended)};
  }

  auto read(uint64_t offset, Arc<ReaderState> reader) const
    -> Task<read_result> {
    auto waiter = Arc<Oneshot<read_result>>{std::in_place};
    {
      auto guard = std::scoped_lock{mutex_};
      if (reader->cancelled) {
        co_return Err{"cache read cancelled"};
      }
      if (offset < events_.size()) {
        if (state_ != cache_state::open) {
          deadline_ = clock::now() + read_timeout_;
        }
        co_return Option{events_[offset]};
      }
      if (state_ == cache_state::closed and offset == events_.size()) {
        deadline_ = clock::now() + read_timeout_;
        co_return Option<nova::Events>{None{}};
      }
      if (state_ == cache_state::failed) {
        TENZIR_ASSERT(failure_);
        co_return Err{*failure_};
      }
      pending_[offset].push_back(PendingRead{std::move(reader), waiter});
    }
    co_return co_await waiter->recv();
  }

  auto cancel(const Arc<ReaderState>& reader) const -> void {
    auto cancelled = std::vector<Arc<Oneshot<read_result>>>{};
    {
      auto guard = std::scoped_lock{mutex_};
      reader->cancelled = true;
      for (auto it = pending_.begin(); it != pending_.end();) {
        auto& reads = it->second;
        for (auto read = reads.begin(); read != reads.end();) {
          if (std::addressof(*read->reader) == std::addressof(*reader)) {
            cancelled.push_back(std::move(read->result));
            read = reads.erase(read);
          } else {
            ++read;
          }
        }
        if (reads.empty()) {
          it = pending_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto& result : cancelled) {
      std::ignore = result->send(read_result{Err{"cache read cancelled"}});
    }
  }

  auto finish() -> void {
    auto waiters = std::vector<Arc<Oneshot<read_result>>>{};
    {
      auto guard = std::scoped_lock{mutex_};
      if (state_ != cache_state::open) {
        return;
      }
      state_ = cache_state::closed;
      deadline_ = clock::now() + read_timeout_;
      for (auto& [offset, pending] : pending_) {
        TENZIR_UNUSED(offset);
        std::ranges::transform(pending, std::back_inserter(waiters),
                               [](PendingRead& read) {
                                 return std::move(read.result);
                               });
      }
      pending_.clear();
    }
    for (auto& waiter : waiters) {
      std::ignore = waiter->send(read_result{Option<nova::Events>{None{}}});
    }
  }

  auto fail() -> void {
    expire_with("cache writer disappeared");
  }

  auto expire() -> void {
    expire_with("cache expired");
  }

  auto due(clock::time_point now) const -> bool {
    auto guard = std::scoped_lock{mutex_};
    return state_ == cache_state::failed or (deadline_ and now >= *deadline_);
  }

  struct Snapshot {
    cache_state state;
    clock::time_point created_at;
  };

  auto snapshot() const -> Snapshot {
    auto guard = std::scoped_lock{mutex_};
    return {
      .state = state_,
      .created_at = created_at_,
    };
  }

private:
  auto expire_with(std::string error) -> void {
    auto waiters = std::vector<Arc<Oneshot<read_result>>>{};
    {
      auto guard = std::scoped_lock{mutex_};
      if (state_ == cache_state::failed) {
        return;
      }
      state_ = cache_state::failed;
      failure_ = std::move(error);
      deadline_ = None{};
      for (auto& [offset, pending] : pending_) {
        TENZIR_UNUSED(offset);
        std::ranges::transform(pending, std::back_inserter(waiters),
                               [](PendingRead& read) {
                                 return std::move(read.result);
                               });
      }
      pending_.clear();
    }
    for (auto& waiter : waiters) {
      std::ignore = waiter->send(read_result{Err{*failure_}});
    }
  }

  // The writer lease must be able to fail the cache synchronously from its
  // destructor, so this state deliberately uses a short-held synchronous
  // mutex rather than an async mutex.
  mutable std::mutex mutex_;
  std::vector<nova::Events> events_;
  mutable std::unordered_map<uint64_t, std::vector<PendingRead>> pending_;
  located<uint64_t> max_events_;
  uint64_t max_bytes_ = 0;
  duration read_timeout_ = {};
  duration write_timeout_ = {};
  uint64_t event_count_ = 0;
  uint64_t byte_size_ = 0;
  cache_state state_ = cache_state::open;
  Option<std::string> failure_ = None{};
  mutable Option<clock::time_point> deadline_ = None{};
  clock::time_point created_at_ = clock::now();
};

class Writer {
public:
  explicit Writer(Arc<Cache> cache) : cache_{std::move(cache)} {
  }

  ~Writer() {
    if (cache_.not_moved_from() and not finished_) {
      cache_->fail();
    }
  }

  Writer(const Writer&) = delete;
  auto operator=(const Writer&) -> Writer& = delete;

  Writer(Writer&& other) noexcept
    : cache_{std::move(other.cache_)},
      finished_{std::exchange(other.finished_, true)} {
  }

  auto operator=(Writer&& other) noexcept -> Writer& {
    if (this != &other) {
      if (cache_.not_moved_from() and not finished_) {
        cache_->fail();
      }
      cache_ = std::move(other.cache_);
      finished_ = std::exchange(other.finished_, true);
    }
    return *this;
  }

  auto write(nova::Events events, diagnostic_handler& dh) -> Task<WriteResult> {
    co_return co_await cache_->write(std::move(events), dh);
  }

  auto finish() -> void {
    if (not finished_) {
      cache_->finish();
      finished_ = true;
    }
  }

  auto identity() const -> const Cache* {
    return std::addressof(*cache_);
  }

private:
  Arc<Cache> cache_;
  bool finished_ = false;
};

class Manager {
public:
  auto configure(uint64_t max_bytes) -> void {
    auto guard = std::scoped_lock{mutex_};
    TENZIR_ASSERT(caches_.empty());
    max_bytes_ = max_bytes;
  }

  auto create(const CacheArgs& args) -> Result<Option<Writer>, std::string> {
    auto guard = std::scoped_lock{mutex_};
    if (closed_) {
      return Err{"cache manager is unavailable"};
    }
    if (caches_.contains(args.id)) {
      return Option<Writer>{None{}};
    }
    TENZIR_ASSERT(args.read_timeout);
    auto cache = Arc<Cache>{
      std::in_place,
      args.capacity ? *args.capacity
                    : located<uint64_t>{std::numeric_limits<uint64_t>::max(),
                                        location::unknown},
      max_bytes_, args.read_timeout->inner,
      args.write_timeout ? args.write_timeout->inner : duration::zero()};
    caches_.emplace(args.id, ManagedCache{cache});
    return Option{Writer{std::move(cache)}};
  }

  auto get(const std::string& id) -> Result<Arc<Cache>, std::string> {
    auto guard = std::scoped_lock{mutex_};
    if (closed_) {
      return Err{"cache manager is unavailable"};
    }
    const auto it = caches_.find(id);
    if (it == caches_.end()) {
      return Err{fmt::format("cache `{}` does not exist", id)};
    }
    return it->second.cache;
  }

  auto expire() -> void {
    auto expired = std::vector<Arc<Cache>>{};
    const auto now = clock::now();
    {
      auto guard = std::scoped_lock{mutex_};
      for (auto it = caches_.begin(); it != caches_.end();) {
        if (it->second.cache->due(now)) {
          expired.push_back(it->second.cache);
          TENZIR_ASSERT_GEQ(total_bytes_, it->second.bytes);
          total_bytes_ -= it->second.bytes;
          it = caches_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto& cache : expired) {
      cache->expire();
    }
  }

  auto account_write(const std::string& id, const Cache* identity,
                     uint64_t bytes_added) -> void {
    auto evicted = std::vector<Arc<Cache>>{};
    {
      auto guard = std::scoped_lock{mutex_};
      const auto it = caches_.find(id);
      if (it == caches_.end()
          or std::addressof(*it->second.cache) != identity) {
        return;
      }
      it->second.bytes += bytes_added;
      total_bytes_ += bytes_added;
      while (total_bytes_ > max_bytes_ and not caches_.empty()) {
        const auto oldest = std::ranges::min_element(
          caches_, std::ranges::less{}, [](const auto& item) {
            const auto snapshot = item.second.cache->snapshot();
            return std::tie(snapshot.state, snapshot.created_at);
          });
        TENZIR_ASSERT(oldest != caches_.end());
        TENZIR_ASSERT_GEQ(total_bytes_, oldest->second.bytes);
        total_bytes_ -= oldest->second.bytes;
        evicted.push_back(oldest->second.cache);
        caches_.erase(oldest);
      }
    }
    for (auto& cache : evicted) {
      cache->expire();
    }
  }

  auto close() -> void {
    auto caches = std::vector<Arc<Cache>>{};
    {
      auto guard = std::scoped_lock{mutex_};
      if (closed_) {
        return;
      }
      closed_ = true;
      for (auto& [id, cache] : caches_) {
        TENZIR_UNUSED(id);
        caches.push_back(std::move(cache.cache));
      }
      caches_.clear();
      total_bytes_ = 0;
    }
    for (auto& cache : caches) {
      cache->expire();
    }
  }

private:
  struct ManagedCache {
    Arc<Cache> cache;
    uint64_t bytes = 0;
  };

  std::mutex mutex_;
  std::unordered_map<std::string, ManagedCache> caches_;
  uint64_t total_bytes_ = 0;
  uint64_t max_bytes_ = 0;
  bool closed_ = false;
};

// A process hosts exactly one node, and all of its pipelines share this store.
auto manager = Arc<Manager>{std::in_place};

// The registry may contain a proxy for a remote node, so checking only whether
// a node is registered would also accept client processes.
auto is_node_process(caf::actor_system& system) -> bool {
  const auto node = system.registry().get<node_actor>("tenzir.node");
  return node and node->node() == system.node();
}

auto reject_client_process(OpCtx& ctx) -> bool {
  if (is_node_process(ctx.actor_system())) {
    return false;
  }
  diagnostic::error("`cache` is only available in node pipelines")
    .note("launch the pipeline on the node instead of running it from a client")
    .emit(ctx);
  return true;
}

class WriteCacheSink final : public Operator<nova::Events, void> {
public:
  explicit WriteCacheSink(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (reject_client_process(ctx)) {
      done_ = true;
      co_return;
    }
    manager_ = manager;
    auto result = (*manager_)->create(args_);
    if (not result) {
      diagnostic::error("{}", std::move(result).unwrap_err())
        .note("failed to create cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto writer = std::move(result).unwrap();
    if (not writer) {
      diagnostic::error("cache `{}` already has a writer", args_.id).emit(ctx);
      done_ = true;
      co_return;
    }
    writer_ = std::move(*writer);
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    TENZIR_ASSERT(writer_);
    const auto result = co_await writer_->write(std::move(input), ctx.dh());
    (*manager_)->account_write(args_.id, writer_->identity(),
                               result.bytes_added);
    if (not result.outcome) {
      diagnostic::error("{}", result.outcome.unwrap_err())
        .note("failed to write to cache")
        .emit(ctx);
      done_ = true;
    } else if (not result.outcome.unwrap()) {
      done_ = true;
    }
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    if (writer_) {
      writer_->finish();
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  CacheArgs args_;
  Option<Arc<Manager>> manager_ = None{};
  Option<Writer> writer_ = None{};
  bool done_ = false;
};

class ReadCacheSource final : public Operator<void, nova::Events> {
public:
  explicit ReadCacheSource(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (reject_client_process(ctx)) {
      done_ = true;
      co_return;
    }
    auto result = manager->get(args_.id);
    if (not result) {
      diagnostic::error("{}", std::move(result).unwrap_err())
        .note("failed to retrieve cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    cache_ = std::move(result).unwrap();
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await (*cache_)->read(read_offset_, reader_);
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto& read = result.as<read_result>();
    if (not read) {
      if (not stopping_) {
        diagnostic::error("{}", std::move(read).unwrap_err())
          .note("failed to read from cache")
          .emit(ctx);
      }
      done_ = true;
      co_return;
    }
    auto events = std::move(read).unwrap();
    if (not events) {
      done_ = true;
      co_return;
    }
    co_await push(std::move(*events));
    ++read_offset_;
  }

  auto stop(OpCtx&) -> Task<void> override {
    stopping_ = true;
    if (cache_) {
      (*cache_)->cancel(reader_);
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  CacheArgs args_;
  Option<Arc<Cache>> cache_ = None{};
  Arc<ReaderState> reader_{std::in_place};
  mutable uint64_t read_offset_ = 0;
  bool stopping_ = false;
  bool done_ = false;
};

class CacheReadwrite final : public Operator<nova::Events, nova::Events> {
public:
  explicit CacheReadwrite(CacheArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (reject_client_process(ctx)) {
      done_ = true;
      co_return;
    }
    manager_ = manager;
    auto result = (*manager_)->create(args_);
    if (not result) {
      diagnostic::error("{}", std::move(result).unwrap_err())
        .note("failed to create cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    auto writer = std::move(result).unwrap();
    if (not writer) {
      diagnostic::error("cache `{}` already has a writer", args_.id).emit(ctx);
      done_ = true;
      co_return;
    }
    writer_ = std::move(*writer);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_ASSERT(writer_);
    auto result = co_await writer_->write(std::move(input), ctx.dh());
    (*manager_)->account_write(args_.id, writer_->identity(),
                               result.bytes_added);
    if (not result.outcome) {
      diagnostic::error("{}", result.outcome.unwrap_err())
        .note("failed to write to cache")
        .emit(ctx);
      done_ = true;
      co_return;
    }
    if (result.appended) {
      co_await push(std::move(*result.appended));
    }
    if (not result.outcome.unwrap()) {
      done_ = true;
    }
  }

  auto finalize(Push<nova::Events>&, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (writer_) {
      writer_->finish();
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  CacheArgs args_;
  Option<Arc<Manager>> manager_ = None{};
  Option<Writer> writer_ = None{};
  bool done_ = false;
};

} // namespace

auto configure(uint64_t max_bytes) -> void {
  manager->configure(max_bytes);
}

auto shutdown() -> void {
  manager->close();
}

auto expire() -> void {
  manager->expire();
}

auto make_write(CacheArgs args) -> AnyOperator {
  return WriteCacheSink{std::move(args)}.with_name("cache");
}

auto make_read(CacheArgs args) -> AnyOperator {
  return ReadCacheSource{std::move(args)}.with_name("cache");
}

auto make_readwrite(CacheArgs args) -> AnyOperator {
  return CacheReadwrite{std::move(args)}.with_name("cache");
}

} // namespace tenzir::plugins::cache::cache2
