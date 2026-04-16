//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/dns.hpp"

#include "tenzir/async/mutex.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/lru_cache.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/socket.hpp"

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <algorithm>
#include <ares.h>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <netdb.h>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace tenzir {

namespace {

using Clock = std::chrono::steady_clock;
using DnsDuration = Clock::duration;

auto normalize_config(DnsResolverConfig config) -> DnsResolverConfig {
  config.max_in_flight = std::max(size_t{1}, config.max_in_flight);
  config.max_entries = std::max(size_t{1}, config.max_entries);
  config.tries = std::max(size_t{1}, config.tries);
  config.timeout = std::max(config.timeout, std::chrono::milliseconds::zero());
  config.positive_ttl
    = std::max(config.positive_ttl, std::chrono::seconds::zero());
  config.negative_ttl
    = std::max(config.negative_ttl, std::chrono::seconds::zero());
  config.literal_ttl
    = std::max(config.literal_ttl, std::chrono::seconds::zero());
  return config;
}

struct AresLibrary {
  AresLibrary() {
    status = static_cast<ares_status_t>(ares_library_init(ARES_LIB_INIT_ALL));
  }

  ~AresLibrary() {
    if (status == ARES_SUCCESS) {
      ares_library_cleanup();
    }
  }

  ares_status_t status = ARES_SUCCESS;
};

auto ares_library() -> AresLibrary& {
  static auto result = AresLibrary{};
  return result;
}

class AresChannel {
public:
  explicit AresChannel(DnsResolverConfig const& config) {
    status_ = ares_library().status;
    if (status_ != ARES_SUCCESS) {
      return;
    }
    auto options = ares_options{};
    options.timeout = detail::narrow<int>(std::min<int64_t>(
      config.timeout.count(), std::numeric_limits<int>::max()));
    options.tries = detail::narrow<int>(
      std::min<size_t>(config.tries, size_t{std::numeric_limits<int>::max()}));
    options.evsys = ARES_EVSYS_DEFAULT;
    auto optmask
      = int{ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES | ARES_OPT_EVENT_THREAD};
    status_ = static_cast<ares_status_t>(
      ares_init_options(&channel_, &options, optmask));
    if (status_ != ARES_SUCCESS) {
      channel_ = nullptr;
    }
  }

  AresChannel(AresChannel const&) = delete;
  auto operator=(AresChannel const&) -> AresChannel& = delete;
  AresChannel(AresChannel&&) = delete;
  auto operator=(AresChannel&&) -> AresChannel& = delete;

  ~AresChannel() {
    if (channel_) {
      ares_destroy(channel_);
    }
  }

  [[nodiscard]] auto valid() const -> bool {
    return channel_ != nullptr;
  }

  [[nodiscard]] auto get() const -> ares_channel {
    return channel_;
  }

  [[nodiscard]] auto status() const -> ares_status_t {
    return status_;
  }

private:
  ares_channel channel_ = nullptr;
  ares_status_t status_ = ARES_ENOTINITIALIZED;
};

auto is_not_found_status(int status) -> bool {
  switch (status) {
    case ARES_ENODATA:
    case ARES_ENOTFOUND:
    case ARES_ENONAME:
      return true;
    default:
      return false;
  }
}

auto make_dns_error(int status) -> DnsError {
  return {
    .error = std::string{ares_strerror(status)},
  };
}

auto make_dns_error(std::string error) -> DnsError {
  return {
    .error = std::move(error),
  };
}

auto make_forward_not_found() -> ForwardDnsResult {
  return ForwardDnsLookup{DnsNotFound{}};
}

auto make_reverse_not_found() -> ReverseDnsResult {
  return ReverseDnsLookup{DnsNotFound{}};
}

auto make_forward_failure(int status) -> ForwardDnsResult {
  return Err{make_dns_error(status)};
}

auto make_forward_failure(std::string error) -> ForwardDnsResult {
  return Err{make_dns_error(std::move(error))};
}

auto make_reverse_failure(int status) -> ReverseDnsResult {
  return Err{make_dns_error(status)};
}

auto make_reverse_failure(std::string error) -> ReverseDnsResult {
  return Err{make_dns_error(std::move(error))};
}

auto make_forward_literal_result(ip address, std::string canonical_name,
                                 DnsResolverConfig const& config)
  -> ForwardDnsResolved {
  auto result = ForwardDnsResolved{};
  result.canonical_name = std::move(canonical_name);
  result.answers.push_back(ForwardDnsAnswer{
    .address = address,
    .type = address.is_v4() ? "A" : "AAAA",
    .ttl = config.literal_ttl,
  });
  return result;
}

auto local_forward_result(std::string_view hostname,
                          DnsResolverConfig const& config)
  -> Option<ForwardDnsResult> {
  if (auto parsed = to<ip>(std::string{hostname})) {
    return ForwardDnsResult{
      make_forward_literal_result(*parsed, std::string{hostname}, config)};
  }
  return None{};
}

auto local_reverse_result(ip address, DnsResolverConfig const&)
  -> Option<ReverseDnsResult> {
  if (address.is_loopback()) {
    return ReverseDnsResult{ReverseDnsResolved{
      .hostname = std::string{"localhost"},
    }};
  }
  return None{};
}

auto sockaddr_to_ip(sockaddr const* address) -> Option<ip> {
  auto result = ip{};
  switch (address->sa_family) {
    case AF_INET: {
      auto in = sockaddr_in{};
      std::memcpy(&in, address, sizeof(in));
      if (auto err = convert(in, result)) {
        return None{};
      }
      return result;
    }
    case AF_INET6: {
      auto in = sockaddr_in6{};
      std::memcpy(&in, address, sizeof(in));
      if (auto err = convert(in, result)) {
        return None{};
      }
      return result;
    }
    default:
      return None{};
  }
}

auto make_sockaddr(ip address, sockaddr_storage& storage,
                   ares_socklen_t& length) -> void {
  if (address.is_v4()) {
    auto sockaddr = sockaddr_in{};
    auto err = convert(address, sockaddr);
    TENZIR_ASSERT(not err);
    std::memcpy(&storage, &sockaddr, sizeof(sockaddr));
    length = sizeof(sockaddr);
  } else {
    auto sockaddr = sockaddr_in6{};
    auto err = convert(address, sockaddr);
    TENZIR_ASSERT(not err);
    std::memcpy(&storage, &sockaddr, sizeof(sockaddr));
    length = sizeof(sockaddr);
  }
}

struct ForwardQueryState {
  Notify notify;
  ForwardDnsResult result = make_forward_not_found();
  std::string hostname;
};

struct ReverseQueryState {
  Notify notify;
  ReverseDnsResult result = make_reverse_not_found();
  sockaddr_storage storage{};
  ares_socklen_t length = 0;
};

template <class State>
struct CallbackCookie {
  explicit CallbackCookie(Arc<State> state) : state{std::move(state)} {
  }

  Arc<State> state;
};

template <class State>
auto make_callback_cookie(Arc<State> state) -> void* {
  return new CallbackCookie<State>{std::move(state)};
}

template <class State>
auto take_callback_state(void* opaque) -> Arc<State> {
  auto cookie = Box<CallbackCookie<State>>::from_non_null(
    std::unique_ptr<CallbackCookie<State>>{
      static_cast<CallbackCookie<State>*>(opaque),
    });
  return std::move(cookie->state);
}

auto forward_query_callback(void* opaque, int status, int /*timeouts*/,
                            ares_addrinfo* info) -> void {
  auto state = take_callback_state<ForwardQueryState>(opaque);
  if (status != ARES_SUCCESS) {
    state->result = is_not_found_status(status) ? make_forward_not_found()
                                                : make_forward_failure(status);
    if (info) {
      ares_freeaddrinfo(info);
    }
    state->notify.notify_one();
    return;
  }
  TENZIR_ASSERT(info);
  auto resolved = ForwardDnsResolved{};
  if (info->name) {
    resolved.canonical_name = std::string{info->name};
  } else if (info->cnames and info->cnames->name) {
    resolved.canonical_name = std::string{info->cnames->name};
  }
  for (auto* node = info->nodes; node != nullptr; node = node->ai_next) {
    auto address = sockaddr_to_ip(node->ai_addr);
    if (not address) {
      continue;
    }
    resolved.answers.push_back(ForwardDnsAnswer{
      .address = std::move(*address),
      .type = node->ai_family == AF_INET ? "A" : "AAAA",
      .ttl = std::chrono::seconds{std::max(node->ai_ttl, 0)},
    });
  }
  ares_freeaddrinfo(info);
  state->result = resolved.answers.empty()
                    ? make_forward_not_found()
                    : ForwardDnsResult{std::move(resolved)};
  state->notify.notify_one();
}

auto reverse_query_callback(void* opaque, int status, int /*timeouts*/,
                            char* node, char* /*service*/) -> void {
  auto state = take_callback_state<ReverseQueryState>(opaque);
  if (status != ARES_SUCCESS) {
    state->result = is_not_found_status(status) ? make_reverse_not_found()
                                                : make_reverse_failure(status);
    state->notify.notify_one();
    return;
  }
  state->result = node ? ReverseDnsResult{ReverseDnsResolved{.hostname = node}}
                       : make_reverse_not_found();
  state->notify.notify_one();
}

auto forward_query(AresChannel const& channel, std::string hostname)
  -> Task<ForwardDnsResult> {
  auto state = Arc<ForwardQueryState>{std::in_place};
  state->hostname = std::move(hostname);
  if (not channel.valid()) {
    co_return make_forward_failure(channel.status());
  }
  auto hints = ares_addrinfo_hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_flags = ARES_AI_CANONNAME;
  ares_getaddrinfo(channel.get(), state->hostname.c_str(), nullptr, &hints,
                   forward_query_callback, make_callback_cookie(state));
  co_await state->notify.wait();
  co_return state->result;
}

auto reverse_query(AresChannel const& channel, ip address)
  -> Task<ReverseDnsResult> {
  auto state = Arc<ReverseQueryState>{std::in_place};
  make_sockaddr(address, state->storage, state->length);
  if (not channel.valid()) {
    co_return make_reverse_failure(channel.status());
  }
  ares_getnameinfo(channel.get(),
                   reinterpret_cast<sockaddr const*>(&state->storage),
                   state->length, NI_NAMEREQD, reverse_query_callback,
                   make_callback_cookie(state));
  co_await state->notify.wait();
  co_return state->result;
}

template <class Key, class Result>
struct PendingLookup {
  Option<Result> result = None{};
  std::vector<Arc<Notify>> waiters;
};

template <class Result>
struct CacheEntry {
  Result result;
  Clock::time_point inserted_at;
  Clock::time_point expires_at;
};

template <class Key, class Result>
struct CacheFactory {
  auto operator()(Key const&) const -> CacheEntry<Result> {
    panic("detail::lru_cache factory must not run for DNS lookups");
  }
};

template <class Key, class Result>
struct LookupState {
  using cache_type
    = detail::lru_cache<Key, CacheEntry<Result>, CacheFactory<Key, Result>>;

  explicit LookupState(size_t max_entries)
    : cache{max_entries, CacheFactory<Key, Result>{}} {
  }

  cache_type cache;
  std::unordered_map<Key, Arc<PendingLookup<Key, Result>>> pending;
};

template <class T>
auto same_arc(Arc<T> const& lhs, Arc<T> const& rhs) -> bool {
  return std::addressof(*lhs) == std::addressof(*rhs);
}

template <class Result>
auto adjust_cached_result(CacheEntry<Result> const& entry,
                          Clock::time_point /*now*/) -> Result {
  return entry.result;
}

auto adjust_cached_result(CacheEntry<ForwardDnsResult> const& entry,
                          Clock::time_point now) -> ForwardDnsResult {
  auto result = entry.result;
  if (result.is_err()) {
    return result;
  }
  auto* resolved = try_as<ForwardDnsResolved>(&result.unwrap());
  if (not resolved) {
    return result;
  }
  auto age
    = std::chrono::duration_cast<std::chrono::seconds>(now - entry.inserted_at);
  for (auto& answer : resolved->answers) {
    answer.ttl = std::max(answer.ttl - age, std::chrono::seconds{0});
  }
  return result;
}

template <class Key, class Result>
auto lookup_cached(LookupState<Key, Result>& state, Key const& key)
  -> Option<Result> {
  auto now = Clock::now();
  auto entry = state.cache.get(key);
  if (not entry) {
    return None{};
  }
  if (entry->expires_at <= now) {
    state.cache.drop(key);
    return None{};
  }
  return adjust_cached_result(*entry, now);
}

template <class Key, class Result>
auto insert_cached(LookupState<Key, Result>& state, Key const& key,
                   Result const& result, DnsDuration ttl) -> void {
  if (ttl <= DnsDuration::zero()) {
    return;
  }
  auto now = Clock::now();
  state.cache.put(key, CacheEntry<Result>{
                         .result = result,
                         .inserted_at = now,
                         .expires_at = now + ttl,
                       });
}

auto ttl_for(DnsResolverConfig const& config, ForwardDnsResult const& result)
  -> DnsDuration {
  if (result.is_err()) {
    return config.negative_ttl;
  }
  if (auto* resolved = try_as<ForwardDnsResolved>(&result.unwrap())) {
    auto ttl = std::chrono::seconds::max();
    for (auto const& answer : resolved->answers) {
      ttl = std::min(ttl, answer.ttl);
    }
    return ttl == std::chrono::seconds::max() ? config.positive_ttl : ttl;
  }
  return config.negative_ttl;
}

auto ttl_for(DnsResolverConfig const& config, ReverseDnsResult const& result)
  -> DnsDuration {
  if (result.is_err()) {
    return config.negative_ttl;
  }
  return is<ReverseDnsResolved>(result.unwrap()) ? config.positive_ttl
                                                 : config.negative_ttl;
}

auto notify_waiters(std::vector<Arc<Notify>> waiters) -> void {
  for (auto& waiter : waiters) {
    waiter->notify_one();
  }
}

template <class Key, class Result>
auto finish_lookup(Mutex<LookupState<Key, Result>>& state_mutex, Key const& key,
                   Arc<PendingLookup<Key, Result>> pending,
                   Result const& result, DnsDuration ttl)
  -> Task<std::vector<Arc<Notify>>> {
  auto state = co_await state_mutex.lock();
  insert_cached(*state, key, result, ttl);
  auto it = state->pending.find(key);
  TENZIR_ASSERT(it != state->pending.end());
  TENZIR_ASSERT(same_arc(it->second, pending));
  pending->result = result;
  auto waiters = std::move(pending->waiters);
  state->pending.erase(it);
  co_return waiters;
}

template <class Key, class Result>
auto remove_waiter(Mutex<LookupState<Key, Result>>& state_mutex, Key const& key,
                   Arc<PendingLookup<Key, Result>> pending, Arc<Notify> waiter)
  -> Task<void> {
  auto state = co_await state_mutex.lock();
  auto it = state->pending.find(key);
  if (it == state->pending.end() or not same_arc(it->second, pending)) {
    co_return;
  }
  auto& waiters = pending->waiters;
  auto waiter_it
    = std::remove_if(waiters.begin(), waiters.end(), [&](auto const& x) {
        return same_arc(x, waiter);
      });
  waiters.erase(waiter_it, waiters.end());
}

template <class Impl, class Awaitable>
auto spawn_lookup(Impl& impl, Awaitable awaitable) -> void {
  impl.scope_.add(
    folly::coro::co_withExecutor(
      impl.executor_,
      folly::coro::co_invoke(
        [awaitable = std::move(awaitable)]() mutable -> Task<void> {
          std::ignore = co_await folly::coro::co_withCancellation(
            folly::CancellationToken{}, std::move(awaitable));
        })),
    FOLLY_ASYNC_STACK_RETURN_ADDRESS());
}

template <class Impl>
auto run_forward_lookup(
  Impl* impl, std::string hostname,
  Arc<PendingLookup<std::string, ForwardDnsResult>> pending)
  -> Task<ForwardDnsResult> {
  auto lookup_result
    = co_await folly::coro::co_awaitTry([&]() -> Task<ForwardDnsResult> {
        if (auto local = local_forward_result(hostname, impl->config_)) {
          co_return *local;
        }
        auto permit = co_await impl->permits_.acquire();
        co_return co_await impl->query_(hostname);
      }());
  auto result = make_forward_not_found();
  if (lookup_result.hasException()) {
    auto exception = std::move(lookup_result).exception();
    result = exception.template is_compatible_with<folly::OperationCancelled>()
               ? make_forward_failure("DNS lookup cancelled")
               : make_forward_failure("DNS lookup aborted");
  } else {
    result = std::move(lookup_result).value();
  }
  auto waiters = co_await finish_lookup(impl->state_, hostname, pending, result,
                                        ttl_for(impl->config_, result));
  notify_waiters(std::move(waiters));
  co_return result;
}

template <class Impl>
auto run_reverse_lookup(Impl* impl, ip address,
                        Arc<PendingLookup<ip, ReverseDnsResult>> pending)
  -> Task<ReverseDnsResult> {
  auto lookup_result
    = co_await folly::coro::co_awaitTry([&]() -> Task<ReverseDnsResult> {
        if (auto local = local_reverse_result(address, impl->config_)) {
          co_return *local;
        }
        auto permit = co_await impl->permits_.acquire();
        co_return co_await impl->query_(address);
      }());
  auto result = make_reverse_not_found();
  if (lookup_result.hasException()) {
    auto exception = std::move(lookup_result).exception();
    result = exception.template is_compatible_with<folly::OperationCancelled>()
               ? make_reverse_failure("DNS lookup cancelled")
               : make_reverse_failure("DNS lookup aborted");
  } else {
    result = std::move(lookup_result).value();
  }
  auto ttl = address.is_loopback() ? DnsDuration{impl->config_.literal_ttl}
                                   : ttl_for(impl->config_, result);
  auto waiters
    = co_await finish_lookup(impl->state_, address, pending, result, ttl);
  notify_waiters(std::move(waiters));
  co_return result;
}

} // namespace

struct ForwardDnsResolver::Impl {
  using State = LookupState<std::string, ForwardDnsResult>;

  explicit Impl(ForwardDnsConfig config,
                std::function<Task<ForwardDnsResult>(std::string)> query)
    : config_{normalize_config(std::move(config))},
      permits_{config_.max_in_flight},
      channel_{config_},
      state_{State{config_.max_entries}},
      lookup_executor_{1},
      executor_{folly::getKeepAliveToken(lookup_executor_)},
      query_{std::move(query)} {
    if (not query_) {
      query_ = [this](std::string hostname) -> Task<ForwardDnsResult> {
        co_return co_await forward_query(channel_, std::move(hostname));
      };
    }
  }

  ~Impl() {
    folly::coro::blockingWait(
      folly::coro::co_withCancellation({}, scope_.joinAsync()));
  }

  ForwardDnsConfig config_;
  Semaphore permits_;
  AresChannel channel_;
  Mutex<State> state_;
  folly::coro::AsyncScope scope_;
  folly::CPUThreadPoolExecutor lookup_executor_;
  folly::Executor::KeepAlive<> executor_;
  std::function<Task<ForwardDnsResult>(std::string)> query_;
};

ForwardDnsResolver::ForwardDnsResolver(ForwardDnsConfig config)
  : ForwardDnsResolver{std::move(config), {}} {
}

ForwardDnsResolver::ForwardDnsResolver(
  ForwardDnsConfig config,
  std::function<Task<ForwardDnsResult>(std::string)> query)
  : impl_{std::in_place, std::move(config), std::move(query)} {
}

ForwardDnsResolver::ForwardDnsResolver(ForwardDnsResolver&&) noexcept = default;

auto ForwardDnsResolver::operator=(ForwardDnsResolver&&) noexcept
  -> ForwardDnsResolver& = default;

ForwardDnsResolver::~ForwardDnsResolver() = default;

auto ForwardDnsResolver::resolve(std::string hostname)
  -> Task<Arc<ForwardDnsResult>> {
  auto pending = Option<Arc<PendingLookup<std::string, ForwardDnsResult>>>{};
  auto waiter = Arc<Notify>{std::in_place};
  auto created = false;
  {
    auto state = co_await impl_->state_.lock();
    if (auto result = lookup_cached(*state, hostname)) {
      co_return Arc<ForwardDnsResult>{std::move(*result)};
    }
    auto it = state->pending.find(hostname);
    if (it == state->pending.end()) {
      auto [inserted_it, inserted] = state->pending.emplace(
        hostname,
        Arc<PendingLookup<std::string, ForwardDnsResult>>{std::in_place});
      TENZIR_ASSERT(inserted);
      it = inserted_it;
      created = true;
    }
    pending = it->second;
    (*pending)->waiters.push_back(waiter);
  }
  TENZIR_ASSERT(pending);
  if (created) {
    spawn_lookup(*impl_,
                 run_forward_lookup(impl_.operator->(), hostname, *pending));
  }
  auto waiter_result = co_await folly::coro::co_awaitTry(waiter->wait());
  if (waiter_result.hasException()) {
    co_await folly::coro::co_withCancellation(
      folly::CancellationToken{},
      remove_waiter(impl_->state_, hostname, *pending, waiter));
    co_yield folly::coro::co_error(std::move(waiter_result).exception());
  }
  TENZIR_ASSERT((*pending)->result);
  co_return Arc<ForwardDnsResult>{*(*pending)->result};
}

auto ForwardDnsResolver::cached(std::string_view hostname)
  -> Task<Option<ForwardDnsResult>> {
  auto state = co_await impl_->state_.lock();
  co_return lookup_cached(*state, std::string{hostname});
}

auto ForwardDnsResolver::startup_error() const -> Option<DnsError> {
  if (impl_->channel_.valid()) {
    return None{};
  }
  return make_dns_error(impl_->channel_.status());
}

struct ReverseDnsResolver::Impl {
  using State = LookupState<ip, ReverseDnsResult>;

  explicit Impl(ReverseDnsConfig config,
                std::function<Task<ReverseDnsResult>(ip)> query)
    : config_{normalize_config(std::move(config))},
      permits_{config_.max_in_flight},
      channel_{config_},
      state_{State{config_.max_entries}},
      lookup_executor_{1},
      executor_{folly::getKeepAliveToken(lookup_executor_)},
      query_{std::move(query)} {
    if (not query_) {
      query_ = [this](ip address) -> Task<ReverseDnsResult> {
        co_return co_await reverse_query(channel_, address);
      };
    }
  }

  ~Impl() {
    folly::coro::blockingWait(
      folly::coro::co_withCancellation({}, scope_.joinAsync()));
  }

  ReverseDnsConfig config_;
  Semaphore permits_;
  AresChannel channel_;
  Mutex<State> state_;
  folly::coro::AsyncScope scope_;
  folly::CPUThreadPoolExecutor lookup_executor_;
  folly::Executor::KeepAlive<> executor_;
  std::function<Task<ReverseDnsResult>(ip)> query_;
};

ReverseDnsResolver::ReverseDnsResolver(ReverseDnsConfig config)
  : ReverseDnsResolver{std::move(config), {}} {
}

ReverseDnsResolver::ReverseDnsResolver(
  ReverseDnsConfig config, std::function<Task<ReverseDnsResult>(ip)> query)
  : impl_{std::in_place, std::move(config), std::move(query)} {
}

ReverseDnsResolver::ReverseDnsResolver(ReverseDnsResolver&&) noexcept = default;

auto ReverseDnsResolver::operator=(ReverseDnsResolver&&) noexcept
  -> ReverseDnsResolver& = default;

ReverseDnsResolver::~ReverseDnsResolver() = default;

auto ReverseDnsResolver::resolve(ip address) -> Task<Arc<ReverseDnsResult>> {
  auto pending = Option<Arc<PendingLookup<ip, ReverseDnsResult>>>{};
  auto waiter = Arc<Notify>{std::in_place};
  auto created = false;
  {
    auto state = co_await impl_->state_.lock();
    if (auto result = lookup_cached(*state, address)) {
      co_return Arc<ReverseDnsResult>{std::move(*result)};
    }
    auto it = state->pending.find(address);
    if (it == state->pending.end()) {
      auto [inserted_it, inserted] = state->pending.emplace(
        address, Arc<PendingLookup<ip, ReverseDnsResult>>{
                   std::in_place,
                 });
      TENZIR_ASSERT(inserted);
      it = inserted_it;
      created = true;
    }
    pending = it->second;
    (*pending)->waiters.push_back(waiter);
  }
  TENZIR_ASSERT(pending);
  if (created) {
    spawn_lookup(*impl_,
                 run_reverse_lookup(impl_.operator->(), address, *pending));
  }
  auto waiter_result = co_await folly::coro::co_awaitTry(waiter->wait());
  if (waiter_result.hasException()) {
    co_await folly::coro::co_withCancellation(
      folly::CancellationToken{},
      remove_waiter(impl_->state_, address, *pending, waiter));
    co_yield folly::coro::co_error(std::move(waiter_result).exception());
  }
  TENZIR_ASSERT((*pending)->result);
  co_return Arc<ReverseDnsResult>{*(*pending)->result};
}

auto ReverseDnsResolver::cached(ip address) -> Task<Option<ReverseDnsResult>> {
  auto state = co_await impl_->state_.lock();
  co_return lookup_cached(*state, address);
}

auto ReverseDnsResolver::startup_error() const -> Option<DnsError> {
  if (impl_->channel_.valid()) {
    return None{};
  }
  return make_dns_error(impl_->channel_.status());
}

} // namespace tenzir
