//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/dns.hpp"

#include "tenzir/async/mutex.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/socket.hpp"

#include <ares.h>
#include <folly/coro/Baton.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <list>
#include <limits>
#include <memory>
#include <netdb.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
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
  config.literal_ttl = std::max(config.literal_ttl, std::chrono::seconds::zero());
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
    options.tries = detail::narrow<int>(std::min<size_t>(
      config.tries, size_t{std::numeric_limits<int>::max()}));
    options.evsys = ARES_EVSYS_DEFAULT;
    auto optmask = int{ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES
                       | ARES_OPT_EVENT_THREAD};
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

auto make_forward_failure(int status) -> ForwardDnsResult {
  return {
    .status = ForwardDnsStatus::failed,
    .error = std::string{ares_strerror(status)},
  };
}

auto make_reverse_failure(int status) -> ReverseDnsResult {
  return {
    .status = ReverseDnsStatus::failed,
    .error = std::string{ares_strerror(status)},
  };
}

auto make_forward_literal_result(ip address, std::string canonical_name,
                                 DnsResolverConfig const& config)
  -> ForwardDnsResult {
  auto result = ForwardDnsResult{
    .status = ForwardDnsStatus::resolved,
  };
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
    return make_forward_literal_result(*parsed, std::string{hostname}, config);
  }
  if (hostname == "localhost") {
    auto result = ForwardDnsResult{
      .status = ForwardDnsStatus::resolved,
      .canonical_name = std::string{"localhost"},
    };
    auto loopback_v4 = std::array<uint8_t, 4>{127, 0, 0, 1};
    result.answers.push_back(ForwardDnsAnswer{
      .address = ip::v4(std::span{loopback_v4}),
      .type = "A",
      .ttl = config.literal_ttl,
    });
    auto loopback_v6 = std::array<uint8_t, 16>{};
    loopback_v6.back() = 1;
    result.answers.push_back(ForwardDnsAnswer{
      .address = ip::v6(std::span{loopback_v6}),
      .type = "AAAA",
      .ttl = config.literal_ttl,
    });
    return result;
  }
  return None{};
}

auto local_reverse_result(ip address, DnsResolverConfig const&)
  -> Option<ReverseDnsResult> {
  if (address.is_loopback()) {
    return ReverseDnsResult{
      .status = ReverseDnsStatus::resolved,
      .hostname = std::string{"localhost"},
    };
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

auto make_sockaddr(ip address, sockaddr_storage& storage, ares_socklen_t& length)
  -> void {
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
  folly::coro::Baton baton;
  ForwardDnsResult result;
  std::string hostname;
};

struct ReverseQueryState {
  folly::coro::Baton baton;
  ReverseDnsResult result;
  sockaddr_storage storage{};
  ares_socklen_t length = 0;
};

auto forward_query_callback(void* opaque, int status, int /*timeouts*/,
                            ares_addrinfo* info) -> void {
  auto holder = std::unique_ptr<std::shared_ptr<ForwardQueryState>>{
    static_cast<std::shared_ptr<ForwardQueryState>*>(opaque),
  };
  auto state = std::move(*holder);
  if (status != ARES_SUCCESS) {
    if (is_not_found_status(status)) {
      state->result.status = ForwardDnsStatus::not_found;
    } else {
      state->result = make_forward_failure(status);
    }
    if (info) {
      ares_freeaddrinfo(info);
    }
    state->baton.post();
    return;
  }
  TENZIR_ASSERT(info);
  if (info->name) {
    state->result.canonical_name = std::string{info->name};
  } else if (info->cnames and info->cnames->name) {
    state->result.canonical_name = std::string{info->cnames->name};
  }
  for (auto* node = info->nodes; node != nullptr; node = node->ai_next) {
    auto address = sockaddr_to_ip(node->ai_addr);
    if (not address) {
      continue;
    }
    state->result.answers.push_back(ForwardDnsAnswer{
      .address = std::move(*address),
      .type = node->ai_family == AF_INET ? "A" : "AAAA",
      .ttl = std::chrono::seconds{std::max(node->ai_ttl, 0)},
    });
  }
  ares_freeaddrinfo(info);
  state->result.status = state->result.answers.empty()
                           ? ForwardDnsStatus::not_found
                           : ForwardDnsStatus::resolved;
  state->baton.post();
}

auto reverse_query_callback(void* opaque, int status, int /*timeouts*/,
                            char* node, char* /*service*/) -> void {
  auto holder = std::unique_ptr<std::shared_ptr<ReverseQueryState>>{
    static_cast<std::shared_ptr<ReverseQueryState>*>(opaque),
  };
  auto state = std::move(*holder);
  if (status != ARES_SUCCESS) {
    if (is_not_found_status(status)) {
      state->result.status = ReverseDnsStatus::not_found;
    } else {
      state->result = make_reverse_failure(status);
    }
    state->baton.post();
    return;
  }
  if (node) {
    state->result = ReverseDnsResult{
      .status = ReverseDnsStatus::resolved,
      .hostname = std::string{node},
    };
  } else {
    state->result.status = ReverseDnsStatus::not_found;
  }
  state->baton.post();
}

auto forward_query(AresChannel const& channel, std::string hostname)
  -> Task<ForwardDnsResult> {
  auto state = std::make_shared<ForwardQueryState>();
  state->hostname = std::move(hostname);
  if (not channel.valid()) {
    co_return make_forward_failure(channel.status());
  }
  auto hints = ares_addrinfo_hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_flags = ARES_AI_CANONNAME;
  ares_getaddrinfo(channel.get(), state->hostname.c_str(), nullptr, &hints,
                   forward_query_callback,
                   new std::shared_ptr<ForwardQueryState>{state});
  co_await state->baton;
  co_return state->result;
}

auto reverse_query(AresChannel const& channel, ip address)
  -> Task<ReverseDnsResult> {
  auto state = std::make_shared<ReverseQueryState>();
  make_sockaddr(address, state->storage, state->length);
  if (not channel.valid()) {
    co_return make_reverse_failure(channel.status());
  }
  ares_getnameinfo(channel.get(),
                   reinterpret_cast<sockaddr const*>(&state->storage),
                   state->length, NI_NAMEREQD, reverse_query_callback,
                   new std::shared_ptr<ReverseQueryState>{state});
  co_await state->baton;
  co_return state->result;
}

template <class Key, class Result>
struct PendingLookup {
  folly::coro::Baton baton;
  Option<Result> result = None{};
};

template <class Key, class Result>
struct LookupState {
  using lru_type = std::list<Key>;

  struct CacheEntry {
    Result result;
    Clock::time_point expires_at;
    typename lru_type::iterator lru_position;
  };

  using cache_map = std::unordered_map<Key, CacheEntry>;

  lru_type lru;
  cache_map cache;
  std::unordered_map<Key, std::shared_ptr<PendingLookup<Key, Result>>> pending;
};

template <class Key, class Result>
auto touch(LookupState<Key, Result>& state,
           typename LookupState<Key, Result>::cache_map::iterator it) -> void {
  state.lru.splice(state.lru.end(), state.lru, it->second.lru_position);
}

template <class Key, class Result>
auto erase(LookupState<Key, Result>& state,
           typename LookupState<Key, Result>::cache_map::iterator it) -> void {
  state.lru.erase(it->second.lru_position);
  state.cache.erase(it);
}

template <class Key, class Result>
auto evict(LookupState<Key, Result>& state, size_t max_entries) -> void {
  while (state.cache.size() > max_entries) {
    TENZIR_ASSERT(not state.lru.empty());
    auto oldest = state.lru.begin();
    auto it = state.cache.find(*oldest);
    TENZIR_ASSERT(it != state.cache.end());
    erase(state, it);
  }
}

template <class Key, class Result>
auto lookup_cached(LookupState<Key, Result>& state, Key const& key)
  -> Option<Result> {
  auto now = Clock::now();
  auto it = state.cache.find(key);
  if (it == state.cache.end()) {
    return None{};
  }
  if (it->second.expires_at <= now) {
    erase(state, it);
    return None{};
  }
  auto result = it->second.result;
  touch(state, it);
  return result;
}

template <class Key, class Result>
auto insert_cached(LookupState<Key, Result>& state, Key const& key,
                   Result const& result, DnsDuration ttl, size_t max_entries)
  -> void {
  if (ttl <= DnsDuration::zero()) {
    return;
  }
  auto expires_at = Clock::now() + ttl;
  if (auto it = state.cache.find(key); it != state.cache.end()) {
    it->second.result = result;
    it->second.expires_at = expires_at;
    touch(state, it);
    return;
  }
  state.lru.push_back(key);
  auto lru_position = std::prev(state.lru.end());
  state.cache.emplace(*lru_position,
                      typename LookupState<Key, Result>::CacheEntry{
                        .result = result,
                        .expires_at = expires_at,
                        .lru_position = lru_position,
                      });
  evict(state, max_entries);
}

auto ttl_for(DnsResolverConfig const& config, ForwardDnsResult const& result)
  -> DnsDuration {
  switch (result.status) {
    case ForwardDnsStatus::resolved: {
      auto ttl = std::chrono::seconds::max();
      for (auto const& answer : result.answers) {
        if (answer.ttl > std::chrono::seconds::zero()) {
          ttl = std::min(ttl, answer.ttl);
        }
      }
      return ttl == std::chrono::seconds::max() ? config.positive_ttl : ttl;
    }
    case ForwardDnsStatus::not_found:
    case ForwardDnsStatus::failed:
      return config.negative_ttl;
  }
  TENZIR_UNREACHABLE();
}

auto ttl_for(DnsResolverConfig const& config, ReverseDnsResult const& result)
  -> DnsDuration {
  switch (result.status) {
    case ReverseDnsStatus::resolved:
      return config.positive_ttl;
    case ReverseDnsStatus::not_found:
    case ReverseDnsStatus::failed:
      return config.negative_ttl;
  }
  TENZIR_UNREACHABLE();
}

template <class Key, class Result>
auto finish_lookup(Mutex<LookupState<Key, Result>>& state_mutex, Key const& key,
                   Result const& result, DnsDuration ttl, size_t max_entries)
  -> Task<std::shared_ptr<PendingLookup<Key, Result>>> {
  auto state = co_await state_mutex.lock();
  insert_cached(*state, key, result, ttl, max_entries);
  auto it = state->pending.find(key);
  TENZIR_ASSERT(it != state->pending.end());
  auto pending = std::move(it->second);
  pending->result = result;
  state->pending.erase(it);
  co_return pending;
}

} // namespace

struct ForwardDnsResolver::Impl {
  using State = LookupState<std::string, ForwardDnsResult>;

  explicit Impl(ForwardDnsConfig config)
    : config_{normalize_config(std::move(config))},
      permits_{config_.max_in_flight},
      channel_{config_},
      state_{State{}} {
  }

  ForwardDnsConfig config_;
  Semaphore permits_;
  AresChannel channel_;
  Mutex<State> state_;
};

ForwardDnsResolver::ForwardDnsResolver(ForwardDnsConfig config)
  : impl_{std::in_place, std::move(config)} {
}

ForwardDnsResolver::ForwardDnsResolver(ForwardDnsResolver&&) noexcept = default;

auto ForwardDnsResolver::operator=(ForwardDnsResolver&&) noexcept
  -> ForwardDnsResolver& = default;

ForwardDnsResolver::~ForwardDnsResolver() = default;

auto ForwardDnsResolver::resolve(std::string hostname)
  -> Task<ForwardDnsResult> {
  auto pending = std::shared_ptr<PendingLookup<std::string, ForwardDnsResult>>{};
  auto created = false;
  {
    auto state = co_await impl_->state_.lock();
    if (auto result = lookup_cached(*state, hostname)) {
      co_return std::move(*result);
    }
    auto [it, inserted] = state->pending.emplace(hostname, nullptr);
    if (inserted) {
      it->second
        = std::make_shared<PendingLookup<std::string, ForwardDnsResult>>();
      created = true;
    }
    pending = it->second;
  }
  if (not created) {
    co_await pending->baton;
    TENZIR_ASSERT(pending->result);
    co_return *pending->result;
  }
  auto result = ForwardDnsResult{};
  if (auto local = local_forward_result(hostname, impl_->config_)) {
    result = std::move(*local);
  } else {
    auto permit = co_await impl_->permits_.acquire();
    result = co_await forward_query(impl_->channel_, hostname);
  }
  auto ttl = ttl_for(impl_->config_, result);
  pending = co_await finish_lookup(impl_->state_, hostname, result, ttl,
                                   impl_->config_.max_entries);
  pending->baton.post();
  co_return result;
}

auto ForwardDnsResolver::cached(std::string_view hostname)
  -> Task<Option<ForwardDnsResult>> {
  auto state = co_await impl_->state_.lock();
  co_return lookup_cached(*state, std::string{hostname});
}

struct ReverseDnsResolver::Impl {
  using State = LookupState<ip, ReverseDnsResult>;

  explicit Impl(ReverseDnsConfig config)
    : config_{normalize_config(std::move(config))},
      permits_{config_.max_in_flight},
      channel_{config_},
      state_{State{}} {
  }

  ReverseDnsConfig config_;
  Semaphore permits_;
  AresChannel channel_;
  Mutex<State> state_;
};

ReverseDnsResolver::ReverseDnsResolver(ReverseDnsConfig config)
  : impl_{std::in_place, std::move(config)} {
}

ReverseDnsResolver::ReverseDnsResolver(ReverseDnsResolver&&) noexcept = default;

auto ReverseDnsResolver::operator=(ReverseDnsResolver&&) noexcept
  -> ReverseDnsResolver& = default;

ReverseDnsResolver::~ReverseDnsResolver() = default;

auto ReverseDnsResolver::resolve(ip address) -> Task<ReverseDnsResult> {
  auto pending = std::shared_ptr<PendingLookup<ip, ReverseDnsResult>>{};
  auto created = false;
  {
    auto state = co_await impl_->state_.lock();
    if (auto result = lookup_cached(*state, address)) {
      co_return std::move(*result);
    }
    auto [it, inserted] = state->pending.emplace(address, nullptr);
    if (inserted) {
      it->second = std::make_shared<PendingLookup<ip, ReverseDnsResult>>();
      created = true;
    }
    pending = it->second;
  }
  if (not created) {
    co_await pending->baton;
    TENZIR_ASSERT(pending->result);
    co_return *pending->result;
  }
  auto result = ReverseDnsResult{};
  auto ttl = DnsDuration{};
  if (auto local = local_reverse_result(address, impl_->config_)) {
    result = std::move(*local);
    ttl = impl_->config_.literal_ttl;
  } else {
    auto permit = co_await impl_->permits_.acquire();
    result = co_await reverse_query(impl_->channel_, address);
    ttl = ttl_for(impl_->config_, result);
  }
  pending = co_await finish_lookup(impl_->state_, address, result, ttl,
                                   impl_->config_.max_entries);
  pending->baton.post();
  co_return result;
}

auto ReverseDnsResolver::cached(ip address)
  -> Task<Option<ReverseDnsResult>> {
  auto state = co_await impl_->state_.lock();
  co_return lookup_cached(*state, address);
}

} // namespace tenzir
