//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <pcap/pcap.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::nic {

namespace {

auto make_nics(diagnostic_handler& dh) -> Option<table_slice> {
  auto err = std::array<char, PCAP_ERRBUF_SIZE>{};
  pcap_if_t* devices = nullptr;
  auto result = pcap_findalldevs(&devices, err.data());
  auto deleter = [](pcap_if_t* ptr) {
    if (ptr != nullptr) {
      pcap_freealldevs(ptr);
    }
  };
  auto interfaces
    = std::unique_ptr<pcap_if_t, decltype(deleter)>{devices, deleter};
  if (result == PCAP_ERROR) {
    diagnostic::error("failed to enumerate NICs")
      .hint("{}", std::string_view{err.data()})
      .hint("pcap_findalldevs")
      .emit(dh);
    return None{};
  }
  TENZIR_ASSERT(result == 0);
  auto builder = series_builder{type{
    "tenzir.nic",
    record_type{
      {"name", string_type{}},
      {"description", string_type{}},
      {"addresses", list_type{ip_type{}}},
      {"loopback", bool_type{}},
      {"up", bool_type{}},
      {"running", bool_type{}},
      {"wireless", bool_type{}},
      {"status",
       record_type{
         {"unknown", bool_type{}},
         {"connected", bool_type{}},
         {"disconnected", bool_type{}},
         {"not_applicable", bool_type{}},
       }},
    },
  }};
  for (auto* ptr = interfaces.get(); ptr != nullptr; ptr = ptr->next) {
    auto event = builder.record();
    event.field("name", std::string_view{ptr->name});
    if (ptr->description) {
      event.field("description", std::string_view{ptr->description});
    }
    auto addrs = list{};
    for (auto* addr = ptr->addresses; addr != nullptr; addr = addr->next) {
      if (addr->addr == nullptr) {
        continue;
      }
      if (auto x = to<ip>(detail::to_string(addr->addr))) {
        addrs.emplace_back(*x);
      }
    }
    event.field("addresses", addrs);
    auto is_set = [ptr](uint32_t x) {
      return (ptr->flags & x) == x;
    };
    auto is_status = [ptr](uint32_t x) {
      return (ptr->flags & PCAP_IF_CONNECTION_STATUS) == x;
    };
    event.field("loopback", is_set(PCAP_IF_LOOPBACK));
    event.field("up", is_set(PCAP_IF_UP));
    event.field("running", is_set(PCAP_IF_RUNNING));
    event.field("wireless", is_set(PCAP_IF_WIRELESS));
    auto status = event.field("status").record();
    status.field("unknown", is_status(PCAP_IF_CONNECTION_STATUS_UNKNOWN));
    status.field("connected", is_status(PCAP_IF_CONNECTION_STATUS_CONNECTED));
    status.field("disconnected",
                 is_status(PCAP_IF_CONNECTION_STATUS_DISCONNECTED));
    status.field("not_applicable",
                 is_status(PCAP_IF_CONNECTION_STATUS_NOT_APPLICABLE));
  }
  if (builder.length() == 0) {
    return None{};
  }
  return builder.finish_assert_one_slice();
}

struct NicsArgs {
  // No arguments.
};

struct NicsListing {
  ~NicsListing() {
    if (devices) {
      pcap_freealldevs(devices);
    }
  }

  NicsListing() = default;
  NicsListing(const NicsListing&) = delete;
  NicsListing(NicsListing&& other) noexcept
    : devices{std::exchange(other.devices, nullptr)},
      error{other.error},
      result{other.result} {
  }
  auto operator=(const NicsListing&) -> NicsListing& = delete;
  auto operator=(NicsListing&& other) noexcept -> NicsListing& {
    std::swap(devices, other.devices);
    error = other.error;
    result = other.result;
    return *this;
  }

  pcap_if_t* devices = nullptr;
  std::array<char, PCAP_ERRBUF_SIZE> error = {};
  int result = PCAP_ERROR;
};

class Nics final : public Operator<void, table_slice> {
public:
  explicit Nics(NicsArgs /*args*/) {
  }

  auto start(OpCtx&) -> Task<void> override {
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    if (auto output = make_nics(ctx.dh())) {
      co_await push(std::move(*output));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class NicsEvents final : public Operator<void, nova::Events> {
public:
  explicit NicsEvents(NicsArgs /*args*/) {
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await spawn_blocking([] {
      auto listing = NicsListing{};
      listing.result = pcap_findalldevs(&listing.devices, listing.error.data());
      return listing;
    });
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    auto interfaces = std::move(result).as<NicsListing>();
    if (interfaces.result == PCAP_ERROR) {
      diagnostic::error("failed to enumerate NICs")
        .hint("{}", std::string_view{interfaces.error.data()})
        .hint("pcap_findalldevs")
        .emit(ctx.dh());
      done_ = true;
      co_return;
    }
    auto builder = nova::ArrayBuilder<nova::Record>{};
    for (auto* ptr = interfaces.devices; ptr != nullptr; ptr = ptr->next) {
      auto event = builder.record();
      event.field("name").data(std::string_view{ptr->name});
      if (ptr->description) {
        event.field("description").data(std::string_view{ptr->description});
      } else {
        event.field("description").null();
      }
      auto addresses = event.field("addresses").list();
      for (auto* addr = ptr->addresses; addr != nullptr; addr = addr->next) {
        if (addr->addr == nullptr) {
          continue;
        }
        if (auto ip = to<tenzir::ip>(detail::to_string(addr->addr))) {
          addresses.data(*ip);
        }
      }
      auto is_set = [ptr](uint32_t flag) {
        return (ptr->flags & flag) == flag;
      };
      auto is_status = [ptr](uint32_t status) {
        return (ptr->flags & PCAP_IF_CONNECTION_STATUS) == status;
      };
      event.field("loopback").data(is_set(PCAP_IF_LOOPBACK));
      event.field("up").data(is_set(PCAP_IF_UP));
      event.field("running").data(is_set(PCAP_IF_RUNNING));
      event.field("wireless").data(is_set(PCAP_IF_WIRELESS));
      auto status = event.field("status").record();
      status.field("unknown").data(
        is_status(PCAP_IF_CONNECTION_STATUS_UNKNOWN));
      status.field("connected")
        .data(is_status(PCAP_IF_CONNECTION_STATUS_CONNECTED));
      status.field("disconnected")
        .data(is_status(PCAP_IF_CONNECTION_STATUS_DISCONNECTED));
      status.field("not_applicable")
        .data(is_status(PCAP_IF_CONNECTION_STATUS_NOT_APPLICABLE));
    }
    auto output = builder.finish();
    auto rows = output.length();
    if (rows > 0) {
      co_await push(
        nova::Events{std::move(output), nova::storage::BitMap{rows, true},
                     nova::Events::Meta::make_empty(rows, "tenzir.nic")});
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }
  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};

class tql2_plugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "nics";
  }

  auto describe() const -> Description override {
    auto d = Describer<NicsArgs, Nics, NicsEvents>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::tql2_plugin)
