//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <pcap/pcap.h>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::nic {

namespace {

auto make_nics(diagnostic_handler& dh) -> std::optional<table_slice> {
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
    return std::nullopt;
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
    return std::nullopt;
  }
  return builder.finish_assert_one_slice();
}

class load_plugin : public virtual operator_plugin2<nic_loader> {
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> {
    // FIXME: Arg parser doesn't support uint32_t
    auto snaplen = std::optional<located<uint64_t>>{};
    auto args = loader_args{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("iface", args.iface);
    parser.named("snaplen", snaplen);
    parser.named("emit_file_headers", args.emit_file_headers);
    TRY(parser.parse(inv, ctx));
    if (snaplen) {
      args.snaplen
        = {detail::narrow<uint32_t>(snaplen->inner), snaplen->source};
    }
    return std::make_unique<nic_loader>(std::move(args));
  }
};

struct NicsArgs {
  // No arguments.
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

class tql2_plugin final : public operator_plugin2<nics_operator>,
                          public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.nics";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<nics_operator>();
  }

  auto describe() const -> Description override {
    auto d = Describer<NicsArgs, Nics>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::load_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::tql2_plugin)
