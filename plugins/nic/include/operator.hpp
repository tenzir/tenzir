//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant_traits.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <pcap/pcap.h>

#include <chrono>
#include <string_view>

using namespace std::chrono_literals;

namespace tenzir::plugins::nic {

namespace {

struct loader_args {
  located<std::string> iface;
  std::optional<located<uint32_t>> snaplen;
  std::optional<location> emit_file_headers;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("iface", x.iface), f.field("snaplen", x.snaplen),
              f.field("emit_file_headers", x.emit_file_headers));
  }
};

auto make_file_header(int snaplen, int linktype) -> pcap::file_header {
  return {
    // Timestamps have microsecond resolution when using pcap_open_live(). If we
    // want nanosecond resolution, we must stop using pcap_open_live() and
    // replace it with pcap_create() and pcap_activate(). See
    // https://stackoverflow.com/q/28310922/1170277 for details.
    .magic_number = pcap::magic_number_1,
    .major_version = 2,
    .minor_version = 4,
    .reserved1 = 0,
    .reserved2 = 0,
    .snaplen = detail::narrow_cast<uint32_t>(snaplen),
    .linktype = detail::narrow_cast<uint32_t>(linktype),
  };
};

class nic_loader final : public crtp_operator<nic_loader> {
public:
  nic_loader() = default;

  explicit nic_loader(loader_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // We yield here, because otherwise the error is terminal to a node on
    // startup.
    co_yield {};
    TENZIR_ASSERT(! args_.iface.inner.empty());
    auto snaplen = args_.snaplen ? args_.snaplen->inner : 262'144;
    TENZIR_DEBUG("capturing from {} with snaplen of {}", args_.iface.inner,
                 snaplen);

    auto put_iface_in_promiscuous_mode = 1;
    // The packet buffer timeout functions much like a read timeout: It
    // describes the number of milliseconds to wait at most until returning
    // from pcap_next_ex.
    auto packet_buffer_timeout_ms
      = std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
          defaults::import::read_timeout)
          .count();
    auto error = std::array<char, PCAP_ERRBUF_SIZE>{};
    auto* ptr = pcap_open_live(args_.iface.inner.c_str(),
                               detail::narrow_cast<int>(snaplen),
                               put_iface_in_promiscuous_mode,
                               packet_buffer_timeout_ms, error.data());
    if (! ptr) {
      diagnostic::error("failed to open interface: {}",
                        std::string_view{error.data()})
        .note("from `nic`")
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto pcap = std::shared_ptr<pcap_t>{ptr, [](pcap_t* p) {
                                          pcap_close(p);
                                        }};
    auto linktype = pcap_datalink(pcap.get());
    TENZIR_ASSERT(linktype != PCAP_ERROR_NOT_ACTIVATED);
    auto num_packets = size_t{0};
    auto num_buffered_packets = size_t{0};
    auto buffer = std::vector<std::byte>{};
    auto last_finish = std::chrono::steady_clock::now();
    while (true) {
      const auto now = std::chrono::steady_clock::now();
      if (num_buffered_packets >= defaults::import::table_slice_size
          or last_finish + defaults::import::batch_timeout < now) {
        TENZIR_DEBUG("yielding buffer after {} with {} packets ({} bytes)",
                     tenzir::data{now - last_finish}, num_buffered_packets,
                     buffer.size());
        last_finish = now;
        co_yield chunk::make(std::exchange(buffer, {}));
        // Reduce number of small allocations based on what we've seen
        // previously.
        auto avg_packet_size = buffer.size() / num_buffered_packets;
        buffer.reserve(avg_packet_size * defaults::import::table_slice_size);
        num_buffered_packets = 0;
      }
      const u_char* pkt_data = nullptr;
      pcap_pkthdr* pkt_hdr = nullptr;
      auto r = ::pcap_next_ex(pcap.get(), &pkt_hdr, &pkt_data);
      if (r == 0) {
        // Timeout
        if (last_finish != now) {
          co_yield {};
        }
        continue;
      }
      if (r == -2) {
        TENZIR_DEBUG("reached end of trace with {} packets", num_packets);
        break;
      }
      if (r == PCAP_ERROR) {
        auto error = std::string_view{::pcap_geterr(pcap.get())};
        diagnostic::error("failed to get next packet: {}", error)
          .note("from `nic`")
          .emit(ctrl.diagnostics());
        break;
      }
      // Emit a PCAP file header, either with every chunk or once initially as
      // separate chunk. This results in a packet stream that looks like a
      // standard PCAP file downstream, allowing users to use the `pcap`
      // format to parse the byte stream.
      if (args_.emit_file_headers) {
        if (buffer.empty()) {
          auto header = make_file_header(snaplen, linktype);
          auto bytes = as_bytes(header);
          buffer.insert(buffer.end(), bytes.begin(), bytes.end());
        }
      } else if (num_packets == 0) {
        auto linktype = pcap_datalink(pcap.get());
        TENZIR_ASSERT(linktype != PCAP_ERROR_NOT_ACTIVATED);
        auto header = make_file_header(snaplen, linktype);
        co_yield chunk::copy(as_bytes(header));
      }
      auto header = pcap::packet_header{
        .timestamp = detail::narrow_cast<uint32_t>(pkt_hdr->ts.tv_sec),
        .timestamp_fraction
        = detail::narrow_cast<uint32_t>(pkt_hdr->ts.tv_usec),
        .captured_packet_length = pkt_hdr->caplen,
        .original_packet_length = pkt_hdr->len,
      };
      auto data = std::span<const std::byte>{
        reinterpret_cast<const std::byte*>(pkt_data),
        static_cast<size_t>(pkt_hdr->caplen)};
      auto buffer_size = buffer.size();
      buffer.resize(buffer_size + sizeof(pcap::packet_header) + data.size());
      std::memcpy(buffer.data() + buffer_size, &header, sizeof(header));
      std::memcpy(buffer.data() + buffer_size + sizeof(pcap::packet_header),
                  data.data(), data.size());
      ++num_buffered_packets;
      ++num_packets;
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "load_nic";
  }

  friend auto inspect(auto& f, nic_loader& x) -> bool {
    return f.object(x)
      .pretty_name("nic_loader")
      .fields(f.field("args", x.args_), f.field("config", x.config_));
  }

private:
  loader_args args_;
  record config_;
};

class nics_operator final : public crtp_operator<nics_operator> {
public:
  nics_operator() = default;

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto err = std::array<char, PCAP_ERRBUF_SIZE>{};
    pcap_if_t* devices = nullptr;
    auto result = pcap_findalldevs(&devices, err.data());
    auto deleter = [](pcap_if_t* ptr) {
      pcap_freealldevs(ptr);
    };
    auto iface = std::shared_ptr<pcap_if_t>{devices, deleter};
    if (result == PCAP_ERROR) {
      diagnostic::error("failed to enumerate NICs")
        .hint("{}", std::string_view{err.data(), err.size()})
        .hint("pcap_findalldevs")
        .emit(ctrl.diagnostics());
      co_return;
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
    for (const auto* ptr = iface.get(); ptr != nullptr; ptr = ptr->next) {
      auto event = builder.record();
      event.field("name", std::string_view{ptr->name});
      if (ptr->description) {
        event.field("description", std::string_view{ptr->description});
      }
      auto addrs = list{};
      for (auto* addr = ptr->addresses; addr != nullptr; addr = addr->next) {
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
    co_yield builder.finish_assert_one_slice();
  }

  auto name() const -> std::string override {
    return "nics";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, nics_operator& x) -> bool {
    return f.object(x).pretty_name("tenzir.plugins.nics.nics_operator").fields();
  }
};
} // namespace
} // namespace tenzir::plugins::nic
