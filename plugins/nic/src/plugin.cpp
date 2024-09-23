//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <pcap/pcap.h>

#include <chrono>

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

class nic_loader final : public plugin_loader {
public:
  nic_loader() = default;

  explicit nic_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(exec_ctx ctx) const
    -> std::optional<generator<chunk_ptr>> override {
    TENZIR_ASSERT(!args_.iface.inner.empty());
    auto snaplen = args_.snaplen ? args_.snaplen->inner : 262'144;
    TENZIR_DEBUG("capturing from {} with snaplen of {}", args_.iface.inner,
                 snaplen);
    auto make = [](auto& ctrl, auto iface, auto snaplen,
                   bool emit_file_headers) mutable -> generator<chunk_ptr> {
      auto put_iface_in_promiscuous_mode = 1;
      // The packet buffer timeout functions much like a read timeout: It
      // describes the number of milliseconds to wait at most until returning
      // from pcap_next_ex.
      auto packet_buffer_timeout_ms
        = std::chrono::duration_cast<std::chrono::duration<int, std::milli>>(
            defaults::import::read_timeout)
            .count();
      auto error = std::array<char, PCAP_ERRBUF_SIZE>{};
      auto* ptr
        = pcap_open_live(iface.c_str(), detail::narrow_cast<int>(snaplen),
                         put_iface_in_promiscuous_mode,
                         packet_buffer_timeout_ms, error.data());
      if (!ptr) {
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
      // We yield once initially to signal that the operator successfully
      // started.
      co_yield {};
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
        if (emit_file_headers) {
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
    };
    return make(ctrl, args_.iface.inner, snaplen, !!args_.emit_file_headers);
  }

  auto name() const -> std::string override {
    return "nic";
  }

  auto default_parser() const -> std::string override {
    return "pcap";
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

/// A type that represents an description of a NIC.
auto nic_type() -> type {
  return type{
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
  };
}

class nics_operator final : public crtp_operator<nics_operator> {
public:
  nics_operator() = default;

  auto operator()(exec_ctx ctx) const -> generator<table_slice> {
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
    auto builder = table_slice_builder{nic_type()};
    for (const auto* ptr = iface.get(); ptr != nullptr; ptr = ptr->next) {
      auto okay = builder.add(std::string_view{ptr->name});
      TENZIR_ASSERT(okay);
      if (ptr->description)
        okay = builder.add(std::string_view{ptr->description});
      else
        okay = builder.add(caf::none);
      auto addrs = list{};
      for (auto* addr = ptr->addresses; addr != nullptr; addr = addr->next)
        if (auto x = to<ip>(detail::to_string(addr->addr)))
          addrs.emplace_back(*x);
      okay = builder.add(addrs);
      TENZIR_ASSERT(okay);
      auto is_set = [ptr](uint32_t x) {
        return (ptr->flags & x) == x;
      };
      auto is_status = [ptr](uint32_t x) {
        return (ptr->flags & PCAP_IF_CONNECTION_STATUS) == x;
      };
      okay = builder.add(is_set(PCAP_IF_LOOPBACK));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_set(PCAP_IF_UP));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_set(PCAP_IF_RUNNING));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_set(PCAP_IF_WIRELESS));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_status(PCAP_IF_CONNECTION_STATUS_UNKNOWN));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_status(PCAP_IF_CONNECTION_STATUS_CONNECTED));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_status(PCAP_IF_CONNECTION_STATUS_DISCONNECTED));
      TENZIR_ASSERT(okay);
      okay = builder.add(is_status(PCAP_IF_CONNECTION_STATUS_NOT_APPLICABLE));
      TENZIR_ASSERT(okay);
    }
    co_yield builder.finish();
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

class plugin final : public virtual loader_plugin<nic_loader>,
                     public virtual operator_plugin<nics_operator> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "nic";
  }

  auto operator_name() const -> std::string override {
    return "nics";
  }

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"nics", "https://docs.tenzir.com/"
                                          "operators/nics"};
    parser.parse(p);
    return std::make_unique<nics_operator>();
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.iface, "<iface>");
    parser.add("-s,--snaplen", args.snaplen, "<count>");
    parser.add("-e,--emit-file-headers", args.emit_file_headers);
    parser.parse(p);
    return std::make_unique<nic_loader>(std::move(args));
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::nic

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nic::plugin)
