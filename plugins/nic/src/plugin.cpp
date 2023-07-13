//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/chunk.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/data.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pcap.hpp>
#include <vast/plugin.hpp>

#include <pcap/pcap.h>

#include <chrono>

using namespace std::chrono_literals;

namespace vast::plugins::nic {

namespace {

struct loader_args {
  located<std::string> iface;
  std::optional<located<uint32_t>> snaplen;

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("loader_args")
      .fields(f.field("iface", x.iface), f.field("snaplen", x.snaplen));
  }
};

class nic_loader final : public plugin_loader {
public:
  nic_loader() = default;

  explicit nic_loader(loader_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    TENZIR_ASSERT(!args_.iface.inner.empty());
    auto snaplen = args_.snaplen ? args_.snaplen->inner : 262'144;
    TENZIR_DEBUG("capturing from {} with snaplen of {}", args_.iface.inner,
                 snaplen);
    auto make = [](auto& ctrl, auto iface,
                   auto snaplen) mutable -> generator<chunk_ptr> {
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
                       vast::data{now - last_finish}, num_buffered_packets,
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
        // Emit a PCAP file header before the first packet. This results in a
        // packet stream that looks like a standard PCAP file downstream,
        // allowing users to use the `pcap` format to parse the byte stream.
        if (num_packets == 0) {
          auto linktype = pcap_datalink(pcap.get());
          TENZIR_ASSERT(linktype != PCAP_ERROR_NOT_ACTIVATED);
          auto header = pcap::file_header{
#ifdef PCAP_TSTAMP_PRECISION_NANO
            .magic_number = pcap::magic_number_2,
#else
            .magic_number = pcap::magic_number_1,
#endif
            .major_version = 2,
            .minor_version = 4,
            .reserved1 = 0,
            .reserved2 = 0,
            .snaplen = snaplen,
            .linktype = detail::narrow_cast<uint32_t>(linktype),
          };
          const auto* ptr = reinterpret_cast<const std::byte*>(&header);
          auto bytes = std::span<const std::byte>{ptr, sizeof(header)};
          co_yield chunk::copy(bytes);
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
    return make(ctrl, args_.iface.inner, snaplen);
  }

  auto to_string() const -> std::string override {
    auto result = name();
    result += fmt::format(" {}", args_.iface);
    if (args_.snaplen)
      result += fmt::format(" --snaplen {}", args_.snaplen->inner);
    return result;
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

class plugin final : public loader_plugin<nic_loader> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/connectors/{}", name())};
    auto args = loader_args{};
    parser.add(args.iface, "<iface>");
    parser.add("-s,--snaplen", args.snaplen, "<count>");
    parser.parse(p);
    return std::make_unique<nic_loader>(std::move(args));
  }

  auto name() const -> std::string override {
    return "nic";
  }

private:
  record config_;
};

} // namespace

} // namespace vast::plugins::nic

TENZIR_REGISTER_PLUGIN(vast::plugins::nic::plugin)
