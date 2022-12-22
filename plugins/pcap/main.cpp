//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/community_id.hpp>
#include <vast/data.hpp>
#include <vast/error.hpp>
#include <vast/ether_type.hpp>
#include <vast/format/reader.hpp>
#include <vast/format/single_layout_reader.hpp>
#include <vast/format/writer.hpp>
#include <vast/frame_type.hpp>
#include <vast/logger.hpp>
#include <vast/module.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <caf/settings.hpp>
#include <netinet/in.h>

#include <optional>
#include <pcap.h>
#include <random>
#include <span>

namespace vast::defaults {

namespace import {

/// Contains settings for the pcap subcommand.
struct pcap {
  /// Number of bytes to keep per event.
  static constexpr size_t cutoff = std::numeric_limits<size_t>::max();

  /// Number of concurrent flows to track.
  static constexpr size_t max_flows = 1'048'576; // 1_Mi

  /// Maximum flow lifetime before eviction.
  static constexpr size_t max_flow_age = 60;

  /// Flow table expiration interval.
  static constexpr size_t flow_expiry = 10;

  /// Inverse factor by which to delay packets. For example, if 5, then for two
  /// packets spaced *t* seconds apart, the source will sleep for *t/5* seconds.
  static constexpr int64_t pseudo_realtime_factor = 0;

  /// If the snapshot length is set to snaplen, and snaplen is less than the
  /// size of a packet that is captured, only the first snaplen bytes of that
  /// packet will be captured and provided as packet data. A snapshot length
  /// of 65535 should be sufficient, on most if not all networks, to capture all
  /// the data available from the packet.
  static constexpr size_t snaplen = 65'535;
};

} // namespace import

namespace export_ {
//
/// Contains settings for the pcap subcommand.
struct pcap {
  /// Flush to disk after that many packets.
  static constexpr size_t flush_interval = 10'000;
};

} // namespace export_

} // namespace vast::defaults

namespace vast::plugins::pcap {

type make_packet_type() {
  // FIXME: once we ship with builtin type aliases, we should reference the
  // port alias type here. Until then, we create the alias manually.
  // See also:
  // - src/format/zeek.cpp
  const auto port_type = type{"port", count_type{}};
  const auto timestamp_type = type{"timestamp", time_type{}};
  return type{
    "pcap.packet",
    record_type{
      {"time", timestamp_type},
      {"src", address_type{}},
      {"dst", address_type{}},
      {"sport", port_type},
      {"dport", port_type},
      {"vlan",
       record_type{
         {"outer", count_type{}},
         {"inner", count_type{}},
       }},
      {"community_id", type{string_type{}, {{"index", "hash"}}}},
      {"payload", type{string_type{}, {{"skip"}}}},
    },
  };
}

struct pcap_close_wrapper {
  void operator()(struct pcap* handle) const noexcept {
    ::pcap_close(handle);
  }
};

struct pcap_dump_close_wrapper {
  void operator()(struct pcap_dumper* handle) const noexcept {
    ::pcap_dump_close(handle);
  }
};

auto to_uint16(std::span<const std::byte, 2> bytes) {
  auto data = bytes.data();
  auto ptr = reinterpret_cast<const uint16_t*>(std::launder(data));
  return detail::to_host_order(*ptr);
}

/// An 802.3 Ethernet frame.
struct frame {
  static std::optional<frame>
  make(std::span<const std::byte> bytes, frame_type type) {
    switch (type) {
      default:
        break;
      case frame_type::ethernet: {
        // Need at least 2 MAC addresses and the 2-byte EtherType.
        constexpr size_t ethernet_header_size = 6 + 6 + 2;
        if (bytes.size() < ethernet_header_size)
          return std::nullopt;
        auto dst = bytes.subspan<0, 6>();
        auto src = bytes.subspan<6, 6>();
        auto result = frame{dst, src};
        auto type = as_ether_type(bytes.subspan<12, 2>());
        switch (type) {
          default:
            result.type = type;
            result.payload = bytes.subspan<ethernet_header_size>();
            break;
          case ether_type::ieee_802_1aq: {
            size_t min_frame_size = 6 + 6 + 4 + 2;
            if (bytes.size() < min_frame_size)
              return std::nullopt;
            result.outer_vid = to_uint16(bytes.subspan<14, 2>());
            *result.outer_vid &= 0x0FFF; // lower 12 bits only
            result.type = as_ether_type(bytes.subspan<16, 2>());
            result.payload = bytes.subspan(min_frame_size);
            // Keep going for QinQ frames (TPID = 0x8100).
            if (result.type == ether_type::ieee_802_1aq) {
              min_frame_size += 4;
              if (bytes.size() < min_frame_size)
                return std::nullopt;
              result.inner_vid = to_uint16(bytes.subspan<18, 2>());
              *result.inner_vid &= 0x0FFF; // lower 12 bits only
              result.type = as_ether_type(bytes.subspan<20, 2>());
              result.payload = bytes.subspan(min_frame_size);
            }
            break;
          }
          case ether_type::ieee_802_1q_db: {
            constexpr size_t min_frame_size = 6 + 6 + 4 + 4 + 2;
            if (bytes.size() < min_frame_size)
              return std::nullopt;
            result.outer_vid = to_uint16(bytes.subspan<14, 2>());
            *result.outer_vid &= 0x0FFF; // lower 12 bits only
            result.inner_vid = to_uint16(bytes.subspan<18, 2>());
            *result.inner_vid &= 0x0FFF; // lower 12 bits only
            result.type = as_ether_type(bytes.subspan<20, 2>());
            result.payload = bytes.subspan<min_frame_size>();
            break;
          }
        }
        return result;
      }
    }
    return std::nullopt;
  }

  frame(std::span<const std::byte, 6> dst, std::span<const std::byte, 6> src)
    : dst{dst}, src{src} {
  }

  std::span<const std::byte, 6> dst;  ///< Destination MAC address
  std::span<const std::byte, 6> src;  ///< Source MAC address
  std::optional<uint16_t> outer_vid;  ///< Outer 802.1Q tag control information
  std::optional<uint16_t> inner_vid;  ///< Outer 802.1Q tag control information
  ether_type type;                    ///< EtherType
  std::span<const std::byte> payload; ///< Payload
};

/// An IP packet.
struct packet {
  static std::optional<packet>
  make(std::span<const std::byte> bytes, ether_type type) {
    packet result;
    switch (type) {
      default:
        break;
      case ether_type::ipv4: {
        constexpr size_t ipv4_header_size = 20;
        if (bytes.size() < ipv4_header_size)
          return std::nullopt;
        size_t header_length = (std::to_integer<uint8_t>(bytes[0]) & 0x0f) * 4;
        if (bytes.size() < header_length)
          return std::nullopt;
        result.src = address::v4(bytes.subspan<12, 4>());
        result.dst = address::v4(bytes.subspan<16, 4>());
        result.type = std::to_integer<uint8_t>(bytes[9]);
        result.payload = bytes.subspan(header_length);
        return result;
      }
      case ether_type::ipv6: {
        constexpr size_t ipv6_header_size = 40;
        if (bytes.size() < ipv6_header_size)
          return std::nullopt;
        result.src = address::v6(bytes.subspan<8, 16>());
        result.dst = address::v6(bytes.subspan<24, 16>());
        result.type = std::to_integer<uint8_t>(bytes[6]);
        result.payload = bytes.subspan(40);
        return result;
      }
    }
    return std::nullopt;
  }

  address src;
  address dst;
  uint8_t type;
  std::span<const std::byte> payload;
};

/// A layer 4 segment.
struct segment {
  static std::optional<segment>
  make(std::span<const std::byte> bytes, uint8_t type) {
    segment result;
    switch (type) {
      default:
        break;
      case IPPROTO_TCP: {
        constexpr size_t min_tcp_header_size = 20;
        if (bytes.size() < min_tcp_header_size)
          return std::nullopt;
        result.src = to_uint16(bytes.subspan<0, 2>());
        result.dst = to_uint16(bytes.subspan<2, 2>());
        result.type = port_type::tcp;
        size_t data_offset = (std::to_integer<uint8_t>(bytes[12]) >> 4) * 4;
        if (bytes.size() < data_offset)
          return std::nullopt;
        result.payload = bytes.subspan(data_offset);
        return result;
      }
      case IPPROTO_UDP: {
        constexpr size_t udp_header_size = 8;
        if (bytes.size() < udp_header_size)
          return std::nullopt;
        result.src = to_uint16(bytes.subspan<0, 2>());
        result.dst = to_uint16(bytes.subspan<2, 2>());
        result.type = port_type::udp;
        result.payload = bytes.subspan<8>();
        return result;
      }
      case IPPROTO_ICMP: {
        constexpr size_t icmp_header_size = 8;
        if (bytes.size() < icmp_header_size)
          return std::nullopt;
        auto message_type = std::to_integer<uint8_t>(bytes[0]);
        auto message_code = std::to_integer<uint8_t>(bytes[1]);
        result.src = message_type;
        result.dst = message_code;
        result.type = port_type::icmp;
        result.payload = bytes.subspan<8>();
        return result;
      }
    }
    return std::nullopt;
  }

  uint16_t src;
  uint16_t dst;
  port_type type;
  std::span<const std::byte> payload;
};

/// A PCAP reader.
class reader : public format::single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a PCAP reader.
  /// @param options Additional options.
  explicit reader(const caf::settings& options) : super(options) {
    using defaults_t = vast::defaults::import::pcap;
    using caf::get_if;
    std::string category = "vast.import.pcap";
    if (auto interface = get_if<std::string>(&options, category + ".interface"))
      interface_ = *interface;
    input_ = get_or(options, "vast.import.read",
                    vast::defaults::import::read.data());
    cutoff_ = get_or(options, category + ".cutoff", defaults_t::cutoff);
    max_flows_
      = get_or(options, category + ".max-flows", defaults_t::max_flows);
    max_age_
      = get_or(options, category + ".max-flow-age", defaults_t::max_flow_age);
    expire_interval_
      = get_or(options, category + ".flow-expiry", defaults_t::flow_expiry);
    pseudo_realtime_ = get_or(options, category + ".pseudo-realtime-factor",
                              defaults_t::pseudo_realtime_factor);
    snaplen_ = get_or(options, category + ".snaplen", defaults_t::snaplen);
    drop_rate_threshold_
      = get_or(options, category + ".drop-rate-threshold", 0.05);
    community_id_ = !get_or(options, category + ".disable-community-id", false);
    packet_type_ = make_packet_type();
    last_stats_ = {};
    discard_count_ = 0;
  }

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;
  ~reader() override = default;

  void reset([[maybe_unused]] std::unique_ptr<std::istream> in) override {
    // This function intentionally does nothing, as libpcap expects a filename
    // instead of an input stream. It only exists for compatibility with our
    // reader abstraction.
  }

  caf::error module(class module new_module) override {
    return replace_if_congruent({&packet_type_}, new_module);
  }

  class module module() const override {
    class module result {};
    result.add(packet_type_);
    return result;
  }

  const char* name() const override {
    return "pcap-reader";
  }

  vast::system::report status() const override {
    using namespace std::string_literals;
    if (!pcap_)
      return {};
    auto stats = pcap_stat{};
    if (auto res = pcap_stats(pcap_.get(), &stats); res != 0)
      return {};
    uint64_t recv = stats.ps_recv - last_stats_.ps_recv;
    if (recv == 0)
      return {};
    uint64_t drop = stats.ps_drop - last_stats_.ps_drop;
    uint64_t ifdrop = stats.ps_ifdrop - last_stats_.ps_ifdrop;
    double drop_rate = static_cast<double>(drop + ifdrop) / recv;
    uint64_t discard = discard_count_;
    double discard_rate = static_cast<double>(discard) / recv;
    // Clean up for next delta.
    last_stats_ = stats;
    discard_count_ = 0;
    if (drop_rate >= drop_rate_threshold_)
      VAST_WARN("{} has dropped {} of {} recent packets",
                detail::pretty_type_name(this), drop + ifdrop, recv);
    if (discard > 0)
      VAST_DEBUG("{} has discarded {} of {} recent packets",
                 detail::pretty_type_name(this), discard, recv);
    return {
      .data = {
        {name() + ".recv"s, recv},
        {name() + ".drop"s, drop},
        {name() + ".ifdrop"s, ifdrop},
        {name() + ".drop-rate"s, drop_rate},
        {name() + ".discard"s, discard},
        {name() + ".discard-rate"s, discard_rate},
      },
    };
  }

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override {
    // Sanity checks.
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    if (builder_ == nullptr) {
      if (!caf::holds_alternative<record_type>(packet_type_))
        return caf::make_error(ec::parse_error, "illegal packet type");
      if (!reset_builder(packet_type_))
        return caf::make_error(ec::parse_error, "unable to create builder for "
                                                "packet type");
    }
    // Local buffer for storing error messages.
    char buf[PCAP_ERRBUF_SIZE];
    // Initialize PCAP if needed.
    if (!pcap_) {
      std::error_code err{};
      const auto file_exists
        = std::filesystem::exists(std::filesystem::path{input_}, err);
      if (err)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("failed to find file {}: {}", input_,
                                           err.message()));
      // Determine interfaces.
      if (interface_) {
        pcap_.reset(
          ::pcap_open_live(interface_->c_str(), snaplen_, 1, 1000, buf));
        if (!pcap_) {
          return caf::make_error(ec::format_error, "failed to open interface",
                                 *interface_, ":", buf);
        }
        if (pseudo_realtime_ > 0) {
          pseudo_realtime_ = 0;
          VAST_WARN("{} ignores pseudo-realtime in live mode",
                    detail::pretty_type_name(this));
        }
        VAST_INFO("{} listens on interface {}", detail::pretty_type_name(this),
                  *interface_);
      } else if (input_ != "-" && !file_exists) {
        return caf::make_error(ec::format_error, "no such file: ", input_);
      } else {
#ifdef PCAP_TSTAMP_PRECISION_NANO
        pcap_.reset(::pcap_open_offline_with_tstamp_precision(
          input_.c_str(), PCAP_TSTAMP_PRECISION_NANO, buf));
#else
        pcap_ = ::pcap_open_offline(input_.c_str(), buf);
#endif
        if (!pcap_) {
          flows_.clear();
          return caf::make_error(ec::format_error, "failed to open pcap file ",
                                 input_, ": ", std::string{buf});
        }
        VAST_INFO("{} reads trace from {}", detail::pretty_type_name(this),
                  input_);
        if (pseudo_realtime_ > 0)
          VAST_VERBOSE("{} uses pseudo-realtime factor 1 / {}",
                       detail::pretty_type_name(this), pseudo_realtime_);
      }
      VAST_VERBOSE("{} cuts off flows after {} bytes in each direction",
                   detail::pretty_type_name(this), cutoff_);
      VAST_VERBOSE("{} keeps at most {} concurrent flows",
                   detail::pretty_type_name(this), max_flows_);
      VAST_VERBOSE("{} evicts flows after {} s of inactivity",
                   detail::pretty_type_name(this), max_age_);
      VAST_VERBOSE("{} expires flow table every {} s",
                   detail::pretty_type_name(this), expire_interval_);
    }
    auto produced = size_t{0};
    while (produced < max_events) {
      if (batch_events_ > 0 && batch_timeout_ > reader_clock::duration::zero()
          && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
        VAST_DEBUG("{} reached batch timeout", detail::pretty_type_name(this));
        return finish(f, ec::timeout);
      }
      // Attempt to fetch next packet.
      const u_char* data = nullptr;
      pcap_pkthdr* header = nullptr;
      auto r = ::pcap_next_ex(pcap_.get(), &header, &data);
      if (r == 0 && produced == 0)
        continue; // timed out, no events produced yet
      if (r == 0)
        return finish(f, caf::none); // timed out
      if (r == -2)
        return finish(f, caf::make_error(ec::end_of_input, "reached end of "
                                                           "trace"));
      if (r == -1) {
        auto err = std::string{::pcap_geterr(pcap_.get())};
        pcap_ = nullptr;
        return finish(f, caf::make_error(ec::format_error,
                                         "failed to get next packet: ", err));
      }
      auto raw_frame = std::span<const std::byte>{
        reinterpret_cast<const std::byte*>(data), header->len};
      // Parse layer 2.
      auto frame = frame::make(raw_frame, frame_type::ethernet);
      if (!frame)
        return caf::make_error(ec::format_error, "failed to decapsulate frame");
      // Parse layer 3.
      auto packet = packet::make(frame->payload, frame->type);
      if (!packet) {
        ++discard_count_;
        VAST_DEBUG("skipping packet of type {}", frame->type);
        continue;
      }
      // Parse layer 4.
      auto segment = segment::make(packet->payload, packet->type);
      if (!segment) {
        ++discard_count_;
        VAST_DEBUG("skipping segment of type {:#0x}", packet->type);
        continue;
      }
      // Make connection
      auto conn = make_flow(packet->src, packet->dst, segment->src,
                            segment->dst, segment->type);
      // Parse packet timestamp
      uint64_t packet_time = header->ts.tv_sec;
      if (last_expire_ == 0)
        last_expire_ = packet_time;
      if (!update_flow(conn, packet_time, segment->payload.size())) {
        ++discard_count_;
        VAST_DEBUG("{} skips cut off packet", detail::pretty_type_name(this));
        continue;
      }
      evict_inactive(packet_time);
      shrink_to_max_size();
      // Extract timestamp.
      using namespace std::chrono;
      auto secs = seconds(header->ts.tv_sec);
      auto ts = time{duration_cast<duration>(secs)};
#ifdef PCAP_TSTAMP_PRECISION_NANO
      ts += nanoseconds(header->ts.tv_usec);
#else
      ts += microseconds(header->ts.tv_usec);
#endif
      // Assemble packet.
      auto make_string_view = [](std::span<const std::byte> bytes) {
        auto data = reinterpret_cast<const char*>(bytes.data());
        auto size = bytes.size();
        return std::string_view{data, size};
      };
      auto payload = make_string_view(raw_frame);
      auto& cid = state(conn).community_id;
      if (!(builder_->add(ts) && builder_->add(conn.src_addr)
            && builder_->add(conn.dst_addr)
            && builder_->add(conn.src_port.number())
            && builder_->add(conn.dst_port.number())
            && (frame->outer_vid ? builder_->add(count{*frame->outer_vid})
                                 : builder_->add(caf::none))
            && (frame->outer_vid ? builder_->add(count{*frame->inner_vid})
                                 : builder_->add(caf::none))
            && (community_id_ ? builder_->add(std::string_view{cid})
                              : builder_->add(caf::none))
            && builder_->add(payload))) {
        return caf::make_error(ec::parse_error, "unable to fill row");
      }
      ++produced;
      ++batch_events_;
      if (pseudo_realtime_ > 0) {
        if (ts < last_timestamp_) {
          VAST_WARN("{} encountered non-monotonic packet timestamps: {} {} {}",
                    detail::pretty_type_name(this),
                    ts.time_since_epoch().count(), '<',
                    last_timestamp_.time_since_epoch().count());
        }
        if (last_timestamp_ != time::min()) {
          auto delta = ts - last_timestamp_;
          std::this_thread::sleep_for(delta / pseudo_realtime_);
        }
        last_timestamp_ = ts;
      }
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(f, caf::none))
          return err;
    }
    return finish(f, caf::none);
  }

private:
  struct flow_state {
    uint64_t bytes;
    uint64_t last;
    std::string community_id;
  };

  /// @returns either an existing state associated to `x` or a new state for
  ///          the flow.
  flow_state& state(const flow& x) {
    auto i = flows_.find(x);
    if (i == flows_.end()) {
      auto id = community_id::compute<policy::base64>(x);
      i = flows_.emplace(x, flow_state{0, 0, std::move(id)}).first;
    }
    return i->second;
  }

  /// @returns whether `true` if the flow remains active, `false` if the flow
  ///          reached the configured cutoff.
  bool update_flow(const flow& x, uint64_t packet_time, uint64_t payload_size) {
    auto& st = state(x);
    st.last = packet_time;
    auto& flow_size = st.bytes;
    if (flow_size == cutoff_)
      return false;
    VAST_ASSERT(flow_size < cutoff_);
    // Trim the packet if needed.
    flow_size += std::min(payload_size, cutoff_ - flow_size);
    return true;
  }

  /// Evict all flows that have been inactive for the maximum age.
  void evict_inactive(uint64_t packet_time) {
    if (packet_time - last_expire_ <= expire_interval_)
      return;
    last_expire_ = packet_time;
    auto i = flows_.begin();
    while (i != flows_.end())
      if (packet_time - i->second.last > max_age_)
        i = flows_.erase(i);
      else
        ++i;
  }

  /// Evicts random flows when exceeding the maximum configured flow count.
  void shrink_to_max_size() {
    while (flows_.size() >= max_flows_) {
      auto buckets = flows_.bucket_count();
      auto unif1 = std::uniform_int_distribution<size_t>{0, buckets - 1};
      auto bucket = unif1(generator_);
      auto bucket_size = flows_.bucket_size(bucket);
      while (bucket_size == 0) {
        ++bucket;
        bucket %= buckets;
        bucket_size = flows_.bucket_size(bucket);
      }
      auto unif2 = std::uniform_int_distribution<size_t>{0, bucket_size - 1};
      auto offset = unif2(generator_);
      VAST_ASSERT(offset < bucket_size);
      auto begin = flows_.begin(bucket);
      std::advance(begin, offset);
      flows_.erase(begin->first);
    }
  }

  std::unique_ptr<struct pcap, pcap_close_wrapper> pcap_ = nullptr;
  std::unordered_map<flow, flow_state> flows_;
  std::string input_;
  std::optional<std::string> interface_;
  uint64_t cutoff_;
  size_t max_flows_;
  std::mt19937 generator_;
  uint64_t max_age_;
  uint64_t expire_interval_;
  uint64_t last_expire_ = 0;
  time last_timestamp_ = time::min();
  int64_t pseudo_realtime_;
  size_t snaplen_;
  bool community_id_;
  type packet_type_;
  double drop_rate_threshold_;
  mutable pcap_stat last_stats_;
  mutable size_t discard_count_;
};

/// A PCAP writer.
class writer : public format::writer {
public:
  using defaults = vast::defaults::export_::pcap;

  /// Constructs a PCAP writer.
  /// @param options The configuration options for the writer.
  explicit writer(const caf::settings& options) {
    flush_interval_ = get_or(options, "vast.export.pcap.flush-interval",
                             defaults::flush_interval);
    trace_ = get_or(options, "vast.export.write",
                    vast::defaults::export_::write.data());
  }

  ~writer() override = default;

  using format::writer::write;

  caf::error write(const table_slice& slice) override {
    if (!pcap_) {
#ifdef PCAP_TSTAMP_PRECISION_NANO
      pcap_.reset(::pcap_open_dead_with_tstamp_precision(
        DLT_EN10MB, snaplen_, PCAP_TSTAMP_PRECISION_NANO));
#else
    pcap_.reset(::pcap_open_dead(DLT_EN10MB, snaplen_);
#endif
      if (!pcap_)
        return caf::make_error(ec::format_error, "failed to open pcap handle");
      dumper_.reset(::pcap_dump_open(pcap_.get(), trace_.c_str()));
      if (!dumper_)
        return caf::make_error(ec::format_error, "failed to open pcap dumper");
    }
    auto&& layout = slice.layout();
    // TODO: relax this check. We really only need the (1) flow, and (2) PCAP
    // payload. Everything else is optional.
    if (!congruent(layout, make_packet_type()))
      return caf::make_error(ec::format_error, "invalid pcap packet type");
    // TODO: Consider iterating in natural order for the slice.
    // TODO: Calculate column offsets and indices only once instead
    // of again for every row.
    for (size_t row = 0; row < slice.rows(); ++row) {
      const auto& layout_rt = caf::get<record_type>(layout);
      const auto payload_offset = layout_rt.resolve_key("payload");
      VAST_ASSERT(payload_offset);
      auto payload_field = slice.at(row, layout_rt.flat_index(*payload_offset),
                                    layout_rt.field(*payload_offset).type);
      auto& payload = caf::get<view<std::string>>(payload_field);
      // Make PCAP header.
      ::pcap_pkthdr header{};
      const auto time_offset = layout_rt.resolve_key("time");
      VAST_ASSERT(time_offset);
      auto ns_field = slice.at(row, layout_rt.flat_index(*time_offset),
                               layout_rt.field(*time_offset).type);
      auto ns = caf::get<view<time>>(ns_field).time_since_epoch().count();
      header.ts.tv_sec = ns / 1000000000;
#ifdef PCAP_TSTAMP_PRECISION_NANO
      header.ts.tv_usec = ns % 1000000000;
#else
      ns /= 1000;
      header.ts.tv_usec = ns % 1000000;
#endif
      header.caplen = payload.size();
      header.len = payload.size();
      // Dump packet.
      ::pcap_dump(
        reinterpret_cast<uint8_t*>(dumper_.get()), &header,
        reinterpret_cast<const uint8_t*>(std::launder(payload.data())));
    }
    if (++total_packets_ % flush_interval_ == 0)
      if (auto r = flush(); !r)
        return r.error();
    return caf::none;
  }

  caf::expected<void> flush() override {
    if (!dumper_)
      return caf::make_error(ec::format_error, "pcap dumper not open");
    VAST_DEBUG("{} flushes at packet {}", detail::pretty_type_name(this),
               total_packets_);
    if (::pcap_dump_flush(dumper_.get()) == -1)
      return caf::make_error(ec::format_error, "failed to flush");
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "pcap-writer";
  }

private:
  vast::module module_;
  size_t flush_interval_ = 0;
  size_t snaplen_ = 65535;
  size_t total_packets_ = 0;
  std::unique_ptr<struct pcap, pcap_close_wrapper> pcap_ = nullptr;
  std::unique_ptr<struct pcap_dumper, pcap_dump_close_wrapper> dumper_
    = nullptr;
  std::string trace_;
};

/// The PCAP reader/writer plugin.
class plugin final : public virtual reader_plugin,
                     public virtual writer_plugin {
public:
  /// Loading logic.
  plugin() = default;

  /// Teardown logic.
  ~plugin() override = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  caf::error initialize(data config) override {
    if (auto* r = caf::get_if<record>(&config))
      config_ = *r;
    return caf::none;
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] std::string_view name() const override {
    return "pcap";
  }

  /// Returns the import format's name.
  [[nodiscard]] const char* reader_format() const override {
    return "pcap";
  }

  /// Returns the `vast import <format>` helptext.
  [[nodiscard]] const char* reader_help() const override {
    return "imports PCAP logs from STDIN or file";
  }

  /// Returns the options for the `vast import <format>` and `vast spawn source
  /// <format>` commands.
  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<std::string>("interface,i", "network interface to read packets from")
      .add<int64_t>("cutoff,c", "skip flow packets after this many bytes")
      .add<int64_t>("max-flows,m", "number of concurrent flows to track")
      .add<int64_t>("max-flow-age,a", "max flow lifetime before eviction")
      .add<int64_t>("flow-expiry,e", "flow table expiration interval")
      .add<int64_t>("pseudo-realtime-factor,p", "factor c delaying packets by "
                                                "1/c")
      .add<int64_t>("snaplen", "snapshot length in bytes")
      .add<double>("drop-rate-threshold", "drop rate that must be exceeded for "
                                          "warnings to occur")
      .add<bool>("disable-community-id", "disable computation of community id "
                                         "for every packet")
      .finish();
  };

  /// Creates a reader, which will be available via `vast import <format>` and
  /// `vast spawn source <format>`.
  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const override {
    return std::make_unique<reader>(options);
  }

  /// Returns the export format's name.
  [[nodiscard]] const char* writer_format() const override {
    return "pcap";
  }

  /// Returns the `vast export <format>` helptext.
  [[nodiscard]] const char* writer_help() const override {
    return "exports query results in PCAP format";
  }

  /// Returns the options for the `vast export <format>` and `vast spawn sink
  /// <format>` commands.
  [[nodiscard]] caf::config_option_set
  writer_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<int64_t>("flush-interval,f", "flush to disk after this many packets")
      .finish();
  }

  /// Creates a reader, which will be available via `vast export <format>` and
  /// `vast spawn sink <format>`.
  [[nodiscard]] std::unique_ptr<format::writer>
  make_writer(const caf::settings& options) const override {
    return std::make_unique<writer>(options);
  }

private:
  record config_ = {};
};

} // namespace vast::plugins::pcap

VAST_REGISTER_PLUGIN(vast::plugins::pcap::plugin)
