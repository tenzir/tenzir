//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/community_id.hpp"
#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/ether_type.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/format/writer.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/schema.hpp"

#include <caf/settings.hpp>

#include <pcap.h>
#include <random>

#include <netinet/in.h>

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
  static constexpr size_t snaplen = 65535;
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

template <class... RecordFields>
record_type make_packet_type(RecordFields&&... record_fields) {
  // FIXME: once we ship with builtin type aliases, we should reference the
  // port alias type here. Until then, we create the alias manually.
  // See also:
  // - src/format/zeek.cpp
  auto port_type = count_type{}.name("port");
  return record_type{{"time", time_type{}.name("timestamp")},
                     {"src", address_type{}},
                     {"dst", address_type{}},
                     {"sport", port_type},
                     {"dport", port_type},
                     std::forward<RecordFields>(record_fields)...,
                     {"payload", string_type{}.attributes({{"skip"}})}}
    .name("pcap.packet");
}

const auto pcap_packet_type = make_packet_type();
const auto pcap_packet_type_community_id = make_packet_type(
  record_field{"community_id", string_type{}.attributes({{"index", "hash"}})});

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

enum class frame_type : char {
  chap_none = '\x00',
  chap_challenge = '\x01',
  chap_response = '\x02',
  chap_both = '\x03',
  ethernet = '\x01',
  vlan = '\x02',
  mpls = '\x03',
  pppoe = '\x04',
  ppp = '\x05',
  chap = '\x06',
  ipv4 = '\x07',
  udp = '\x08',
  radius = '\x09',
  radavp = '\x0a',
  l2tp = '\x0b',
  l2avp = '\x0c',
  ospfv2 = '\x0d',
  ospf_md5 = '\x0e',
  tcp = '\x0f',
  ip_md5 = '\x10',
  unknown = '\x11',
  gre = '\x12',
  gtp = '\x13',
  vxlan = '\x14'
};

// Strips all data from a frame until the IP layer is reached. The frame type
// distinguisher exists for future recursive stripping.
span<const std::byte>
decapsulate(span<const std::byte> frame, frame_type type) {
  switch (type) {
    default:
      return {};
    case frame_type::ethernet: {
      constexpr size_t ethernet_header_size = 14;
      if (frame.size() < ethernet_header_size)
        return {}; // need at least 2 MAC addresses and the 2-byte EtherType.
      switch (as_ether_type(frame.subspan<12, 2>())) {
        default:
          return frame;
        case ether_type::ieee_802_1aq:
          return frame.subspan<4>(); // One 32-bit VLAN tag
        case ether_type::ieee_802_1q_db:
          return frame.subspan<2 * 4>(); // Two 32-bit VLAN tags
      }
    }
  }
}

/// A PCAP reader.
class reader : public format::single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a PCAP reader.
  /// @param options Additional options.
  /// @param in Input stream (unused). Pass filename via options instead.
  explicit reader(const caf::settings& options,
                  [[maybe_unused]] std::unique_ptr<std::istream> in)
    : super(options) {
    using defaults_t = vast::defaults::import::pcap;
    using caf::get_if;
    std::string category = "vast.import.pcap";
    if (auto interface = get_if<std::string>(&options, category + ".interface"))
      interface_ = *interface;
    input_ = get_or(options, "vast.import.read", vast::defaults::import::read);
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
    packet_type_
      = community_id_ ? pcap_packet_type_community_id : pcap_packet_type;
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

  caf::error schema(class schema new_schema) override {
    return replace_if_congruent({&packet_type_}, new_schema);
  }

  class schema schema() const override {
    class schema result {};
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
      VAST_WARN("{} has discarded {} of {} recent packets",
                detail::pretty_type_name(this), discard, recv);
    return {
      {name() + ".recv"s, recv},
      {name() + ".drop"s, drop},
      {name() + ".ifdrop"s, ifdrop},
      {name() + ".drop-rate"s, drop_rate},
      {name() + ".discard"s, discard},
      {name() + ".discard-rate"s, discard_rate},
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
      if (!reset_builder(caf::get<record_type>(packet_type_)))
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
      // Parse frame.
      span<const std::byte> frame{reinterpret_cast<const std::byte*>(data),
                                  header->len};
      frame = decapsulate(frame, frame_type::ethernet);
      if (frame.empty())
        return caf::make_error(ec::format_error, "failed to decapsulate frame");
      constexpr size_t ethernet_header_size = 14;
      auto layer3 = frame.subspan<ethernet_header_size>();
      span<const std::byte> layer4;
      uint8_t layer4_proto = 0;
      flow conn;
      // Parse layer 3.
      switch (as_ether_type(frame.subspan<12, 2>())) {
        default: {
          ++discard_count_;
          VAST_DEBUG("{} skips non-IP packet", detail::pretty_type_name(this));
          continue;
        }
        case ether_type::ipv4: {
          constexpr size_t ipv4_header_size = 20;
          if (header->len < ethernet_header_size + ipv4_header_size)
            return caf::make_error(ec::format_error, "IPv4 header too short");
          size_t header_size = (std::to_integer<uint8_t>(layer3[0]) & 0x0f) * 4;
          if (header_size < ipv4_header_size)
            return caf::make_error(ec::format_error,
                                   "IPv4 header too short: ", header_size,
                                   " bytes");
          const auto* orig_h = reinterpret_cast<const uint32_t*>(
            std::launder(layer3.data() + 12));
          const auto* resp_h = reinterpret_cast<const uint32_t*>(
            std::launder(layer3.data() + 16));
          conn.src_addr = {orig_h, address::ipv4, address::network};
          conn.dst_addr = {resp_h, address::ipv4, address::network};
          layer4_proto = std::to_integer<uint8_t>(layer3[9]);
          layer4 = layer3.subspan(header_size);
          break;
        }
        case ether_type::ipv6: {
          if (header->len < ethernet_header_size + 40)
            return caf::make_error(ec::format_error, "IPv6 header too short");
          const auto* orig_h = reinterpret_cast<const uint32_t*>(
            std::launder(layer3.data() + 8));
          const auto* resp_h = reinterpret_cast<const uint32_t*>(
            std::launder(layer3.data() + 24));
          conn.src_addr = {orig_h, address::ipv4, address::network};
          conn.dst_addr = {resp_h, address::ipv4, address::network};
          layer4_proto = std::to_integer<uint8_t>(layer3[6]);
          layer4 = layer3.subspan(40);
          break;
        }
      }
      // Parse layer 4.
      auto payload_size = layer4.size();
      if (layer4_proto == IPPROTO_TCP) {
        VAST_ASSERT(!layer4.empty());
        auto orig_p
          = *reinterpret_cast<const uint16_t*>(std::launder(layer4.data()));
        auto resp_p
          = *reinterpret_cast<const uint16_t*>(std::launder(layer4.data() + 2));
        orig_p = detail::to_host_order(orig_p);
        resp_p = detail::to_host_order(resp_p);
        conn.src_port = {orig_p, port_type::tcp};
        conn.dst_port = {resp_p, port_type::tcp};
        auto data_offset
          = *reinterpret_cast<const uint8_t*>(std::launder(layer4.data() + 12))
            >> 4;
        payload_size -= data_offset * 4;
      } else if (layer4_proto == IPPROTO_UDP) {
        VAST_ASSERT(!layer4.empty());
        auto orig_p
          = *reinterpret_cast<const uint16_t*>(std::launder(layer4.data()));
        auto resp_p
          = *reinterpret_cast<const uint16_t*>(std::launder(layer4.data() + 2));
        orig_p = detail::to_host_order(orig_p);
        resp_p = detail::to_host_order(resp_p);
        conn.src_port = {orig_p, port_type::udp};
        conn.dst_port = {resp_p, port_type::udp};
        payload_size -= 8;
      } else if (layer4_proto == IPPROTO_ICMP) {
        VAST_ASSERT(!layer4.empty());
        auto message_type = std::to_integer<uint8_t>(layer4[0]);
        auto message_code = std::to_integer<uint8_t>(layer4[1]);
        conn.src_port = {message_type, port_type::icmp};
        conn.dst_port = {message_code, port_type::icmp};
        payload_size -= 8; // TODO: account for variable-size data.
      }
      // Parse packet timestamp
      uint64_t packet_time = header->ts.tv_sec;
      if (last_expire_ == 0)
        last_expire_ = packet_time;
      if (!update_flow(conn, packet_time, payload_size)) {
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
      const auto* layer3_ptr = reinterpret_cast<const char*>(layer3.data());
      auto packet = std::string_view{std::launder(layer3_ptr), layer3.size()};
      auto& cid = state(conn).community_id;
      if (!(builder_->add(ts) && builder_->add(conn.src_addr)
            && builder_->add(conn.dst_addr)
            && builder_->add(conn.src_port.number())
            && builder_->add(conn.dst_port.number())
            && (!community_id_ || builder_->add(std::string_view{cid}))
            && builder_->add(packet))) {
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
  caf::optional<std::string> interface_;
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
  explicit writer([[maybe_unused]] std::unique_ptr<std::ostream> out,
                  const caf::settings& options) {
    flush_interval_ = get_or(options, "vast.export.pcap.flush-interval",
                             defaults::flush_interval);
    trace_
      = get_or(options, "vast.export.write", vast::defaults::export_::write);
  }

  ~writer() override = default;

  using format::writer::write;

  caf::error write(const table_slice& slice) override {
    if (!pcap_) {
#ifdef PCAP_TSTAMP_PRECISION_NANO
      pcap_.reset(::pcap_open_dead_with_tstamp_precision(
        DLT_RAW, snaplen_, PCAP_TSTAMP_PRECISION_NANO));
#else
    pcap_.reset(::pcap_open_dead(DLT_RAW, snaplen_);
#endif
      if (!pcap_)
        return caf::make_error(ec::format_error, "failed to open pcap handle");
      dumper_.reset(::pcap_dump_open(pcap_.get(), trace_.c_str()));
      if (!dumper_)
        return caf::make_error(ec::format_error, "failed to open pcap dumper");
    }
    auto&& layout = slice.layout();
    if (!congruent(layout, pcap_packet_type)
        && !congruent(layout, pcap_packet_type_community_id))
      return caf::make_error(ec::format_error, "invalid pcap packet type");
    // TODO: consider iterating in natural order for the slice.
    for (size_t row = 0; row < slice.rows(); ++row) {
      auto payload_field
        = slice.at(row, 6, pcap_packet_type.at("payload")->type);
      auto& payload = caf::get<view<std::string>>(payload_field);
      // Make PCAP header.
      ::pcap_pkthdr header{};
      auto ns_field = slice.at(row, 0, pcap_packet_type.at("time")->type);
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
    return caf::no_error;
  }

  [[nodiscard]] const char* name() const override {
    return "pcap-writer";
  }

private:
  vast::schema schema_;
  size_t flush_interval_ = 0;
  size_t snaplen_ = 65535;
  size_t total_packets_ = 0;
  std::unique_ptr<struct pcap, pcap_close_wrapper> pcap_ = nullptr;
  std::unique_ptr<struct pcap_dumper, pcap_dump_close_wrapper> dumper_
    = nullptr;
  std::string trace_;
};

/// An example plugin.
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
  [[nodiscard]] const char* name() const override {
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

  /// Returns the `vast import <format>` documentation.
  [[nodiscard]] const char* reader_documentation() const override {
    return R"__(The `import pcap` command uses [libpcap](https://www.tcpdump.org) to read
network packets from a trace or an interface.

The `spawn source pcap` command spawns a PCAP source inside the node and is the
analog to the `import pcap` command.

VAST automatically calculates the [Community
ID](https://github.com/corelight/community-id-spec) for PCAPs for better
pivoting support. The extra computation induces an overhead of approximately 15%
of the ingestion rate. The option `--disable-community-id` disables the
computation completely.

The PCAP import format has many additional options that offer a user interface
that should be familiar to users of other tools interacting with PCAPs. To see
a list of all available options, run `vast import pcap help`.

Here's an example that reads from the network interface `en0` cuts off packets
after 65535 bytes.

```bash
sudo vast import pcap --interface=en0 --cutoff=65535
```
)__";
  }

  /// Returns the options for the `vast import <format>` and `vast spawn source
  /// <format>` commands.
  [[nodiscard]] caf::config_option_set
  reader_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<std::string>("interface,i", "network interface to read packets from")
      .add<size_t>("cutoff,c", "skip flow packets after this many bytes")
      .add<size_t>("max-flows,m", "number of concurrent flows to track")
      .add<size_t>("max-flow-age,a", "max flow lifetime before eviction")
      .add<size_t>("flow-expiry,e", "flow table expiration interval")
      .add<size_t>("pseudo-realtime-factor,p", "factor c delaying packets by "
                                               "1/c")
      .add<size_t>("snaplen", "snapshot length in bytes")
      .add<double>("drop-rate-threshold", "drop rate that must be exceeded for "
                                          "warnings to occur")
      .add<bool>("disable-community-id", "disable computation of community id "
                                         "for every packet")
      .finish();
  };

  /// Creates a reader, which will be available via `vast import <format>` and
  /// `vast spawn source <format>`.
  [[nodiscard]] std::unique_ptr<format::reader>
  make_reader(const caf::settings& options,
              std::unique_ptr<std::istream> in) const override {
    return std::make_unique<reader>(options, std::move(in));
  }

  /// Returns the export format's name.
  [[nodiscard]] const char* writer_format() const override {
    return "pcap";
  }

  /// Returns the `vast export <format>` helptext.
  [[nodiscard]] const char* writer_help() const override {
    return "exports query results in PCAP format";
  }

  /// Returns the `vast export <format>` documentation.
  [[nodiscard]] const char* writer_documentation() const override {
    return R"__(The PCAP export format uses [libpcap](https://www.tcpdump.org) to write PCAP
events as a trace.

This command only supports events of type `pcap.packet`. As a result, VAST
transforms a provided query expression `E` into `#type == "pcap.packet" && E`.
)__";
  }

  /// Returns the options for the `vast export <format>` and `vast spawn sink
  /// <format>` commands.
  [[nodiscard]] caf::config_option_set
  writer_options(command::opts_builder&& opts) const override {
    return std::move(opts)
      .add<size_t>("flush-interval,f", "flush to disk after this many packets")
      .finish();
  }

  /// Creates a reader, which will be available via `vast export <format>` and
  /// `vast spawn sink <format>`.
  [[nodiscard]] std::unique_ptr<format::writer>
  make_writer(const caf::settings& options,
              std::unique_ptr<std::ostream> out) const override {
    return std::make_unique<writer>(std::move(out), options);
  }

private:
  record config_ = {};
};

} // namespace vast::plugins::pcap

VAST_REGISTER_PLUGIN(vast::plugins::pcap::plugin, vast::version::major,
                     vast::version::minor, vast::version::patch,
                     vast::version::tweak)
