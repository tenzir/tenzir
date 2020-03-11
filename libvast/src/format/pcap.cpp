/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/format/pcap.hpp"

#include "vast/byte.hpp"
#include "vast/community_id.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/error.hpp"
#include "vast/ether_type.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/span.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <thread>
#include <utility>

#include <netinet/in.h>

namespace vast {
namespace format {
namespace pcap {
namespace {

template <class... RecordFields>
inline type make_packet_type(RecordFields&&... record_fields) {
  return record_type{{"time", time_type{}.attributes({{"timestamp"}})},
                     {"src", address_type{}},
                     {"dst", address_type{}},
                     {"sport", port_type{}},
                     {"dport", port_type{}},
                     std::forward<RecordFields>(record_fields)...,
                     {"payload", string_type{}.attributes({{"skip"}})}}
    .name("pcap.packet");
}

inline const auto pcap_packet_type = make_packet_type();
inline const auto pcap_packet_type_community_id = make_packet_type(
  record_field{"community_id", string_type{}.attributes({{"index", "hash"}})});

} // namespace <anonymous>

reader::reader(caf::atom_value id, const caf::settings& options,
               std::unique_ptr<std::istream>)
  : super(id) {
  using defaults_t = vast::defaults::import::pcap;
  using caf::get_if;
  std::string category = defaults_t::category;
  if (auto interface = get_if<std::string>(&options, category + ".interface"))
    interface_ = *interface;
  input_ = get_or(options, category + ".read", defaults_t::read);
  cutoff_ = get_or(options, category + ".cutoff", defaults_t::cutoff);
  max_flows_ = get_or(options, category + ".max-flows", defaults_t::max_flows);
  max_age_
    = get_or(options, category + ".max-flow-age", defaults_t::max_flow_age);
  expire_interval_
    = get_or(options, category + ".flow-expiry", defaults_t::flow_expiry);
  pseudo_realtime_ = get_or(options, category + ".pseudo-realtime-factor",
                            defaults_t::pseudo_realtime_factor);
  snaplen_ = get_or(options, category + ".snaplen", defaults_t::snaplen);
  community_id_ = !get_or(options, category + ".disable-community-id", false);
  packet_type_
    = community_id_ ? pcap_packet_type_community_id : pcap_packet_type;
}

void reader::reset(std::unique_ptr<std::istream>) {
  // This function intentionally does nothing, as libpcap expects a filename
  // instead of an input stream. It only exists for compatibility with our
  // reader abstraction.
}

reader::~reader() {
  if (pcap_)
    ::pcap_close(pcap_);
}

caf::error reader::schema(vast::schema sch) {
  return replace_if_congruent({&packet_type_}, sch);
}

schema reader::schema() const {
  vast::schema result;
  result.add(packet_type_);
  return result;
}

const char* reader::name() const {
  return "pcap-reader";
}

namespace {

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
span<const byte> decapsulate(span<const byte> frame, frame_type type) {
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

} // namespace

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& f) {
  // Sanity checks.
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  if (builder_ == nullptr) {
    if (!caf::holds_alternative<record_type>(packet_type_))
      return make_error(ec::parse_error, "illegal packet type");
    if (!reset_builder(caf::get<record_type>(packet_type_)))
      return make_error(ec::parse_error,
                        "unable to create builder for packet type");
  }
  // Local buffer for storing error messages.
  char buf[PCAP_ERRBUF_SIZE];
  // Initialize PCAP if needed.
  if (!pcap_) {
    // Determine interfaces.
    if (interface_) {
      pcap_ = ::pcap_open_live(interface_->c_str(), snaplen_, 1, 1000, buf);
      if (!pcap_) {
        return make_error(ec::format_error, "failed to open interface",
                          *interface_, ":", buf);
      }
      if (pseudo_realtime_ > 0) {
        pseudo_realtime_ = 0;
        VAST_WARNING(this, "ignores pseudo-realtime in live mode");
      }
      VAST_DEBUG(this, "listens on interface", *interface_);
    } else if (input_ != "-" && !exists(input_)) {
      return make_error(ec::format_error, "no such file: ", input_);
    } else {
#ifdef PCAP_TSTAMP_PRECISION_NANO
      pcap_ = ::
        pcap_open_offline_with_tstamp_precision(input_.c_str(),
                                                PCAP_TSTAMP_PRECISION_NANO,
                                                buf);
#else
      pcap_ = ::pcap_open_offline(input_.c_str(), buf);
#endif
      if (!pcap_) {
        flows_.clear();
        return make_error(ec::format_error, "failed to open pcap file ", input_,
                          ": ", std::string{buf});
      }
      VAST_DEBUG(this, "reads trace from", input_);
      if (pseudo_realtime_ > 0)
        VAST_DEBUG(this, "uses pseudo-realtime factor 1/" << pseudo_realtime_);
    }
    VAST_DEBUG(this, "cuts off flows after", cutoff_,
               "bytes in each direction");
    VAST_DEBUG(this, "keeps at most", max_flows_, "concurrent flows");
    VAST_DEBUG(this, "evicts flows after", max_age_ << "s of inactivity");
    VAST_DEBUG(this, "expires flow table every", expire_interval_ << "s");
  }
  for (size_t produced = 0; produced < max_events; ++produced) {
    // Attempt to fetch next packet.
    const u_char* data;
    pcap_pkthdr* header;
    auto r = ::pcap_next_ex(pcap_, &header, &data);
    if (r == 0)
      return finish(f, caf::none); // timed out
    if (r == -2)
      return finish(f, make_error(ec::end_of_input, "reached end of trace"));
    if (r == -1) {
      auto err = std::string{::pcap_geterr(pcap_)};
      ::pcap_close(pcap_);
      pcap_ = nullptr;
      return finish(f, make_error(ec::format_error,
                                  "failed to get next packet: ", err));
    }
    // Parse frame.
    span<const byte> frame{reinterpret_cast<const byte*>(data), header->len};
    frame = decapsulate(frame, frame_type::ethernet);
    if (frame.empty())
      return make_error(ec::format_error, "failed to decapsulate frame");
    constexpr size_t ethernet_header_size = 14;
    auto layer3 = frame.subspan<ethernet_header_size>();
    span<const byte> layer4;
    uint8_t layer4_proto = 0;
    flow conn;
    // Parse layer 3.
    switch (as_ether_type(frame.subspan<12, 2>())) {
      default: {
        VAST_DEBUG(this, "skips non-IP packet");
        continue;
      }
      case ether_type::ipv4: {
        constexpr size_t ipv4_header_size = 20;
        if (header->len < ethernet_header_size + ipv4_header_size)
          return make_error(ec::format_error, "IPv4 header too short");
        size_t header_size = (to_integer<uint8_t>(layer3[0]) & 0x0f) * 4;
        if (header_size < ipv4_header_size)
          return make_error(ec::format_error,
                            "IPv4 header too short: ", header_size, " bytes");
        auto orig_h
          = reinterpret_cast<const uint32_t*>(std::launder(layer3.data() + 12));
        auto resp_h
          = reinterpret_cast<const uint32_t*>(std::launder(layer3.data() + 16));
        conn.src_addr = {orig_h, address::ipv4, address::network};
        conn.dst_addr = {resp_h, address::ipv4, address::network};
        layer4_proto = to_integer<uint8_t>(layer3[9]);
        layer4 = layer3.subspan(header_size);
        break;
      }
      case ether_type::ipv6: {
        if (header->len < ethernet_header_size + 40)
          return make_error(ec::format_error, "IPv6 header too short");
        auto orig_h
          = reinterpret_cast<const uint32_t*>(std::launder(layer3.data() + 8));
        auto resp_h
          = reinterpret_cast<const uint32_t*>(std::launder(layer3.data() + 24));
        conn.src_addr = {orig_h, address::ipv4, address::network};
        conn.dst_addr = {resp_h, address::ipv4, address::network};
        layer4_proto = to_integer<uint8_t>(layer3[6]);
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
      conn.src_port = {orig_p, port::tcp};
      conn.dst_port = {resp_p, port::tcp};
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
      conn.src_port = {orig_p, port::udp};
      conn.dst_port = {resp_p, port::udp};
      payload_size -= 8;
    } else if (layer4_proto == IPPROTO_ICMP) {
      VAST_ASSERT(!layer4.empty());
      auto message_type = to_integer<uint8_t>(layer4[0]);
      auto message_code = to_integer<uint8_t>(layer4[1]);
      conn.src_port = {message_type, port::icmp};
      conn.dst_port = {message_code, port::icmp};
      payload_size -= 8; // TODO: account for variable-size data.
    }
    // Parse packet timestamp
    uint64_t packet_time = header->ts.tv_sec;
    if (last_expire_ == 0)
      last_expire_ = packet_time;
    if (!update_flow(conn, packet_time, payload_size)) {
      // Skip cut off packets.
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
    auto layer3_ptr = reinterpret_cast<const char*>(layer3.data());
    auto packet = std::string_view{std::launder(layer3_ptr), layer3.size()};
    auto& cid = state(conn).community_id;
    if (!(builder_->add(ts) && builder_->add(conn.src_addr)
          && builder_->add(conn.dst_addr) && builder_->add(conn.src_port)
          && builder_->add(conn.dst_port)
          && (!community_id_ || builder_->add(std::string_view{cid}))
          && builder_->add(packet))) {
      return make_error(ec::parse_error, "unable to fill row");
    }
    if (pseudo_realtime_ > 0) {
      if (ts < last_timestamp_) {
        VAST_WARNING(this, "encountered non-monotonic packet timestamps:",
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

reader::flow_state& reader::state(const flow& x) {
  auto i = flows_.find(x);
  if (i == flows_.end()) {
    auto cf = flow{x.src_addr, x.dst_addr, x.src_port, x.dst_port};
    auto id = community_id::compute<policy::base64>(cf);
    i = flows_.emplace(x, flow_state{0, 0, std::move(id)}).first;
  }
  return i->second;
}

bool reader::update_flow(const flow& x, uint64_t packet_time,
                         uint64_t payload_size) {
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

void reader::evict_inactive(uint64_t packet_time) {
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

void reader::shrink_to_max_size() {
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

writer::writer(std::string trace, size_t flush_interval, size_t snaplen)
  : flush_interval_{flush_interval}, snaplen_{snaplen}, trace_{std::move(trace)} {
}

writer::~writer() {
  if (dumper_)
    ::pcap_dump_close(dumper_);
  if (pcap_)
    ::pcap_close(pcap_);
}

caf::error writer::write(const table_slice& slice) {
  if (!pcap_) {
#ifdef PCAP_TSTAMP_PRECISION_NANO
    pcap_ = ::pcap_open_dead_with_tstamp_precision(DLT_RAW, snaplen_,
                                                   PCAP_TSTAMP_PRECISION_NANO);
#else
    pcap_ = ::pcap_open_dead(DLT_RAW, snaplen_);
#endif
    if (!pcap_)
      return make_error(ec::format_error, "failed to open pcap handle");
    dumper_ = ::pcap_dump_open(pcap_, trace_.c_str());
    if (!dumper_)
      return make_error(ec::format_error, "failed to open pcap dumper");
  }
  if (!congruent(slice.layout(), pcap_packet_type)
      && !congruent(slice.layout(), pcap_packet_type_community_id))
    return make_error(ec::format_error, "invalid pcap packet type");
  // TODO: consider iterating in natural order for the slice.
  for (size_t row = 0; row < slice.rows(); ++row) {
    auto payload_field = slice.at(row, 6);
    auto& payload = caf::get<view<std::string>>(payload_field);
    // Make PCAP header.
    ::pcap_pkthdr header;
    auto ns_field = slice.at(row, 0);
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
    ::pcap_dump(reinterpret_cast<uint8_t*>(dumper_), &header,
                reinterpret_cast<const uint8_t*>(std::launder(payload.data())));
  }
  if (++total_packets_ % flush_interval_ == 0)
    if (auto r = flush(); !r)
      return r.error();
  return caf::none;
}

caf::expected<void> writer::flush() {
  if (!dumper_)
    return make_error(ec::format_error, "pcap dumper not open");
  VAST_DEBUG(this, "flushes at packet", total_packets_);
  if (::pcap_dump_flush(dumper_) == -1)
    return make_error(ec::format_error, "failed to flush");
  return caf::no_error;
}

const char* writer::name() const {
  return "pcap-writer";
}

} // namespace pcap
} // namespace format
} // namespace vast
