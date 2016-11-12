#include <netinet/in.h>

#include <thread>

#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/pcap_packet_type.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"

#include "vast/format/pcap.hpp"

namespace vast {
namespace format {

pcap::pcap() : packet_type_{detail::pcap_packet_type} {
  // nop
}

pcap::~pcap() {
  if (pcap_)
    ::pcap_close(pcap_);
}

void pcap::init(std::string input, uint64_t cutoff, size_t max_flows,
                size_t max_age, size_t expire_interval,
                int64_t pseudo_realtime) {
  input_ = std::move(input);
  cutoff_ = cutoff;
  max_flows_ = max_flows;
  max_age_ = max_age;
  expire_interval_ = expire_interval;
  pseudo_realtime_ = pseudo_realtime;
}

maybe<event> pcap::extract() {
  char buf[PCAP_ERRBUF_SIZE]; // for errors.
  if (!pcap_) {
    // Determine interfaces.
    pcap_if_t* iface;
    if (::pcap_findalldevs(&iface, buf) == -1)
      return make_error(ec::format_error,
                        "failed to enumerate interfaces: ", buf);
    for (auto i = iface; i != nullptr; i = i->next)
      if (input_ == i->name) {
        pcap_ = ::pcap_open_live(i->name, 65535, 1, 1000, buf);
        if (!pcap_) {
          ::pcap_freealldevs(iface);
          return make_error(ec::format_error, "failed to open interface ",
                            input_, ": ", buf);
        }
        if (pseudo_realtime_ > 0) {
          pseudo_realtime_ = 0;
          VAST_WARNING(name(), "ignores pseudo-realtime in live mode");
        }
        VAST_INFO(name(), "listens on interface " << i->name);
        break;
      }
    ::pcap_freealldevs(iface);
    if (!pcap_) {
      if (input_ != "-" && !exists(input_))
        return make_error(ec::format_error, "no such file: ", input_);
#ifdef PCAP_TSTAMP_PRECISION_NANO
      pcap_ = ::pcap_open_offline_with_tstamp_precision(
        input_.c_str(), PCAP_TSTAMP_PRECISION_NANO, buf);
#else
      pcap_ = ::pcap_open_offline(input_.c_str(), buf);
#endif
      if (!pcap_) {
        flows_.clear();
        return make_error(ec::format_error, "failed to open pcap file ",
                          input_, ": ", std::string{buf});
      }

      VAST_INFO(name(), "reads trace from", input_);
      if (pseudo_realtime_ > 0)
        VAST_INFO(name(), "uses pseudo-realtime factor 1/" << pseudo_realtime_);
    }
    VAST_INFO(name(), "cuts off flows after", cutoff_,
                    "bytes in each direction");
    VAST_INFO(name(), "keeps at most", max_flows_, "concurrent flows");
    VAST_INFO(name(), "evicts flows after", max_age_ << "s of inactivity");
    VAST_INFO(name(), "expires flow table every", expire_interval_ << "s");
  }
  uint8_t const* data;
  auto r = ::pcap_next_ex(pcap_, &packet_header_, &data);
  if (r == 0)
    return {}; // Attempt to fetch next packet timed out.
  if (r == -2) {
    return make_error(ec::end_of_input, "reached end of trace");
  }
  if (r == -1) {
    auto err = std::string{::pcap_geterr(pcap_)};
    ::pcap_close(pcap_);
    pcap_ = nullptr;
    return make_error(ec::format_error, "failed to get next packet: ", err);
  }
  // Parse packet.
  connection conn;
  auto packet_size = packet_header_->len - 14;
  auto layer3 = data + 14;
  uint8_t const* layer4 = nullptr;
  uint8_t layer4_proto = 0;
  auto layer2_type = *reinterpret_cast<uint16_t const*>(data + 12);
  uint64_t payload_size = packet_size;
  switch (detail::to_host_order(layer2_type)) {
    default:
      return {}; // Skip all non-IP packets.
    case 0x0800: {
      if (packet_header_->len < 14 + 20)
        return make_error(ec::format_error, "IPv4 header too short");
      size_t header_size = (*layer3 & 0x0f) * 4;
      if (header_size < 20)
        return make_error(ec::format_error, "IPv4 header too short: ",
                          header_size, " bytes");
      auto orig_h = reinterpret_cast<uint32_t const*>(layer3 + 12);
      auto resp_h = reinterpret_cast<uint32_t const*>(layer3 + 16);
      conn.src = {orig_h, address::ipv4, address::network};
      conn.dst = {resp_h, address::ipv4, address::network};
      layer4_proto = *(layer3 + 9);
      layer4 = layer3 + header_size;
      payload_size -= header_size;
    } break;
    case 0x86dd: {
      if (packet_header_->len < 14 + 40)
        return make_error(ec::format_error, "IPv6 header too short");
      auto orig_h = reinterpret_cast<uint32_t const*>(layer3 + 8);
      auto resp_h = reinterpret_cast<uint32_t const*>(layer3 + 24);
      conn.src = {orig_h, address::ipv4, address::network};
      conn.dst = {resp_h, address::ipv4, address::network};
      layer4_proto = *(layer3 + 6);
      layer4 = layer3 + 40;
      payload_size -= 40;
    } break;
  }
  if (layer4_proto == IPPROTO_TCP) {
    VAST_ASSERT(layer4);
    auto orig_p = *reinterpret_cast<uint16_t const*>(layer4);
    auto resp_p = *reinterpret_cast<uint16_t const*>(layer4 + 2);
    orig_p = detail::to_host_order(orig_p);
    resp_p = detail::to_host_order(resp_p);
    conn.sport = {orig_p, port::tcp};
    conn.dport = {resp_p, port::tcp};
    auto data_offset = *reinterpret_cast<uint8_t const*>(layer4 + 12) >> 4;
    payload_size -= data_offset * 4;
  } else if (layer4_proto == IPPROTO_UDP) {
    VAST_ASSERT(layer4);
    auto orig_p = *reinterpret_cast<uint16_t const*>(layer4);
    auto resp_p = *reinterpret_cast<uint16_t const*>(layer4 + 2);
    orig_p = detail::to_host_order(orig_p);
    resp_p = detail::to_host_order(resp_p);
    conn.sport = {orig_p, port::udp};
    conn.dport = {resp_p, port::udp};
    payload_size -= 8;
  } else if (layer4_proto == IPPROTO_ICMP) {
    VAST_ASSERT(layer4);
    auto message_type = *reinterpret_cast<uint8_t const*>(layer4);
    auto message_code = *reinterpret_cast<uint8_t const*>(layer4 + 1);
    conn.sport = {message_type, port::icmp};
    conn.dport = {message_code, port::icmp};
    payload_size -= 8; // TODO: account for variable-size data.
  }
  // Parse packet timestamp
  uint64_t packet_time = packet_header_->ts.tv_sec;
  if (last_expire_ == 0)
    last_expire_ = packet_time;
  auto i = flows_.find(conn);
  if (i == flows_.end())
    i = flows_.insert(std::make_pair(conn, connection_state{0, packet_time}))
          .first;
  else
    i->second.last = packet_time;
  auto& flow_size = i->second.bytes;
  if (flow_size == cutoff_)
    return {};
  if (flow_size + payload_size <= cutoff_) {
    flow_size += payload_size;
  } else {
    // Trim the last packet so that it fits.
    packet_size -= flow_size + payload_size - cutoff_;
    flow_size = cutoff_;
  }
  // Evict all elements that have been inactive for a while.
  if (packet_time - last_expire_ > expire_interval_) {
    last_expire_ = packet_time;
    auto i = flows_.begin();
    while (i != flows_.end())
      if (packet_time - i->second.last > max_age_)
        i = flows_.erase(i);
      else
        ++i;
  }
  // If the flow table gets too large, we evict a random element.
  if (!flows_.empty() && flows_.size() % max_flows_ == 0) {
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
    for (size_t n = 0; n < offset; ++n)
      ++begin;
    flows_.erase(begin->first);
  }
  // Assemble packet.
  vector packet;
  vector meta;
  meta.emplace_back(std::move(conn.src));
  meta.emplace_back(std::move(conn.dst));
  meta.emplace_back(std::move(conn.sport));
  meta.emplace_back(std::move(conn.dport));
  packet.emplace_back(std::move(meta));
  // We start with the network layer and skip the link layer.
  auto str = reinterpret_cast<char const*>(data + 14);
  packet.emplace_back(std::string{str, packet_size});
  using namespace std::chrono;
  auto secs = seconds(packet_header_->ts.tv_sec);
  auto ts = timestamp{duration_cast<interval>(secs)};
#ifdef PCAP_TSTAMP_PRECISION_NANO
  ts += nanoseconds(packet_header_->ts.tv_usec);
#else
  ts += microseconds(packet_header_->ts.tv_usec);
#endif
  if (pseudo_realtime_ > 0) {
    if (ts < last_timestamp_) {
      VAST_WARNING(name(), "encountered non-monotonic packet timestamps:",
                   ts.time_since_epoch().count(), '<',
                   last_timestamp_.time_since_epoch().count());
    }
    if (last_timestamp_ != timestamp::min()) {
      auto delta = ts - last_timestamp_;
      std::this_thread::sleep_for(delta / pseudo_realtime_);
    }
    last_timestamp_ = ts;
  }
  event e{{std::move(packet), packet_type_}};
  e.timestamp(ts);
  return e;
}

void pcap::schema(vast::schema const& sch) {
  auto t = sch.find("vast::packet");
  if (!t) {
    VAST_ERROR(name(), "did not find type vast::packet in given schema");
    return;
  }
  if (!congruent(packet_type_, *t)) {
    VAST_WARNING(name(), "ignores incongruent schema type:", t->name());
    return;
  }
  VAST_INFO(name(), "prefers type in schema over default type");
  packet_type_ = *t;
}

schema pcap::schema() const {
  vast::schema sch;
  sch.add(packet_type_);
  return sch;
}

const char* pcap::name() const {
  return "pcap";
}

} // namespace format
} // namespace vast
