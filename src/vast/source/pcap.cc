#include "vast/source/pcap.h"

#include <netinet/in.h>
#include "vast/event.h"
#include "vast/detail/packet_type.h"
#include "vast/util/byte_swap.h"

namespace vast {
namespace source {

pcap::pcap(caf::actor sink, path trace)
  : synchronous<pcap>{std::move(sink)},
    trace_{std::move(trace)},
    packet_type_{detail::make_packet_type()}
{
}

pcap::~pcap()
{
  if (pcap_)
    ::pcap_close(pcap_);
}

result<event> pcap::extract()
{
  if (! pcap_ && ! done_)
  {
    if (trace_ != "-" && ! exists(trace_))
    {
      done_ = true;
      quit(exit::error);
      return error{"no such file: ", trace_};
    }

    char buf[PCAP_ERRBUF_SIZE];
    pcap_ = ::pcap_open_offline_with_tstamp_precision(
        trace_.str().c_str(), PCAP_TSTAMP_PRECISION_NANO, buf);

    if (! pcap_)
    {
      std::string err{buf};
      done_ = true;
      quit(exit::error);
      return error{"failed to open pcap file ", trace_, ": ", err};
    }
  }

  uint8_t const* data;
  auto r = ::pcap_next_ex(pcap_, &packet_header_, &data);

  if (r == -2)
  {
    done_ = true;
    return {};
  }
  if (r == -1)
  {
    std::string err{::pcap_geterr(pcap_)};
    pcap_ = nullptr;
    return error{"failed to get next packet: ", err};
  }
  if (r == 0)
  {
    return error{"timeout should not occur with traces"};
  }

  record network;
  auto layer3 = data + 14;
  uint8_t const* layer4 = nullptr;
  uint8_t layer4_proto = 0;
  auto layer2_type = *reinterpret_cast<uint16_t const*>(data + 12);
  switch (util::byte_swap<network_endian, host_endian>(layer2_type))
  {
    default:
      return error{"unsupported network layer"};
    case 0x0800:
      {
        if (packet_header_->len < 14 + 20)
          return error{"IPv4 header too short"};

        size_t header_size = (*layer3 & 0x0f) * 4;
        if (header_size < 20)
          return error{"IPv4 header too short: ", header_size, " bytes"};

        network.emplace_back(*reinterpret_cast<uint8_t const*>(layer3 + 8));

        auto orig_h = reinterpret_cast<uint32_t const*>(layer3 + 12);
        network.emplace_back(
            vast::address{orig_h, address::ipv4, address::network});

        auto resp_h = reinterpret_cast<uint32_t const*>(layer3 + 16);
        network.emplace_back(
            vast::address{resp_h, address::ipv4, address::network});

        layer4_proto = *(layer3 + 9);
        layer4 = layer3 + header_size;
      }
      break;
    case 0x86dd:
      {
        if (packet_header_->len < 14 + 40)
          return error{"IPv6 header too short"};

        network.emplace_back(*reinterpret_cast<uint8_t const*>(layer3 + 7));

        auto orig_h = reinterpret_cast<uint32_t const*>(layer3 + 8);
        network.emplace_back(
            vast::address{orig_h, address::ipv6, address::network});

        auto resp_h = reinterpret_cast<uint32_t const*>(layer3 + 24);
        network.emplace_back(
            vast::address{resp_h, address::ipv6, address::network});

        layer4_proto = *(layer3 + 6);
        layer4 = layer3 + 40;
      }
      break;
  }

  record meta;
  meta.emplace_back(std::move(network));

  record transport;
  if (layer4_proto == IPPROTO_TCP)
  {
    assert(layer4);
    auto orig_p = *reinterpret_cast<uint16_t const*>(layer4);
    orig_p = util::byte_swap<network_endian, host_endian>(orig_p);
    auto resp_p = *reinterpret_cast<uint16_t const*>(layer4 + 2);
    resp_p = util::byte_swap<network_endian, host_endian>(resp_p);

    record ports;
    ports.emplace_back(port{orig_p, port::tcp});
    ports.emplace_back(port{resp_p, port::tcp});

    transport.emplace_back(std::move(ports));
    transport.emplace_back(nil);
    transport.emplace_back(nil);
  }
  else if (layer4_proto == IPPROTO_UDP)
  {
    assert(layer4);
    auto orig_p = *reinterpret_cast<uint16_t const*>(layer4);
    orig_p = util::byte_swap<network_endian, host_endian>(orig_p);
    auto resp_p = *reinterpret_cast<uint16_t const*>(layer4 + 2);
    resp_p = util::byte_swap<network_endian, host_endian>(resp_p);

    record ports;
    ports.emplace_back(port{orig_p, port::udp});
    ports.emplace_back(port{resp_p, port::udp});

    transport.emplace_back(nil);
    transport.emplace_back(std::move(ports));
    transport.emplace_back(nil);
  }
  else if (layer4_proto == IPPROTO_ICMP)
  {
    assert(layer4);
    auto t = *reinterpret_cast<uint8_t const*>(layer4);
    auto c = *reinterpret_cast<uint8_t const*>(layer4 + 1);

    transport.emplace_back(nil);
    transport.emplace_back(nil);
    transport.emplace_back(record{t, c});
  }
  else
  {
    transport.emplace_back(nil);
    transport.emplace_back(nil);
    transport.emplace_back(nil);
  }

  meta.emplace_back(std::move(transport));

  record packet;
  packet.emplace_back(std::move(meta));

  auto str = reinterpret_cast<char const*>(data + 14);
  packet.emplace_back(std::string{str, packet_header_->len - 14});

  event e{std::move(packet), packet_type_};

  auto s = time_duration::seconds(packet_header_->ts.tv_sec);
  auto ns = time_duration::nanoseconds(packet_header_->ts.tv_usec);
  auto t = s + ns;
  e.timestamp(t);

  return std::move(e);
}

bool pcap::done() const
{
  return ! pcap_ || done_;
}

std::string pcap::describe() const
{
  return "pcap-source";
}

} // namespace source
} // namespace vast
