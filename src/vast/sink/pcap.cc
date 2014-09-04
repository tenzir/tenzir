#include "vast/sink/pcap.h"

#include "vast/detail/packet_type.h"

namespace vast {
namespace sink {

pcap::pcap(path trace, size_t flush)
  : trace_{std::move(trace)},
    packet_type_{detail::make_packet_type()},
    flush_{flush}
{
}

pcap::~pcap()
{
  if (pcap_dumper_)
    ::pcap_dump_close(pcap_dumper_);

  if (pcap_)
    ::pcap_close(pcap_);
}

bool pcap::process(event const& e)
{
  if (e.type() != packet_type_)
  {
    VAST_LOG_ACTOR_ERROR("cannot process non-packet event: " << e.type());
    quit(exit::error);
    return false;
  }

  if (! pcap_)
  {
    if (trace_ != "-" && ! exists(trace_))
    {
      VAST_LOG_ACTOR_ERROR("no such file: " << trace_);
      quit(exit::error);
      return false;
    }

    pcap_ = ::pcap_open_dead_with_tstamp_precision(
        DLT_RAW, 65535, PCAP_TSTAMP_PRECISION_NANO);

    if (! pcap_)
    {
      VAST_LOG_ACTOR_ERROR("failed to open pcap handle");
      quit(exit::error);
      return false;
    }

    pcap_dumper_ = ::pcap_dump_open(pcap_, trace_.str().c_str());
    if (! pcap_dumper_)
    {
      VAST_LOG_ACTOR_ERROR("failed to open pcap dumper for " << trace_);
      quit(exit::error);
      return false;
    }
  }

  auto r = get<record>(e);
  assert(r);
  assert(r->size() == 2);
  auto data = get<std::string>((*r)[1]);
  assert(data);

  ::pcap_pkthdr header;
  auto ns = e.timestamp().since_epoch().count();
  header.ts.tv_sec = ns / 1000000000;
  header.ts.tv_usec = ns % 1000000000;
  header.caplen = data->size();
  header.len = data->size();

  ::pcap_dump(reinterpret_cast<uint8_t*>(pcap_dumper_), &header,
              reinterpret_cast<uint8_t const*>(data->c_str()));

  if (++total_packets_ % flush_ == 0 && ::pcap_dump_flush(pcap_dumper_) == -1)
  {
    VAST_LOG_ACTOR_ERROR("failed to flush at packet " << total_packets_);
    quit(exit::error);
    return false;
  }

  return true;
}

std::string pcap::describe() const
{
  return "pcap-sink";
}

} // namespace sink
} // namespace vast
