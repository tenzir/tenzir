#include "vast/actor/sink/pcap.h"
#include "vast/detail/pcap_packet_type.h"
#include "vast/concept/printable/vast/filesystem.h"

namespace vast {
namespace sink {

pcap_state::pcap_state(local_actor* self)
  : state{self, "pcap-sink"} {
}

pcap_state::~pcap_state() {
  if (pcap_dumper)
    ::pcap_dump_close(pcap_dumper);
  if (pcap_handle)
    ::pcap_close(pcap_handle);
}

bool pcap_state::process(event const& e) {
  if (!pcap_handle) {
    if (trace != "-" && !exists(trace)) {
      VAST_ERROR_AT(self, "cannot locate file:", trace);
      self->quit(exit::error);
      return false;
    }
#ifdef PCAP_TSTAMP_PRECISION_NANO
    pcap_handle = ::pcap_open_dead_with_tstamp_precision(DLT_RAW, 65535,
                                                   PCAP_TSTAMP_PRECISION_NANO);
#else
    pcap_handle = ::pcap_open_dead(DLT_RAW, 65535);
#endif
    if (!pcap_handle) {
      VAST_ERROR_AT(self, "failed to open pcap handle");
      self->quit(exit::error);
      return false;
    }
    pcap_dumper = ::pcap_dump_open(pcap_handle, trace.str().c_str());
    if (!pcap_dumper) {
      VAST_ERROR_AT(self, "failed to open pcap dumper for", trace);
      self->quit(exit::error);
      return false;
    }
    if (auto t = schema.find("vast::packet")) {
      if (congruent(packet_type, *t)) {
        VAST_VERBOSE_AT(self, "prefers type in schema over default type");
        packet_type = *t;
      } else {
        VAST_WARN_AT(self, "ignores incongruent schema type:", t->name());
      }
    }
  }
  if (e.type() != packet_type) {
    VAST_ERROR_AT(self, "cannot process non-packet event:", e.type());
    self->quit(exit::error);
    return false;
  }
  auto r = get<record>(e);
  assert(r);
  assert(r->size() == 2);
  auto data = get<std::string>((*r)[1]);
  assert(data);
  // Make PCAP header.
  ::pcap_pkthdr header;
  auto ns = e.timestamp().time_since_epoch().count();
  header.ts.tv_sec = ns / 1000000000;
#ifdef PCAP_TSTAMP_PRECISION_NANO
  header.ts.tv_usec = ns % 1000000000;
#else
  ns /= 1000;
  header.ts.tv_usec = ns % 1000000;
#endif
  header.caplen = data->size();
  header.len = data->size();
  // Dump packet.
  ::pcap_dump(reinterpret_cast<uint8_t*>(pcap_dumper), &header,
              reinterpret_cast<uint8_t const*>(data->c_str()));
  if (++total_packets % flush_packets == 0 
      && ::pcap_dump_flush(pcap_dumper) == -1) {
    VAST_ERROR_AT(self, "failed to flush at packet", total_packets);
    self->quit(exit::error);
    return false;
  }
  return true;
}

behavior pcap(stateful_actor<pcap_state>* self,
              schema sch, path trace, size_t flush_packets) {
  self->state.schema = std::move(sch);
  self->state.trace = std::move(trace);
  self->state.packet_type = detail::pcap_packet_type;
  self->state.flush_packets = flush_packets;
  return make(self);
}

} // namespace sink
} // namespace vast
