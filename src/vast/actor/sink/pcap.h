#ifndef VAST_ACTOR_SINK_PCAP_H
#define VAST_ACTOR_SINK_PCAP_H

#include <pcap.h>

#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/type.h"
#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

/// A file source that reads PCAP traces.
struct pcap_state : state {
  pcap_state(local_actor* self);
  ~pcap_state();

  bool process(event const& e) override;

  vast::schema schema;
  path trace;
  type packet_type;
  size_t flush_packets;
  size_t total_packets = 0;
  pcap_t* pcap_handle = nullptr;
  pcap_dumper_t* pcap_dumper = nullptr;
};

/// Spawns a PCAP source.
/// @param self The actor handle.
/// @param sch The schema containing the packet type.
/// @param trace The name of the trace file to construct.
/// @param flush_packets Number of packets after which to flush to disk.
behavior pcap(stateful_actor<pcap_state>* self,
              schema sch, path trace, size_t flush_packets);

} // namespace sink
} // namespace vast

#endif
