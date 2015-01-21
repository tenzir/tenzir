#ifndef VAST_ACTOR_SINK_PCAP_H
#define VAST_ACTOR_SINK_PCAP_H

#include <pcap.h>
#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/actor/sink/base.h"
#include "vast/detail/packet_type.h"

namespace vast {
namespace sink {

/// A file source that reads PCAP traces.
class pcap : public base<pcap>
{
public:
  /// Constructs a file source.
  /// @param sch The schema containing the packet type.
  /// @param trace The name of the trace file to construct.
  /// @param flush Number of packets after which to flush to disk.
  pcap(schema sch, path trace, size_t flush = 10000);

  ~pcap();

  bool process(event const& e);
  std::string name() const override;

private:
  schema schema_;
  path trace_;
  type packet_type_;
  size_t flush_;
  size_t total_packets_ = 0;
  pcap_t* pcap_ = nullptr;
  pcap_dumper_t* pcap_dumper_ = nullptr;
};

} // namespace sink
} // namespace vast

#endif
