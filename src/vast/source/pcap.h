#ifndef VAST_SOURCE_PCAP_H
#define VAST_SOURCE_PCAP_H

#include <pcap.h>
#include "vast/file_system.h"
#include "vast/source/synchronous.h"

namespace vast {
namespace source {

/// A file source that reads PCAP traces.
class pcap : public synchronous<pcap>
{
public:
  /// Constructs a file source.
  /// @param sink The actor to send the generated events to.
  /// @param trace The name of the trace file.
  pcap(caf::actor sink, path trace);

  ~pcap();

  result<event> extract();
  bool done() const;
  std::string describe() const final;

private:
  path trace_;
  bool done_ = false;
  type packet_type_;
  pcap_t* pcap_ = nullptr;
  pcap_pkthdr* packet_header_ = nullptr;
};

} // namespace source
} // namespace vast

#endif
