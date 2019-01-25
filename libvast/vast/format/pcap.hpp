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

#pragma once

#include <pcap.h>

#include <chrono>
#include <unordered_map>
#include <random>

#include "vast/address.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/operators.hpp"
#include "vast/expected.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/format/writer.hpp"
#include "vast/port.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"

namespace vast {

class event;

namespace format {
namespace pcap {

struct connection : detail::equality_comparable<connection> {
  address src;
  address dst;
  port sport;
  port dport;

  friend bool operator==(const connection& lhs, const connection& rhs) {
    return lhs.src == rhs.src
      && lhs.dst == rhs.dst
      && lhs.sport == rhs.sport
      && lhs.dport == rhs.dport;
  }
};

template <class Hasher>
void hash_append(Hasher& h, const connection& c) {
  hash_append(h, c.src, c.dst, c.sport.number(), c.dport.number());
}

} // namespace pcap
} // namespace format
} // namespace vast

namespace std {

template <>
struct hash<vast::format::pcap::connection> {
  size_t operator()(const vast::format::pcap::connection& c) const {
    return vast::uhash<vast::xxhash>{}(c);
  }
};

} // namespace std

namespace vast {
namespace format {
namespace pcap {

/// A PCAP reader.
class reader : public single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a PCAP reader.
  /// @param input The name of the interface or trace file.
  /// @param cutoff The number of bytes to keep per flow.
  /// @param max_flows The maximum number of flows to keep state for.
  /// @param max_age The number of seconds to wait since the last seen packet
  ///                before evicting the corresponding flow.
  /// @param expire_interval The number of seconds between successive expire
  ///                        passes over the flow table.
  /// @param pseudo_realtime The inverse factor by which to delay packets. For
  ///                        example, if 5, then for two packets spaced *t*
  ///                        seconds apart, the source will sleep for *t/5*
  ///                        seconds.
  reader(caf::atom_value id, std::string input, uint64_t cutoff = -1,
         size_t max_flows = 100000, size_t max_age = 60,
         size_t expire_interval = 10, int64_t pseudo_realtime = 0);

  ~reader();

  caf::expected<void> schema(vast::schema sch) override;

  caf::expected<vast::schema> schema() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  struct connection_state {
    uint64_t bytes;
    uint64_t last;
  };

  pcap_t* pcap_ = nullptr;
  type packet_type_;
  std::unordered_map<connection, connection_state> flows_;
  uint64_t cutoff_;
  size_t max_flows_;
  std::mt19937 generator_;
  uint64_t max_age_;
  uint64_t expire_interval_;
  uint64_t last_expire_ = 0;
  timestamp last_timestamp_ = timestamp::min();
  int64_t pseudo_realtime_;
  std::string input_;
};

/// A PCAP writer.
class writer : public format::writer {
public:
  writer() = default;

  /// Constructs a PCAP writer.
  /// @param trace The path where to write the trace file.
  /// @param flush_interval The number of packets after which to flush to disk.
  writer(std::string trace, size_t flush_interval = -1);

  ~writer();

  caf::expected<void> write(const event& e) override;

  caf::expected<void> flush() override;

  void cleanup() override;

  const char* name() const override;

private:
  vast::schema schema_;
  size_t flush_interval_ = 0;
  size_t total_packets_ = 0;
  pcap_t* pcap_ = nullptr;
  pcap_dumper_t* dumper_ = nullptr;
  std::string trace_;
};

} // namespace pcap
} // namespace format
} // namespace vast
