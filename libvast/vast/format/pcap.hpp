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

#include "vast/address.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/operators.hpp"
#include "vast/flow.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/format/writer.hpp"
#include "vast/fwd.hpp"
#include "vast/port.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"

#include <caf/expected.hpp>

#include <chrono>
#include <pcap.h>
#include <random>
#include <unordered_map>

namespace vast {
namespace format {
namespace pcap {

/// A PCAP reader.
class reader : public single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a PCAP reader.
  /// @param id The ID for table slice type to build.
  /// @param options Additional options.
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
  reader(caf::atom_value id, const caf::settings& options, std::string input,
         uint64_t cutoff = -1, size_t max_flows = 100000, size_t max_age = 60,
         size_t expire_interval = 10, int64_t pseudo_realtime = 0);

  ~reader();

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  struct flow_state {
    uint64_t bytes;
    uint64_t last;
    std::string community_id;
  };

  /// @returns either an existing state associated to `x` or a new state for
  ///          the flow.
  flow_state& state(const flow& x);

  /// @returns whether `true` if the flow remains active, `false` if the flow
  ///          reached the configured cutoff.
  bool update_flow(const flow& x, uint64_t packet_time, uint64_t payload_size);

  /// Evict all flows that have been inactive for the maximum age.
  void evict_inactive(uint64_t packet_time);

  /// Evicts random flows when exceeding the maximum configured flow count.
  void shrink_to_max_size();

  pcap_t* pcap_ = nullptr;
  type packet_type_;
  std::unordered_map<flow, flow_state> flows_;
  uint64_t cutoff_;
  size_t max_flows_;
  std::mt19937 generator_;
  uint64_t max_age_;
  uint64_t expire_interval_;
  uint64_t last_expire_ = 0;
  time last_timestamp_ = time::min();
  int64_t pseudo_realtime_;
  std::string input_;
};

/// A PCAP writer.
class writer : public format::writer {
public:
  using defaults = vast::defaults::export_::pcap;

  writer() = default;

  /// Constructs a PCAP writer.
  /// @param trace The path where to write the trace file.
  /// @param flush_interval The number of packets after which to flush to disk.
  writer(std::string trace, size_t flush_interval = -1);

  ~writer();

  using format::writer::write;

  caf::error write(const table_slice& slice) override;

  caf::expected<void> flush() override;

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
