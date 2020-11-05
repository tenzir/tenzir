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
#include <caf/optional.hpp>

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
  /// @param in Input stream (unused). Pass filename via options instead.
  reader(caf::atom_value id, const caf::settings& options,
         std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  ~reader();

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

  vast::system::report status() const override;

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
  std::unordered_map<flow, flow_state> flows_;
  std::string input_;
  caf::optional<std::string> interface_;
  uint64_t cutoff_;
  size_t max_flows_;
  std::mt19937 generator_;
  uint64_t max_age_;
  uint64_t expire_interval_;
  uint64_t last_expire_ = 0;
  time last_timestamp_ = time::min();
  int64_t pseudo_realtime_;
  size_t snaplen_;
  bool community_id_;
  type packet_type_;
  double drop_rate_threshold_;
  mutable pcap_stat last_stats_;
  mutable size_t discard_count_;
};

/// A PCAP writer.
class writer : public format::writer {
public:
  using defaults = vast::defaults::export_::pcap;

  writer() = default;

  /// Constructs a PCAP writer.
  /// @param trace The path where to write the trace file.
  /// @param flush_interval The number of packets after which to flush to disk.
  /// @param snaplen The snapshot length in bytes.
  writer(std::string trace, size_t flush_interval = -1, size_t snaplen = 65535);

  ~writer();

  using format::writer::write;

  caf::error write(const table_slice_ptr& slice) override;

  caf::expected<void> flush() override;

  const char* name() const override;

private:
  vast::schema schema_;
  size_t flush_interval_ = 0;
  size_t snaplen_ = 65535;
  size_t total_packets_ = 0;
  pcap_t* pcap_ = nullptr;
  pcap_dumper_t* dumper_ = nullptr;
  std::string trace_;
};

} // namespace pcap
} // namespace format
} // namespace vast
