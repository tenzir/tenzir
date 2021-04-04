//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/address.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/operators.hpp"
#include "vast/flow.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/format/writer.hpp"
#include "vast/port.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"

#include <caf/expected.hpp>
#include <caf/optional.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
#include <unordered_map>

// Forward declarations from libpcap.
struct pcap;
struct pcap_dumper;
struct pcap_stat;

namespace vast::format::pcap {

struct pcap_close_wrapper {
  void operator()(struct pcap* handle) const noexcept;
};

struct pcap_dump_close_wrapper {
  void operator()(struct pcap_dumper* handle) const noexcept;
};

struct pcap_stat_delete_wrapper {
  void operator()(struct pcap_stat* handle) const noexcept;
};

/// A PCAP reader.
class reader : public single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a PCAP reader.
  /// @param options Additional options.
  /// @param in Input stream (unused). Pass filename via options instead.
  reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                       = nullptr);

  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = default;
  reader& operator=(reader&&) noexcept = default;

  void reset(std::unique_ptr<std::istream> in);

  ~reader() override = default;

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

  vast::system::report status() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

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

  std::unique_ptr<struct pcap, pcap_close_wrapper> pcap_ = nullptr;

  std::unordered_map<flow, flow_state> flows_;
  std::filesystem::path input_;
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
  std::unique_ptr<struct pcap_stat, pcap_stat_delete_wrapper> last_stats_
    = nullptr;
  mutable size_t discard_count_;
};

/// A PCAP writer.
class writer : public format::writer {
public:
  using defaults = vast::defaults::export_::pcap;

  /// Constructs a PCAP writer.
  /// @param options The configuration options for the writer.
  explicit writer(const caf::settings& options);

  ~writer() override = default;

  using format::writer::write;

  caf::error write(const table_slice& slice) override;

  caf::expected<void> flush() override;

  const char* name() const override;

private:
  vast::schema schema_;
  size_t flush_interval_ = 0;
  size_t snaplen_ = 65535;
  size_t total_packets_ = 0;
  std::unique_ptr<struct pcap, pcap_close_wrapper> pcap_ = nullptr;
  std::unique_ptr<struct pcap_dumper, pcap_dump_close_wrapper> dumper_
    = nullptr;
  std::filesystem::path trace_;
};

} // namespace vast::format::pcap
