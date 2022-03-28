//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

namespace fixtures {

using namespace vast;

struct events {
  events();

  /// Maximum size of all generated slices.
  static constexpr size_t slice_size = 8;

  // TODO: remove these entirely; all operations should be on table slices.
  static std::vector<table_slice> zeek_conn_log;
  static std::vector<table_slice> zeek_dns_log;
  static std::vector<table_slice> zeek_http_log;
  static std::vector<table_slice> random;

  static std::vector<table_slice> suricata_alert_log;
  static std::vector<table_slice> suricata_dns_log;
  static std::vector<table_slice> suricata_fileinfo_log;
  static std::vector<table_slice> suricata_flow_log;
  static std::vector<table_slice> suricata_http_log;
  static std::vector<table_slice> suricata_netflow_log;
  static std::vector<table_slice> suricata_stats_log;

  static vast::module suricata_module;

  static std::vector<table_slice> zeek_conn_log_full;

  /// 10000 ascending integer values, starting at 0.
  static std::vector<table_slice> ascending_integers;

  /// 10000 integer values, alternating between 0 and 1.
  static std::vector<table_slice> alternating_integers;

  template <class... Ts>
  static std::vector<std::vector<data>> make_rows(Ts... xs) {
    return {std::vector<data>{data{std::move(xs)}}...};
  }

  auto take(const std::vector<table_slice>& xs, size_t n) {
    VAST_ASSERT(n <= xs.size());
    auto first = xs.begin();
    auto last = first + n;
    return std::vector<table_slice>(first, last);
  }
};

} // namespace fixtures
