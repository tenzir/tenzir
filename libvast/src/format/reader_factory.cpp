//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/reader_factory.hpp"

#include "vast/config.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/json/default_selector.hpp"
#include "vast/format/json/suricata_selector.hpp"
#include "vast/format/json/zeek_selector.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/syslog.hpp"
#include "vast/format/zeek.hpp"
#include "vast/logger.hpp"

#if VAST_ENABLE_PCAP
#  include "vast/format/pcap.hpp"
#endif

#if VAST_ENABLE_ARROW
#  include "vast/format/arrow.hpp"
#endif

namespace vast {

template <class Reader>
caf::expected<std::unique_ptr<format::reader>>
make_reader(const caf::settings& options) {
  using istream_ptr = std::unique_ptr<std::istream>;
  if constexpr (std::is_constructible_v<Reader, caf::settings, istream_ptr>) {
    auto in = detail::make_input_stream(options);
    if (!in)
      return in.error();
    return std::make_unique<Reader>(options, std::move(*in));
  } else {
    return std::make_unique<Reader>(options);
  }
}

template <class Reader, class ReaderS,
          class Defaults = typename Reader::defaults>
caf::expected<std::unique_ptr<format::reader>>
make_json_reader(const caf::settings& options) {
  return make_reader<Reader, Defaults>(options);
}

void factory_traits<format::reader>::initialize() {
  using namespace format;
  using fac = factory<reader>;
  fac::add("csv", make_reader<csv::reader>);
  fac::add("json",
           make_reader<format::json::reader<format::json::default_selector>>);
  fac::add("suricata",
           make_reader<format::json::reader<format::json::suricata_selector>>);
  fac::add("syslog", make_reader<syslog::reader>);
  fac::add("zeek", make_reader<zeek::reader>);
  fac::add("zeek-json",
           make_reader<format::json::reader<format::json::zeek_selector>>);
#if VAST_ENABLE_PCAP
  fac::add("pcap", make_reader<pcap::reader>);
#endif
}

} // namespace vast
