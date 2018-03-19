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


#include "vast/format/bgpdump.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/test.hpp"
#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif

#include "vast/detail/make_io_stream.hpp"

#include "vast/system/format_factory.hpp"

namespace vast::system {

namespace {

template <class Reader>
expected<Reader> create_reader(caf::message& args) {
  format_factory::reader_default_args defaults;
  auto r = defaults.parse(args);
  auto in = detail::make_input_stream(defaults.input, defaults.uds);
  if (!in)
    return in.error();
  args = r.remainder;
  return Reader{std::move(*in)};
}

} // namespace <anonymous>

caf::message::cli_res
format_factory::reader_default_args::parse(caf::message& args) {
  return args.extract_opts(
    {{"read,r", "path to input where to read events from", input},
     {"uds,d", "treat -r as listening UNIX domain socket", uds}});
}

format_factory::format_factory() {
    using pcap_reader = vast::format::pcap::reader;
    using test_reader = vast::format::test::reader;
    using bro_reader = vast::format::bro::reader;
    using mrt_reader = vast::format::mrt::reader;
    using bgpdump_reader = vast::format::bgpdump::reader;
#ifndef VAST_HAVE_PCAP
    auto pcap_factory = 
      [=](caf::message&) -> expected<format::pcap::reader> {
        return make_error(ec::unspecified, "not compiled with pcap support");
      }
#else
    auto pcap_factory = 
      [=](caf::message& args) -> expected<format::pcap::reader> {
      reader_default_args defaults;
      auto r = defaults.parse(args);
      auto flow_max = uint64_t{1} << 20;
      auto flow_age = 60u;
      auto flow_expiry = 10u;
      auto cutoff = std::numeric_limits<size_t>::max();
      auto pseudo_realtime = int64_t{0};
      r = r.remainder.extract_opts({
        {"cutoff,c", "skip flow packets after this many bytes", cutoff},
        {"flow-max,m", "number of concurrent flows to track", flow_max},
        {"flow-age,a", "max flow lifetime before eviction", flow_age},
        {"flow-expiry,e", "flow table expiration interval", flow_expiry},
        {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
         pseudo_realtime}
      });
      if (!r.error.empty())
        return make_error(ec::syntax_error, r.error);
      args = r.remainder;
      return format::pcap::reader{defaults.input, cutoff, flow_max,
                                  flow_age, flow_expiry, pseudo_realtime};
    };
#endif
    auto test_factory = 
      [=](caf::message& args) -> expected<format::test::reader> {
      auto seed = size_t{0};
      auto id = event_id{0};
      auto n = uint64_t{100};
      auto r = args.extract_opts({
        {"seed,s", "the PRNG seed", seed},
        {"events,n", "number of events to generate", n},
        {"id,i", "the base event ID", id}
      });
      // FIXME: The former comment is not correct anymore:
      // Since the test source doesn't consume any data and only generates
      // events out of thin air, we use the input channel to specify the schema.
      if (!r.error.empty())
        return make_error(ec::syntax_error, r.error);
      return format::test::reader{seed, n, id};
    };
    auto mrt_factory = create_reader<mrt_reader>;
    auto bro_factory = create_reader<bro_reader>;
    auto bgpdump_factory = create_reader<bgpdump_reader>;
    add_reader<pcap_reader>("pcap", std::move(pcap_factory));
    add_reader<test_reader>("test", std::move(test_factory));
    add_reader<mrt_reader>("mrt", std::move(mrt_factory));
    add_reader<bro_reader>("bro", std::move(bro_factory));
    add_reader<bgpdump_reader>("bgpdump", std::move(bgpdump_factory));
}

expected<format_factory::actor_factory_function>
format_factory::reader(const std::string& format) {
  if (auto it = readers_.find(format); it != readers_.end())
    return it->second;
  return make_error(ec::syntax_error, "invalid format:", format);
}

expected<format_factory::actor_factory_function>
format_factory::writer(const std::string& format) {
  if (auto it = writers_.find(format); it != writers_.end())
    return it->second;
  return make_error(ec::syntax_error, "invalid format:", format);
}

} // namespace vast::system::format
