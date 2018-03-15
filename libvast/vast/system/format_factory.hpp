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

#ifndef FORMAT_FACTORY_HPP 
#define FORMAT_FACTORY_HPP

#include <string>
#include <functional>

#include <caf/local_actor.hpp>

#include "vast/system/source.hpp"
//#include "vast/system/spawn.hpp"

#include "vast/format/bgpdump.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/bro.hpp"
#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

class format_factory {
public:
  // TODO: document me
  template <class Format>
  using format_factory_function = std::function<expected<Format>(caf::message&)>;

  using actor_factory_function
    = std::function<caf::actor(caf::local_actor*, caf::message&)>;

  struct default_args {
    default_args(caf::message& args) 
      : args(args) {
      parse_result = args.extract_opts(
        {{"read,r", "path to input where to read events from", input},
         {"schema,s", "path to alternate schema", schema_file},
         {"uds,d", "treat -r as listening UNIX domain socket", uds}});
    }

    ~default_args() {
      // Ensure that, we update the parameter list such that it no longer
      // contains the command line options that we have used
      args = parse_result.remainder;
    }

    caf::message& args;
    std::string input = "-"s;
    std::string schema_file;
    bool uds = false;
    caf::message::cli_res parse_result;
  };

  // TODO: create cpp file
  format_factory() {
    auto mrt_factory = [=](caf::message& args) {
      default_args d(args);
      auto in = detail::make_input_stream(d.input, d.uds);
      return vast::format::mrt::reader{std::move(*in)};
    };
    add_reader<vast::format::mrt::reader>("mrt", std::move(mrt_factory));
#ifdef VAST_HAVE_PCAP
    auto pcap_factory
      = [=](caf::message& args) -> expected<format::pcap::reader> {
      default_args d(args);
      auto flow_max = uint64_t{1} << 20;
      auto flow_age = 60u;
      auto flow_expiry = 10u;
      auto cutoff = std::numeric_limits<size_t>::max();
      auto pseudo_realtime = int64_t{0};
      d.parse_result = d.parse_result.remainder.extract_opts({
        {"cutoff,c", "skip flow packets after this many bytes", cutoff},
        {"flow-max,m", "number of concurrent flows to track", flow_max},
        {"flow-age,a", "max flow lifetime before eviction", flow_age},
        {"flow-expiry,e", "flow table expiration interval", flow_expiry},
        {"pseudo-realtime,p", "factor c delaying trace packets by 1/c",
         pseudo_realtime}
      });
      if (!d.parse_result.error.empty())
        return make_error(ec::syntax_error, d.parse_result.error);
      return format::pcap::reader{d.input,  cutoff,      flow_max,
              flow_age, flow_expiry, pseudo_realtime};
    };
    add_reader<vast::format::pcap::reader>("pcap", std::move(pcap_factory));
#endif
  }

  template <class Reader>
  bool add_reader(const std::string& format,
                  format_factory_function<Reader> make_reader) {
    auto factory = [=](caf::local_actor* self, caf::message& args) {
      auto reader = make_reader(args);
      if (reader)
        return self->spawn(source<Reader>, std::move(*reader));
      else
        return caf::actor{};
    };
    return readers_.try_emplace(format, std::move(factory)).second;
  }

  template <class Writer>
  bool add_writer(const std::string&, format_factory_function<Writer>) {
    // TODO: implement me
  }

  expected<actor_factory_function> reader(const std::string& format) {
    if (auto it = readers_.find(format); it != readers_.end())
      return it->second;
    return make_error(ec::syntax_error, "invalid format:", format);
  }

  expected<actor_factory_function> writer(const std::string& format) {
    if (auto it = writers_.find(format); it != writers_.end())
      return it->second;
    return make_error(ec::syntax_error, "invalid format:", format);
  }
private:
  std::unordered_map<std::string, actor_factory_function> readers_;
  std::unordered_map<std::string, actor_factory_function> writers_;
};


} // namespace vast::system::format

#endif
