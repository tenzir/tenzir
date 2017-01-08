#include <caf/all.hpp>

#include <caf/detail/scope_guard.hpp>

#include "vast/config.hpp"

#include "vast/format/ascii.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/pcap.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn.hpp"

#include "vast/detail/make_io_stream.hpp"

using namespace caf;

namespace vast {
namespace system {

expected<actor> spawn_sink(local_actor* self, options& opts) {
  if (opts.params.empty())
    return make_error(ec::syntax_error, "missing format");
  auto& format = opts.params.get_as<std::string>(0);
  auto sink_args = opts.params.drop(1);
  // Parse common parameters first.
  auto output = "-"s;
  auto schema_file = ""s;
  auto r = sink_args.extract_opts({
    {"write,w", "path to write events to", output},
    //{"schema,s", "alternate schema file", schema_file},
    {"uds,u", "treat -w as UNIX domain socket to connect to"}
  }, nullptr, true);
  auto grd = caf::detail::make_scope_guard([&] { opts.params = r.remainder; });
  actor snk;
  // Parse sink-specific parameters, if any.
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return make_error(ec::unspecified, "not compiled with pcap support");
#else
    auto flush = 10000u;
    r = r.remainder.extract_opts({
      {"flush,f", "flush to disk after this many packets", flush}
    });
    if (!r.error.empty())
      return make_error(ec::syntax_error, r.error);
    format::pcap::writer writer{output, flush};
    snk = self->spawn(sink<format::pcap::writer>, std::move(writer));
#endif
  } else if (format == "bro") {
    format::bro::writer writer{output};
    snk = self->spawn(sink<format::bro::writer>, std::move(writer));
  } else {
    auto out = detail::make_output_stream(output, r.opts.count("uds") > 0);
    if (!out)
      return out.error();
    if (format == "csv") {
      format::csv::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::csv::writer>, std::move(writer));
    } else if (format == "ascii") {
      format::ascii::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::ascii::writer>, std::move(writer));
    } else if (format == "json") {
      format::json::writer writer{std::move(*out)};
      snk = self->spawn(sink<format::json::writer>, std::move(writer));
    } else {
      return make_error(ec::syntax_error, "invalid format:", format);
    }
  }
  return snk;
}

} // namespace system
} // namespace vast
