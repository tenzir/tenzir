#include <caf/all.hpp>

#include <caf/detail/scope_guard.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/error.hpp"
#include "vast/query_options.hpp"

#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/pcap.hpp"
#include "vast/format/test.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn.hpp"

#include "vast/detail/make_io_stream.hpp"

using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

expected<actor> spawn_source(local_actor* self, options& opts) {
  if (opts.params.empty())
    return make_error(ec::syntax_error, "missing format");
  auto& format = opts.params.get_as<std::string>(0);
  auto source_args = opts.params.drop(1);
  // Parse common parameters first.
  auto input = "-"s;
  std::string schema_file;
  auto r = source_args.extract_opts({
    {"read,r", "path to input where to read events from", input},
    {"schema,s", "path to alternate schema", schema_file},
    {"uds,u", "treat -r as listening UNIX domain socket"}
  });
  auto grd = caf::detail::make_scope_guard([&] { opts.params = r.remainder; });
  actor src;
  // Parse source-specific parameters, if any.
  if (format == "pcap") {
#ifndef VAST_HAVE_PCAP
    return make_error(ec::unspecified, "not compiled with pcap support");
#else
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
    format::pcap::reader reader{input, cutoff, flow_max, flow_age, flow_expiry,
                                pseudo_realtime};
    src = self->spawn(source<format::pcap::reader>, std::move(reader));
#endif
  } else if (format == "bro" || format == "bgpdump") {
    auto in = detail::make_input_stream(input, r.opts.count("uds") > 0);
    if (!in)
      return in.error();
    if (format == "bro") {
      format::bro::reader reader{std::move(*in)};
      src = self->spawn(source<format::bro::reader>, std::move(reader));
    } else /* if (format == "bgpdump") */ {
      format::bgpdump::reader reader{std::move(*in)};
      src = self->spawn(source<format::bgpdump::reader>, std::move(reader));
    }
  } else if (format == "test") {
    auto seed = size_t{0};
    auto id = event_id{0};
    auto n = uint64_t{100};
    r = r.remainder.extract_opts({
      {"seed,s", "the PRNG seed", seed},
      {"events,n", "number of events to generate", n},
      {"id,i", "the base event ID", id}
    });
    if (!r.error.empty())
      return make_error(ec::syntax_error, r.error);
    format::test::reader reader{seed, n, id};
    src = self->spawn(source<format::test::reader>, std::move(reader));
    // Since the test source doesn't consume any data and only generates
    // events out of thin air, we use the input channel to specify the schema.
    schema_file = input;
  } else {
    return make_error(ec::syntax_error, "invalid format:", format);
  }
  // Supply an alternate schema, if requested.
  if (!schema_file.empty()) {
    auto str = load_contents(schema_file);
    if (!str)
      return str.error();
    auto sch = to<schema>(*str);
    if (!sch)
      return sch.error();
    // Send anonymously, since we can't process the reply here.
    anon_send(src, put_atom::value, std::move(*sch));
  }
  return src;
}

} // namespace system
} // namespace vast
