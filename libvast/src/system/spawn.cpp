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

#include "vast/system/spawn.hpp"

#include <iterator>

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>
#include <caf/expected.hpp>
#include <caf/send.hpp>
#include <caf/string_algorithms.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/test.hpp"
#include "vast/query_options.hpp"
#include "vast/schema.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/node.hpp"
#include "vast/system/profiler.hpp"
#include "vast/system/replicated_store.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/source.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif // VAST_HAVE_PCAP

// Save ourself repeated lines of code that either declare a local variable or
// return with an error.
#define UNBOX_VAR(var_name, expr)                                              \
  std::remove_reference_t<decltype(*(expr))> var_name;                         \
  if (auto maybe = expr; !maybe)                                               \
    return std::move(maybe.error());                                           \
  else                                                                         \
    var_name = std::move(*maybe);

using caf::actor;
using caf::get_if;
using caf::get_or;
using caf::infinite;
using caf::local_actor;

using namespace std::chrono_literals;
using namespace vast::binary_byte_literals;

// Convenience alias for the system defaults namespace.
namespace sd = vast::defaults::system;

// Convenience alias for the command defaults namespace.
namespace cd = vast::defaults::command;

namespace vast::system {

namespace {

// Attempts to parse [args.first, args.last) as vast::expression and
// returns a normalized and validated version of that expression on success.
caf::expected<expression> normalized_and_valided(spawn_arguments& args) {
  if (args.empty())
    return make_error(ec::syntax_error, "no query expression given");
  if (auto e = to<expression>(caf::join(args.first, args.last, " ")); !e)
    return std::move(e.error());
  else
    return normalize_and_validate(*e);
}

// Attemps to read a schema file and parse its content. Can either 1) return
// nothing if the user didn't specifiy a schema file, 2) produce a valid
// schema, or 3) run into an error.
caf::expected<caf::optional<schema>> read_schema(spawn_arguments& args) {
  auto schema_file_ptr = get_if<std::string>(&args.options, "global.schema");
  if (!schema_file_ptr)
    return caf::optional<schema>{caf::none};
  if (auto str = load_contents(*schema_file_ptr); !str)
    return std::move(str.error());
  else if (auto result = to<schema>(*str); !result)
    return std::move(result.error());
  else
    return caf::optional<schema>{std::move(*result)};
}

} // namespace <anonymous>

maybe_actor spawn_archive(local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  auto mss = args.opt("global.max-segment-size", sd::max_segment_size);
  auto segments = args.opt("global.segments", sd::segments);
  mss *= 1_MiB;
  auto a = self->spawn(archive, args.dir / args.label, segments, mss);
  return caf::actor_cast<actor>(a);
}

maybe_actor spawn_exporter(node_actor* self, spawn_arguments& args) {
  // Parse given expression.
  UNBOX_VAR(expr, normalized_and_valided(args));
  // Parse query options.
  auto query_opts = no_query_options;
  if (args.opt("global.continuous", false))
    query_opts = query_opts + continuous;
  if (args.opt("global.historical", false))
    query_opts = query_opts + historical;
  if (args.opt("global.unified", false))
    query_opts = unified;
  // Default to historical if no options provided.
  if (query_opts == no_query_options)
    query_opts = historical;
  auto exp = self->spawn(exporter, std::move(expr), query_opts);
  // Setting max-events to 0 means infinite.
  auto max_events = args.opt("global.events", uint64_t{0});
  if (max_events > 0)
    caf::anon_send(exp, extract_atom::value, max_events);
  else
    caf::anon_send(exp, extract_atom::value);
  // Send the running IMPORTERs to the EXPORTER if it handles a continous query.
  if (has_continuous_option(query_opts)) {
    self->request(self->state.tracker, infinite, get_atom::value).then(
      [=](registry& reg) mutable {
        VAST_DEBUG(self, "looks for importers");
        auto& local = reg.components[self->state.name];
        const std::string wanted = "importer";
        std::vector<actor> importers;
        for (auto& [component, state] : local)
          if (std::equal(wanted.begin(), wanted.end(), component.begin()))
            importers.push_back(state.actor);
        if (!importers.empty())
          self->send(exp, importer_atom::value, std::move(importers));
      }
    );
  }
  return exp;
}

maybe_actor spawn_importer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  // FIXME: Notify exporters with a continuous query.
  return self->spawn(importer, args.dir / args.label,
                     caf::get_or(self->system().config(),
                                 "vast.table-slice-size",
                                 sd::table_slice_size));
}

maybe_actor spawn_index(local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  return self->spawn(index, args.dir / args.label,
                     args.opt("global.max-events", sd::max_partition_size),
                     args.opt("global.max-parts", sd::max_in_mem_partitions),
                     args.opt("global.taste-parts", sd::taste_partitions),
                     args.opt("global.max_queries", sd::num_collectors));
}

maybe_actor spawn_metastore(local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  auto id = args.opt("global.id", raft::server_id{0});
  // Bring up the consensus module.
  auto consensus = self->spawn(raft::consensus, args.dir / "consensus");
  self->monitor(consensus);
  if (id != 0)
    anon_send(consensus, id_atom::value, id);
  anon_send(consensus, run_atom::value);
  // Spawn the store on top.
  auto s = self->spawn(replicated_store<std::string, data>, consensus);
  s->attach_functor(
    [=](const error&) {
      anon_send_exit(consensus, caf::exit_reason::user_shutdown);
    }
  );
  return caf::actor_cast<actor>(s);
}

maybe_actor spawn_profiler(local_actor* self, spawn_arguments& args) {
#ifdef VAST_HAVE_GPERFTOOLS
  VAST_UNUSED(self, args);
  return make_error(ec::unspecified, "not compiled with gperftools");
#else // VAST_HAVE_GPERFTOOLS
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  auto resolution = args.opt("global.resolution", size_t{1});
  auto secs = std::chrono::seconds(resolution);
  auto prof = self->spawn(profiler, args.dir / args.label, secs);
  if (args.opt("global.cpu", false))
    anon_send(prof, start_atom::value, cpu_atom::value);
  if (args.opt("global.heap", false))
    anon_send(prof, start_atom::value, heap_atom::value);
  return prof;
#endif // VAST_HAVE_GPERFTOOLS
}

namespace {

template <class Reader, class... Ts>
maybe_actor spawn_generic_source(local_actor* self, spawn_arguments& args,
                                 Ts&&... writer_args) {
  UNBOX_VAR(expr, normalized_and_valided(args));
  UNBOX_VAR(sch, read_schema(args));
  UNBOX_VAR(out,
            detail::make_output_stream(args.opt("global.write", cd::write_path),
                                       args.opt("uds", false)));
  Reader reader{std::forward<Ts>(writer_args)...};
  auto src = self->spawn(default_source<Reader>, std::move(reader));
  caf::anon_send(src, std::move(expr));
  if (sch)
    caf::anon_send(src, put_atom::value, std::move(*sch));
  return src;
}

} // namespace <anonymous>

maybe_actor spawn_pcap_source(caf::local_actor* self, spawn_arguments& args) {
#ifndef VAST_HAVE_PCAP
  VAST_UNUSED(self, args);
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  return spawn_generic_source<format::pcap::reader>(
    self, args, args.opt("global.read", cd::read_path),
    args.opt("global.cutoff", cd::cutoff),
    args.opt("global.flow-max", cd::max_flows),
    args.opt("global.flow-age", cd::max_flow_age),
    args.opt("global.flow-expiry", cd::flow_expiry),
    args.opt("global.pseudo-realtime", cd::pseudo_realtime_factor));
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_test_source(caf::local_actor* self, spawn_arguments& args) {
  using reader_type = format::test::reader;
  UNBOX_VAR(sch, read_schema(args));
  // The test source only generates events out of thin air and thus accepts no
  // source expression.
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  reader_type reader{args.opt("global.seed", size_t{0}),
                     args.opt("global.events", size_t{100})};
  auto src = self->spawn(default_source<reader_type>, std::move(reader));
  if (sch)
    caf::anon_send(src, put_atom::value, std::move(*sch));
  return src;
}

maybe_actor spawn_bro_source(caf::local_actor* self, spawn_arguments& args) {
  UNBOX_VAR(in,
            detail::make_input_stream(args.opt("global.read", cd::read_path),
                                      args.opt("global.uds", false)));
  return spawn_generic_source<format::bro::reader>(self, args, std::move(in));
}

maybe_actor spawn_bgpdump_source(caf::local_actor* self,
                                 spawn_arguments& args) {
  UNBOX_VAR(in,
            detail::make_input_stream(args.opt("global.read", cd::read_path),
                                      args.opt("global.uds", false)));
  return spawn_generic_source<format::bgpdump::reader>(self, args,
                                                       std::move(in));
}

maybe_actor spawn_mrt_source(caf::local_actor* self, spawn_arguments& args) {
  UNBOX_VAR(in,
            detail::make_input_stream(args.opt("global.read", cd::read_path),
                                      args.opt("global.uds", false)));
  return spawn_generic_source<format::mrt::reader>(self, args, std::move(in));
}

maybe_actor spawn_pcap_sink(local_actor* self, spawn_arguments& args) {
#ifndef VAST_HAVE_PCAP
  VAST_UNUSED(self, args);
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  format::pcap::writer writer{args.opt("global.write", cd::write_path),
                              args.opt("global.flush", size_t{0})};
  return self->spawn(sink<format::pcap::writer>, std::move(writer), 0u);
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_bro_sink(local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  format::bro::writer writer{args.opt("global.write", "-")};
  return self->spawn(sink<format::bro::writer>, std::move(writer), 0u);
}

namespace {

template <class Writer>
maybe_actor spawn_generic_sink(local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return make_error(ec::syntax_error, "unexpected argument(s)");
  UNBOX_VAR(out, detail::make_output_stream(args.opt("global.write", "-"),
                                            args.opt("uds", false)));
  return self->spawn(sink<Writer>, Writer{std::move(out)}, 0u);
}

} // namespace <anonymous>

maybe_actor spawn_ascii_sink(local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::ascii::writer>(self, args);
}

maybe_actor spawn_csv_sink(local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::csv::writer>(self, args);
}

maybe_actor spawn_json_sink(local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::json::writer>(self, args);
}

} // namespace vast::system
