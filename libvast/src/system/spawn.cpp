#include <caf/all.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/data.hpp"
#include "vast/error.hpp"
#include "vast/query_options.hpp"

#include "vast/detail/posix.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/fdostream.hpp"

#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/pcap.hpp"
#include "vast/format/test.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/profiler.hpp"
#include "vast/system/spawn.hpp"
#include "vast/system/replicated_store.hpp"

using namespace std::chrono_literals;
using namespace caf;

namespace vast {
namespace system {

expected<actor> spawn_archive(local_actor* self, options& opts) {
  auto mss = size_t{128};
  auto segments = size_t{10};
  auto r = opts.params.extract_opts({
    {"segments,s", "number of cached segments", segments},
    {"max-segment-size,m", "maximum segment size in MB", mss}
  });
  opts.params = r.remainder;
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto a = self->spawn(archive, opts.dir / opts.label, segments, mss);
  return actor_cast<actor>(a);
}

expected<actor> spawn_exporter(local_actor* self, options& opts) {
  auto limit = uint64_t{0};
  auto r = opts.params.extract_opts({
    {"continuous,c", "marks a query as continuous"},
    {"historical,h", "marks a query as historical"},
    {"unified,u", "marks a query as unified"},
    {"limit,l", "limit the number of results", limit},
  }, nullptr, true);
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  if (r.remainder.empty())
    return make_error(ec::syntax_error, "no query expression given");
  auto str = r.remainder.get_as<std::string>(0);
  for (auto i = 1u; i < r.remainder.size(); ++i)
    str += ' ' + r.remainder.get_as<std::string>(i);
  // Parse expression.
  auto expr = to<expression>(str);
  if (!expr)
    return expr.error();
  *expr = normalize(*expr);
  // Parse query options.
  auto query_opts = no_query_options;
  if (r.opts.count("continuous") > 0)
    query_opts = query_opts + continuous;
  if (r.opts.count("historical") > 0)
    query_opts = query_opts + historical;
  if (r.opts.count("unified") > 0)
    query_opts = unified;
  if (query_opts == no_query_options)
    return make_error(ec::syntax_error, "missing query options (-h, -c, -u)");
  auto exp = self->spawn(exporter, std::move(*expr), query_opts);
  if (limit > 0)
    anon_send(exp, extract_atom::value, limit);
  else
    anon_send(exp, extract_atom::value);
  return exp;
}

expected<actor> spawn_importer(local_actor* self, options& opts) {
  auto ids = size_t{128};
  auto r = opts.params.extract_opts({
    {"ids,n", "number of initial IDs to request", ids},
  });
  opts.params = r.remainder;
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(importer, opts.dir / opts.label, ids);
}

expected<actor> spawn_index(local_actor* self, options& opts) {
  size_t max_events = 1 << 20;
  size_t max_parts = 10;
  size_t taste_parts = 5;
  auto r = opts.params.extract_opts({
    {"max-events,e", "maximum events per partition", max_events},
    {"max-parts,p", "maximum number of in-memory partitions", max_parts},
    {"taste-parts,p", "number of immediately scheduled partitions", taste_parts}
  });
  opts.params = r.remainder;
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(index, opts.dir / opts.label, max_events, max_parts,
                     taste_parts);
}

expected<actor> spawn_metastore(local_actor* self, options& opts) {
  auto id = raft::server_id{0};
  auto r = opts.params.extract_opts({
    {"id,i", "the server ID of the consensus module", id},
  });
  opts.params = r.remainder;
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  // Bring up the consensus module.
  auto consensus = self->spawn(raft::consensus, opts.dir / "consensus");
  self->monitor(consensus);
  if (id != 0)
    anon_send(consensus, id_atom::value, id);
  anon_send(consensus, run_atom::value);
  // Spawn the store on top.
  auto s = self->spawn(replicated_store<std::string, data>, consensus);
  s->attach_functor(
    [=](const error&) {
      anon_send_exit(consensus, exit_reason::user_shutdown);
    }
  );
  return actor_cast<actor>(s);
}

#ifdef VAST_HAVE_GPERFTOOLS
expected<actor> spawn_profiler(local_actor* self, options& opts) {
  auto resolution = 1u;
  auto r = opts.params.extract_opts({
    {"cpu,c", "start the CPU profiler"},
    {"heap,h", "start the heap profiler"},
    {"resolution,r", "seconds between measurements", resolution}
  });
  opts.params = r.remainder;
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto secs = std::chrono::seconds(resolution);
  auto prof = self->spawn(profiler, opts.dir / opts.label, secs);
  if (r.opts.count("cpu") > 0)
    anon_send(prof, start_atom::value, cpu_atom::value);
  if (r.opts.count("heap") > 0)
    anon_send(prof, start_atom::value, heap_atom::value);
  return prof;
}
#else
expected<actor> spawn_profiler(local_actor*, options&) {
  return make_error(ec::unspecified, "not compiled with gperftools");
}
#endif

} // namespace system
} // namespace vast
