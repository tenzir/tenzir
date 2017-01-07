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
#include "vast/system/consensus.hpp"
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

expected<actor> spawn_archive(local_actor* self, options opts) {
  auto mss = size_t{128};
  auto segments = size_t{10};
  auto r = opts.params.extract_opts({
    {"segments,s", "number of cached segments", segments},
    {"max-segment-size,m", "maximum segment size in MB", mss}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto a = self->spawn(archive, opts.dir / opts.label, segments, mss);
  return actor_cast<actor>(a);
}

expected<actor> spawn_exporter(local_actor* self, options opts) {
  std::string expr_str;
  auto r = opts.params.extract_opts({
    {"expression,e", "the query expression", expr_str},
    {"continuous,c", "marks a query as continuous"},
    {"historical,h", "marks a query as historical"},
    {"unified,u", "marks a query as unified"},
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  // Parse expression.
  auto expr = to<expression>(expr_str);
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
    return make_error(ec::syntax_error, "got query w/o options (-h, -c, -u)");
  return self->spawn(exporter, std::move(*expr), query_opts);
}

expected<actor> spawn_importer(local_actor* self, options opts) {
  auto ids = size_t{128};
  auto r = opts.params.extract_opts({
    {"ids,n", "number of initial IDs to request", ids},
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(importer, opts.dir / opts.label, ids);
}

expected<actor> spawn_index(local_actor* self, options opts) {
  uint64_t max_events = 1 << 20;
  uint64_t passive = 10;
  auto r = opts.params.extract_opts({
    {"events,e", "maximum events per partition", max_events},
    {"passive,p", "maximum number of passive partitions", passive}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  return self->spawn(index, opts.dir / opts.label, max_events, passive);
}

expected<actor> spawn_metastore(local_actor* self, options opts) {
  auto id = raft::server_id{0};
  auto r = opts.params.extract_opts({
    {"id,i", "the static ID of the consensus module", id}
  });
  if (id == 0)
    return make_error(ec::unspecified, "invalid server ID: 0");
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
  if (!r.error.empty())
    return make_error(ec::syntax_error, r.error);
  auto consensus = self->spawn(raft::consensus, opts.dir / opts.label, id);
  anon_send(consensus, run_atom::value);
  auto s = self->spawn(replicated_store<std::string, data>, consensus, 10000ms);
  return actor_cast<actor>(s);
}

#ifdef VAST_HAVE_GPERFTOOLS
expected<actor> spawn_profiler(local_actor* self, options opts) {
  auto resolution = 1u;
  auto r = opts.params.extract_opts({
    {"cpu,c", "start the CPU profiler"},
    {"heap,h", "start the heap profiler"},
    {"resolution,r", "seconds between measurements", resolution}
  });
  if (!r.remainder.empty()) {
    auto invalid = r.remainder.get_as<std::string>(0);
    return make_error(ec::syntax_error, "invalid syntax", invalid);
  }
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
expected<actor> spawn_profiler(local_actor*, options) {
  return make_error(ec::unspecified, "not compiled with gperftools");
}
#endif

} // namespace system
} // namespace vast
