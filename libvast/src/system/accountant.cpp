#include <ios>
#include <iomanip>

#include "vast/logger.hpp"

#include <caf/all.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/detail/coding.hpp"

#include "vast/system/accountant.hpp"

namespace vast {
namespace system {
namespace {

template <class Actor>
void init(Actor self, path const& filename) {
  if (!exists(filename.parent())) {
    auto t = mkdir(filename.parent());
    if (!t) {
      VAST_ERROR(self, to_string(t.error()));
      self->quit(t.error());
      return;
    }
  }
  VAST_DEBUG(self, "opens log file:", filename);
  auto& file = self->state.file;
  file.open(filename.str());
  if (!file.is_open()) {
    VAST_ERROR(self, "failed to open file:", filename);
    auto e = make_error(ec::filesystem_error, "failed to open file:", filename);
    self->quit(e);
    return;
  }
  file << "time\thost\tpid\taid\tkey\tvalue\n";
  if (!file)
    self->quit(make_error(ec::filesystem_error));
  // Kick off flush loop.
  self->send(self, flush_atom::value);
}

template <class Actor, class T>
void record(Actor self, std::string const& key, T x) {
  using namespace std::chrono;
  auto node = self->current_sender()->node();
  auto now = system_clock::now().time_since_epoch();
  auto ts = duration_cast<double_seconds>(now).count();
  self->state.file << std::fixed << std::showpoint << std::setprecision(6)
                    << ts << '\t' << std::hex;
  for (auto byte : node.host_id())
    self->state.file << static_cast<int>(byte);
  self->state.file
    << std::dec << '\t'
    << node.process_id() << '\t'
    << self->current_sender()->id() << '\t'
    << key << '\t'
    << std::setprecision(6) << x << '\n';
}

} // namespace <anonymous>

accountant_type::behavior_type accountant(
  accountant_type::stateful_pointer<accountant_state> self,
  path const& filename) {
  using namespace std::chrono;
  init(self, filename);
  return {
    [=](shutdown_atom) {
      self->state.file.flush();
      self->quit(caf::exit_reason::user_shutdown);
    },
    [=](flush_atom) {
      // Flush every 10 seconds.
      if (self->state.file)
        self->state.file.flush();
      if (self->current_sender() == self)
        self->delayed_send(self, seconds(10), flush_atom::value);
    },
    [=](std::string const& key, std::string const& value) {
      record(self, key, value);
    },
    // Helpers to avoid to_string(..) in sender context.
    [=](std::string const& key, timespan value) {
      auto us = duration_cast<microseconds>(value).count();
      record(self, key, us);
    },
    [=](std::string const& key, timestamp value) {
      auto us = duration_cast<microseconds>(value.time_since_epoch()).count();
      record(self, key, us);
    },
    [=](std::string const& key, int64_t value) {
      record(self, key, value);
    },
    [=](std::string const& key, uint64_t value) {
      record(self, key, value);
    },
    [=](std::string const& key, double value) {
      record(self, key, value);
    }
  };
}

} // namespace system
} // namespace vast
