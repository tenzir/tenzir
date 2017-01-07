#ifndef VAST_SYSTEM_SOURCE_HPP
#define VAST_SYSTEM_SOURCE_HPP

#include "vast/logger.hpp"

#include <caf/actor_pool.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/schema.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"

namespace vast {
namespace system {

#if 0
/// The *Reader* concept.
struct Reader {
  Reader();

  maybe<result> read();

  expected<void> schema(vast::schema&);

  expected<vast::schema> schema() const;

  char const* name() const;
};
#endif

/// The source state.
/// @tparam Reader The reader type, which must model the *Reader* concept.
template <class Reader>
struct source_state {
  static constexpr size_t max_batch_size = 1 << 20;
  uint64_t batch_size = 65536;
  std::vector<event> events;
  std::chrono::steady_clock::time_point start;
  accountant_type accountant;
  caf::actor sink;
  Reader reader;
  char const* name;
};

/// An event producer.
/// @tparam Reader The concrete source implementation.
/// @param self The actor handle.
/// @param reader The reader instance.
template <class Reader>
caf::behavior
source(caf::stateful_actor<source_state<Reader>>* self, Reader&& reader) {
  using namespace caf;
  using namespace std::chrono;
  self->state.reader = std::move(reader);
  self->state.name = self->state.reader.name();
  auto eu = self->system().dummy_execution_unit();
  self->state.sink = actor_pool::make(eu, actor_pool::round_robin());
  // Register the accountant, if available.
  auto acc = self->system().registry().get(accountant_atom::value);
  if (acc) {
    VAST_DEBUG(self, "registers accountant", acc);
    self->state.accountant = actor_cast<accountant_type>(acc);
  }
  self->set_exit_handler(
    [=](const exit_msg& msg) {
      if (self->state.accountant) {
        timestamp now = system_clock::now();
        self->send(self->state.accountant, "source.end", now);
      }
      self->send(self->state.sink, sys_atom::value, delete_atom::value);
      self->send(self->state.sink, msg);
      self->quit(msg.reason);
    }
  );
  return {
    [=](run_atom) {
      if (self->state.accountant && self->current_sender() != self) {
        timestamp now = system_clock::now();
        self->send(self->state.accountant, "source.start", now);
      }
      // Extract events until the source has exhausted its input or until we
      // have completed a batch.
      auto start = steady_clock::now();
      auto done = false;
      while (self->state.events.size() < self->state.batch_size) {
        auto e = self->state.reader.read();
        if (e) {
          self->state.events.push_back(std::move(*e));
        } else if (e.empty()) {
          continue; // Try again.
        } else {
          if (e == ec::parse_error) {
            VAST_WARNING(self->system().render(e.error()));
            continue; // Just skip bogous events.
          } else if (e == ec::end_of_input) {
            VAST_INFO(self->system().render(e.error()));
          } else {
            VAST_ERROR(self->system().render(e.error()));
          }
          done = true;
          break;
        }
      }
      auto stop = steady_clock::now();
      // Ship the current batch.
      if (!self->state.events.empty()) {
        auto runtime = stop - start;
        auto unit = duration_cast<microseconds>(runtime).count();
        auto rate = self->state.events.size() * 1e6 / unit;
        auto events = uint64_t{self->state.events.size()};
        VAST_INFO(self, "produced", events, "events in", runtime,
                  '(' << size_t(rate), "events/sec)");
        if (self->state.accountant) {
          auto rt = duration_cast<timespan>(runtime);
          self->send(self->state.accountant, "source.batch.runtime", rt);
          self->send(self->state.accountant, "source.batch.events", events);
          self->send(self->state.accountant, "source.batch.rate", rate);
        }
        self->send(self->state.sink, std::move(self->state.events));
        self->state.events = {};
        self->state.events.reserve(self->state.batch_size);
        // FIXME: if we do not give the stdlib implementation a hint to yield
        // here, this actor can monopolize all available resources. In
        // particular, we encountered a scenario where it prevented the BASP
        // broker from a getting a chance to operate, thereby queuing up
        // all event batches locally and running out of memory, as opposed to
        // sending them out as soon as possible. This yield fix temporarily
        // works around a deeper issue in CAF, which needs to be addressed in
        // the future.
        std::this_thread::yield();
      }
      if (done)
        self->send_exit(self, exit_reason::normal);
      else
        self->send(self, run_atom::value);
    },
    [=](batch_atom, uint64_t batch_size) {
      if (batch_size > source_state<Reader>::max_batch_size) {
        VAST_WARNING(self, "ignores too large batch size:", batch_size);
        return;
      }
      VAST_DEBUG(self, "sets batch size to", batch_size);
      self->state.batch_size = batch_size;
      self->state.events.reserve(batch_size);
    },
    [=](get_atom, schema_atom) -> result<schema> {
      auto sch = self->state.reader.schema();
      if (sch)
        return *sch;
      return sch.error();
    },
    [=](put_atom, schema const& sch) -> result<void> {
      auto r = self->state.reader.schema(sch);
      if (r)
        return {};
      return r.error();
    },
    [=](sink_atom, actor const& sink) {
      VAST_ASSERT(sink);
      VAST_DEBUG(self, "registers sink", sink);
      self->send(self->state.sink, sys_atom::value, put_atom::value, sink);
    },
  };
}

} // namespace system
} // namespace vast

#endif
