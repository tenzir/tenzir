#include <caf/all.hpp>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/concept/printable/vast/key.hpp"
#include "vast/detail/assert.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/filesystem.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/offset.hpp"
#include "vast/save.hpp"
#include "vast/value_index.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/indexer.hpp"

using namespace caf;

namespace vast {
namespace system {
namespace {

struct value_indexer_state {
  path filename;
  vast::type type;
  std::unique_ptr<value_index> idx;
  value_index::size_type last_flush = 0;
  const char* name = "value-indexer";
};

// Wraps a value index into an actor.
template <class Extract>
behavior value_indexer(stateful_actor<value_indexer_state>* self,
                       path filename, type index_type, Extract extract) {
  self->state.type = std::move(index_type);
  self->state.filename = std::move(filename);
  if (exists(self->state.filename)) {
    // Materialize the index when encountering persistent state.
    detail::value_index_inspect_helper tmp{self->state.type, self->state.idx};
    auto result = load(self->state.filename, self->state.last_flush, tmp);
    if (!result) {
      VAST_ERROR(self, "failed to load bitmap index:",
                 self->system().render(result.error()));
      self->quit(result.error());
    } else {
      VAST_DEBUG(self, "loaded value index with offset",
                 self->state.idx->offset());
    }
  } else {
    // Otherwise construct a new one.
    self->state.idx = value_index::make(self->state.type);
    if (!self->state.idx)
      self->quit(make_error(ec::unspecified, "failed to construct index"));
  }
  return {
    [=](std::vector<event> const& events) {
      VAST_TRACE(self, "got", events.size(), "events");
      for (auto& e : events) {
        VAST_ASSERT(e.id() != invalid_event_id);
        if (auto data = extract(e)) {
          auto result = self->state.idx->push_back(*data, e.id());
          if (!result) {
            VAST_ERROR(self->system().render(result.error()));
            self->quit(result.error());
            break;
          }
        }
      }
    },
    [=](predicate const& pred) -> result<bitmap> {
      VAST_TRACE(self, "got predicate:", pred);
      return self->state.idx->lookup(pred.op, get<data>(pred.rhs));
    },
    [=](shutdown_atom) {
      // Flush index to disk.
      auto offset = self->state.idx->offset();
      if (offset == self->state.last_flush) {
        // Nothing to write.
        self->quit(exit_reason::user_shutdown);
        return;
      }
      // Create parent directory if it doesn't exist.
      auto dir = self->state.filename.parent();
      if (!exists(dir)) {
        auto result = mkdir(dir);
        if (!result) {
          self->quit(result.error());
          return;
        }
      }
      VAST_DEBUG(self, "flushes index ("
                 << (offset - self->state.last_flush) << '/' << offset,
                 "new/total bits)");
      self->state.last_flush = offset;
      detail::value_index_inspect_helper tmp{self->state.type, self->state.idx};
      auto result = save(self->state.filename, self->state.last_flush, tmp);
      if (result)
        self->quit(exit_reason::user_shutdown);
      else
        self->quit(result.error());
    },
  };
}

// In the current event indexing design, all indexers receive all events and
// pick the aspect of the event that's relevant to them. For event meta data
// indexers, every event is relevant. Event data indexers concern themselves
// only with a specific aspect of an event.

behavior time_indexer(stateful_actor<value_indexer_state>* self,
                      path const& p) {
  // TODO: add type attributes to tune index, e.g., for seconds granularity.
  auto t = timestamp_type{};
  auto extract = [](event const& e) { return optional<data>{e.timestamp()}; };
  return value_indexer(self, p, t, extract);
}

behavior type_indexer(stateful_actor<value_indexer_state>* self,
                      path const& p) {
  auto t = string_type{};
  auto extract = [](event const& e) { return optional<data>{e.type().name()}; };
  return value_indexer(self, p, t, extract);
}

// Indexes the data from non-record event type.
behavior flat_data_indexer(stateful_actor<value_indexer_state>* self,
                           path dir, type event_type) {
  auto extract = [=](event const& e) -> optional<data const&> {
    if (e.type() != event_type)
      return {};
    return e.data();
  };
  return value_indexer(self, dir, event_type, extract);
}

// Indexes a field of data from record event type.
behavior field_data_indexer(stateful_actor<value_indexer_state>* self,
                            path dir, type event_type, type value_type,
                            offset off) {
  auto extract = [=](event const& e) -> optional<data const&> {
    if (e.type() != event_type)
      return {};
    auto v = get_if<vector>(e.data());
    if (!v)
      return {};
    if (auto x = get(*v, off))
      return *x;
    // If there is no data at a given offset, it means that an intermediate
    // record is nil but we're trying to access a deeper field.
    static const auto nil_data = data{nil};
    return nil_data;
  };
  return value_indexer(self, dir, value_type, extract);
}

// Tests whether a type has a "skip" attribute.
bool skip(type const& t) {
  auto& attrs = t.attributes();
  auto pred = [](auto& x) { return x.key == "skip"; };
  return std::find_if(attrs.begin(), attrs.end(), pred) != attrs.end();
}

// Loads indexes for a predicate.
struct loader {
  using result_type = std::vector<actor>;

  template <class T>
  result_type operator()(T const&) {
    return {};
  }

  template <class T, class U>
  result_type operator()(T const&, U const&) {
    return {};
  }

  result_type operator()(disjunction const& d) {
    result_type result;
    for (auto& op : d) {
      auto x = visit(*this, op);
      result.insert(result.end(),
                    std::make_move_iterator(x.begin()),
                    std::make_move_iterator(x.end()));
    }
    return result;
  }

  result_type operator()(predicate const& p) {
    return visit(*this, p.lhs, p.rhs);
  }

  result_type operator()(attribute_extractor const& ex, data const& x) {
    result_type result;
    auto p = self->state.dir / "meta";
    if (ex.attr == "time") {
      VAST_ASSERT(is<timestamp>(x));
      p /= ex.attr;
      VAST_DEBUG(self, "loads time index at", p);
      auto& a = self->state.indexers[p];
      if (!a)
        a = self->spawn<monitored>(time_indexer, p);
      result.push_back(a);
    } else if (ex.attr == "type") {
      VAST_ASSERT(is<std::string>(x));
      p /= ex.attr;
      VAST_DEBUG(self, "loads type index at", p);
      auto& a = self->state.indexers[p];
      if (!a)
        a = self->spawn<monitored>(type_indexer, p);
      result.push_back(a);
    } else {
      VAST_WARNING(self, "got unsupported attribute:", ex.attr);
    }
    return result;
  }

  result_type operator()(data_extractor const& dx, data const&) {
    result_type result;
    if (dx.offset.empty()) {
      auto p = self->state.dir / "data";
      VAST_DEBUG(self, "loads value index for", self->state.event_type.name());
      auto& a = self->state.indexers[p];
      if (!a)
        a = self->spawn<monitored>(flat_data_indexer, p,
                                   self->state.event_type);
      result.push_back(a);
    } else {
      auto r = get<record_type>(dx.type);
      auto k = r.resolve(dx.offset);
      VAST_ASSERT(k);
      auto t = r.at(dx.offset);
      VAST_ASSERT(t);
      auto p = self->state.dir / "data";
      for (auto& x : *k)
        p /= x;
      VAST_DEBUG(self, "loads value index for", *k);
      auto& a = self->state.indexers[p];
      if (!a)
        a = self->spawn<monitored>(field_data_indexer, p, dx.type, *t,
                                   dx.offset);
      result.push_back(a);
    }
    return result;
  }

  stateful_actor<event_indexer_state>* self;
};

} // namespace <anonymous>

behavior event_indexer(stateful_actor<event_indexer_state>* self,
                       path dir, type event_type) {
  self->state.dir = dir;
  self->state.event_type = event_type;
  VAST_DEBUG(self, "operates for event", event_type);
  // If the directory doesn't exist yet, we're in "construction" mode,
  // where we spawn all indexers to be able to handle incoming events directly.
  // Otherwise we deal with a "frozen" indexer that only spawns indexers as
  // needed for answering queries.
  if (!exists(dir)) {
    VAST_DEBUG(self, "didn't find persistent state, spawning new indexers");
    // Spawn indexers for event meta data.
    auto p = dir / "meta" / "time";
    auto a = self->spawn<monitored>(time_indexer, p);
    self->state.indexers.emplace(p, a);
    p = dir / "meta" / "type";
    a = self->spawn<monitored>(type_indexer, p);
    self->state.indexers.emplace(p, a);
    // Spawn indexers for event data.
    if (skip(event_type)) {
      VAST_DEBUG(self, "skips event:", event_type);
    } else {
      auto r = get_if<record_type>(event_type);
      if (!r) {
        p = dir / "data";
        VAST_DEBUG(self, "spawns data indexer");
        a = self->spawn<monitored>(flat_data_indexer, p, event_type);
        self->state.indexers.emplace(p, a);
      } else {
        for (auto& f : record_type::each{*r}) {
          auto& value_type = f.trace.back()->type;
          if (skip(value_type)) {
            VAST_DEBUG(self, "skips record field:", f.key());
          } else {
            p = dir / "data";
            for (auto& k : f.key())
              p /= k;
            VAST_DEBUG(self, "spawns field indexer at offset", f.offset,
                       "with type", value_type);
            a = self->spawn<monitored>(field_data_indexer, p, event_type,
                                       value_type, f.offset);
            self->state.indexers.emplace(p, a);
          }
        }
      }
    }
  }
  // We monitor all indexers so that we can control the shutdown process
  // explicitly.
  auto remove_indexer = [=](auto& indexer) {
    auto i = std::find_if(self->state.indexers.begin(),
                          self->state.indexers.end(),
                          [&](auto& p) { return p.second == indexer; });
    VAST_ASSERT(i != self->state.indexers.end());
    self->state.indexers.erase(i);
  };
  self->set_down_handler(
    [=](down_msg const& msg) { remove_indexer(msg.source); }
  );
  return {
    [=](std::vector<event> const&) {
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& x : self->state.indexers)
        self->send(x.second, msg);
    },
    [=](predicate const& pred) {
      VAST_DEBUG(self, "got predicate:", pred);
      auto rp = self->make_response_promise<bitmap>();
      // For now, we require that the predicate is part of a normalized
      // expression, i.e., LHS an extractor type and RHS of type data.
      auto rhs = get_if<data>(pred.rhs);
      VAST_ASSERT(rhs);
      auto resolved = type_resolver{self->state.event_type}(pred);
      if (!resolved) {
        VAST_DEBUG(self, "failed to resolve predicate:",
                   self->system().render(resolved.error()));
        rp.deliver(resolved.error());
        return;
      }

      auto indexers = visit(loader{self}, *resolved);
      // Forward predicate to all available indexers.
      if (indexers.empty()) {
        VAST_DEBUG(self, "did not find matching indexers for", pred);
        rp.deliver(bitmap{});
        return;
      }
      VAST_DEBUG(self, "asks", indexers.size(), "indexers");
      // Manual map-reduce over the indexers.
      auto n = std::make_shared<size_t>(indexers.size());
      auto reducer = self->system().spawn([=]() mutable -> behavior {
        auto result = std::make_shared<bitmap>();
        return {
          [=](const bitmap& bm) mutable {
            if (!bm.empty())
              *result |= bm;
            if (--*n == 0)
              rp.deliver(std::move(*result));
          },
          [=](error& e) mutable {
            rp.deliver(std::move(e));
          }
        };
      });
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& x : indexers)
        send_as(reducer, x, msg);
    },
    [=](shutdown_atom) {
      for (auto& i : self->state.indexers)
        self->send(i.second, shutdown_atom::value);
      // Wait until all indexers have terminated.
      self->set_down_handler(
        [=](down_msg const& msg) {
          remove_indexer(msg.source);
          if (self->state.indexers.empty())
            self->quit(exit_reason::user_shutdown);
        }
      );
    },
  };
}

} // namespace system
} // namespace vast
