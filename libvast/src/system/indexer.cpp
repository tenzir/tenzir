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
    // Materialize an existing index when encountering persistent state.
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
    // Construct a new index.
    self->state.idx = value_index::make(self->state.type);
    if (!self->state.idx)
      self->quit(make_error(ec::unspecified, "failed to construct index"));
  }
  // Flush bitmap index to disk.
  auto dir = self->state.filename.parent();
  auto flush = [=]() -> expected<void> {
    auto& last = self->state.last_flush;
    auto offset = self->state.idx->offset();
    if (offset == last)
      return {};
    // Create parent directory if it doesn't exist.
    if (!exists(dir)) {
      auto result = mkdir(dir);
      if (!result)
        return result.error();
    }
    VAST_DEBUG(self, "flushes index", "(" << (offset - last) << '/' << offset,
               "new/total bits)");
    last = offset;
    detail::value_index_inspect_helper tmp{self->state.type, self->state.idx};
    return save(self->state.filename, self->state.last_flush, tmp);
  };
  return {
    [=](shutdown_atom) {
      auto result = flush();
      if (result)
        self->quit(exit_reason::user_shutdown);
      else
        self->quit(result.error());
    },
    [=](flush_atom, actor const& task) {
      auto result = flush();
      self->send(task, done_atom::value);
      if (!result)
        self->quit(result.error());
    },
    [=](std::vector<event> const& events, actor const& task) {
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
      self->send(task, done_atom::value);
    },
    [=](predicate const& pred, actor const& sink, actor const& task) {
      VAST_TRACE(self, "got predicate:", pred);
      auto result = self->state.idx->lookup(pred.op, get<data>(pred.rhs));
      if (result) {
        self->send(sink, pred, std::move(*result));
      } else {
        VAST_ERROR(self, "failed to lookup:", pred,
                   '(' << self->system().render(result.error()) << ')');
        self->quit(result.error());
      }
      self->send(task, done_atom::value);
    }
  };
}

// In the current event indexing design, all indexers receive all events and
// pick the aspect of the event that's relevant to them. For event meta data
// indexers, every event is relevant. Event data indexers concern themselves
// only a specific aspect of a specific event type.

behavior time_indexer(stateful_actor<value_indexer_state>* self,
                           path const& p) {
  // TODO: add type attributes to tune index, e.g., for seconds granularity.
  auto t = timestamp_type{};
  auto extract = [](event const& e) { return optional<data>{e.timestamp()}; };
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

  template <class T, class U>
  result_type operator()(T const&, U const&) {
    return {};
  }

  result_type operator()(attribute_extractor const& ex, data const& x) {
    result_type result;
    auto p = self->state.dir / "meta";
    if (ex.attr == "time") {
      if (!is<timestamp>(x)) {
        VAST_WARNING(self, "got time attribute but no timestamp:", x);
      } else {
        p /= "time";
        VAST_DEBUG(self, "loads value index at", p);
        auto& a = self->state.indexers[p];
        if (!a)
          a = self->spawn<monitored>(time_indexer, p);
        result.push_back(a);
      }
    } else {
      VAST_WARNING(self, "got unsupported attribute:", ex.attr);
    }
    return result;
  }

  result_type operator()(key_extractor const& ex, data const& x) {
    result_type result;
    VAST_ASSERT(!ex.key.empty());
    // TODO: this branching logic is identical to the one used during index
    // lookup in src/expression_visitors.cpp. We should factor it.
    // First, try to interpret the key as a type.
    if (auto t = to<type>(ex.key[0])) {
      if (ex.key.size() == 1) {
        if (auto r = get_if<record_type>(self->state.event_type)) {
          for (auto& f : record_type::each{*r}) {
            auto& value_type = f.trace.back()->type;
            if (congruent(value_type, *t)) {
              auto p = self->state.dir / "data";
              for (auto& k : f.key())
                p /= k;
              VAST_DEBUG(self, "loads value index at", p);
              auto& a = self->state.indexers[p];
              if (!a)
                a = self->spawn<monitored>(field_data_indexer, p,
                                           self->state.event_type,
                                           value_type, f.offset);
              result.push_back(a);
            }
          }
        } else if (congruent(self->state.event_type, *t)) {
          auto p = self->state.dir / "data";
          VAST_DEBUG(self, "loads value index at", p);
          auto& a = self->state.indexers[p];
          if (!a)
            a = self->spawn<monitored>(flat_data_indexer, p,
                                       self->state.event_type);
          result.push_back(a);
        }
      } else {
        // Keys that look like types, but have more than one component don't
        // make sense, e.g., addr.count.vector<int>.
        VAST_WARNING(self, "got weird key:", ex.key);
      }
    // Second, interpret the key as a suffix of a record field name.
    } else if (auto r = get_if<record_type>(self->state.event_type)) {
      auto suffixes = r->find_suffix(ex.key);
      // All suffixes must pass the type check, otherwise the RHS of a
      // predicate would be ambiguous.
      for (auto& pair : suffixes) {
        auto t = r->at(pair.first);
        VAST_ASSERT(t);
        if (!compatible(*t, op, x)) {
          VAST_WARNING(self, "encountered type clash: ", *t, op, x);
          return {};
        }
      }
      for (auto& pair : suffixes) {
        auto p = self->state.dir / "data";
        if (!pair.second.empty()) {
          // The suffix contains the top-level record name as well, which we
          // don't need here.
          VAST_ASSERT(pair.second.size() >= 2);
          for (auto i = pair.second.begin() + 1; i != pair.second.end(); ++i)
            p /= *i;
        }
        VAST_DEBUG(self, "loads value index at", p);
        auto& a = self->state.indexers[p];
        if (!a)
          a = self->spawn<monitored>(field_data_indexer, p,
                                     self->state.event_type,
                                     *r->at(pair.first), pair.first);
        result.push_back(a);
      }
    // Third, try to interpret the key as the name of a single type.
    } else if (ex.key[0] == self->state.event_type.name()) {
      if (!compatible(self->state.event_type, op, x)) {
          VAST_WARNING(self, "encountered type clash: ",
                       self->state.event_type, op, x);
      } else {
        auto p = self->state.dir / "data";
        VAST_DEBUG(self, "loads value index at", p);
        auto& a = self->state.indexers[p];
        if (!a)
          a = self->spawn<monitored>(flat_data_indexer, p,
                                     self->state.event_type);
        result.push_back(a);
      }
    }
    return result;
  }

  stateful_actor<event_indexer_state>* self;
  relational_operator op;
};

} // namespace <anonymous>

behavior event_indexer(stateful_actor<event_indexer_state>* self,
                       path dir, type event_type) {
  self->state.dir = dir;
  self->state.event_type = event_type;
  VAST_DEBUG(self, "operates for type", event_type.name(), "in", dir);
  // If the directory doesn't exist yet, we're in "construction" mode,
  // where we spawn all indexers to be able to handle incoming events directly.
  // Otherwise we deal with a "frozen" indexer that only spawns indexers as
  // needed for answering queries.
  if (!exists(dir)) {
    VAST_DEBUG(self, "has no persistent state, spawning indexers");
    // Spawn indexers for event meta data.
    auto p = dir / "meta" / "time";
    auto a = self->spawn<monitored>(time_indexer, p);
    self->state.indexers.emplace(p, a);
    // Spawn indexers for event data.
    if (!skip(event_type)) {
      auto r = get_if<record_type>(event_type);
      if (!r) {
        p = dir / "data";
        VAST_DEBUG(self, "spawns new value index at", p);
        a = self->spawn<monitored>(flat_data_indexer, p, event_type);
        self->state.indexers.emplace(p, a);
      } else {
        for (auto& f : record_type::each{*r}) {
          auto& value_type = f.trace.back()->type;
          if (!skip(value_type)) {
            p = dir / "data";
            for (auto& k : f.key())
              p /= k;
            VAST_DEBUG(self, "spawns new value index at", p);
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
    [=](std::vector<event> const&, actor task) {
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& i : self->state.indexers) {
        self->send(task, i.second);
        self->send(i.second, msg);
      }
      self->send(task, done_atom::value);
    },
    [=](flush_atom, actor const& task) {
      VAST_DEBUG(self, "flushes", self->state.indexers.size(), "indexers");
      for (auto& i : self->state.indexers) {
        self->send(task, i.second);
        self->send(i.second, flush_atom::value, task);
      }
      self->send(task, done_atom::value);
    },
    [=](predicate const& pred, actor const& /* sink */, actor task) {
      // For now, we require that the predicate is part of a normalized
      // expression, i.e., LHS an extractor type and RHS of type data.
      VAST_DEBUG(self, "got predicate:", pred);
      auto rhs = get_if<data>(pred.rhs);
      VAST_ASSERT(rhs);
      auto indexers = visit(loader{self, pred.op}, pred.lhs, pred.rhs);
      // Forward predicate to all available indexers.
      if (indexers.empty())
        VAST_DEBUG(self, "did not find matching indexers for", pred);
      else
        VAST_DEBUG(self, "found", indexers.size(), "matching indexers");
      auto msg = self->current_mailbox_element()->move_content_to_message();
      for (auto& i : indexers) {
        self->send(task, i);
        self->send(i, msg);
      }
      self->send(task, done_atom::value);
    }
  };
}

} // namespace system
} // namespace vast
