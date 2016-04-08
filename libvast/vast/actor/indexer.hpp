#ifndef VAST_ACTOR_INDEXER_HPP
#define VAST_ACTOR_INDEXER_HPP

#include <caf/all.hpp>

#include "vast/bitmap_index_polymorphic.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/offset.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/basic_state.hpp"
#include "vast/concept/serializable/io.hpp"
#include "vast/concept/serializable/vast/bitmap_index_polymorphic.hpp"
#include "vast/concept/serializable/vast/type.hpp"
#include "vast/util/assert.hpp"

namespace vast {
namespace detail {

/// Wraps a bitmap index into an actor.
template <typename BitmapIndex>
struct bitmap_indexer {
  struct state : basic_state {
    state(local_actor* self, std::string name)
      : basic_state{self, std::move(name)} {
    }

    virtual bool push_back(BitmapIndex& bmi, event const& e) = 0;

    vast::path path;
    BitmapIndex bmi;
    uint64_t last_flush_ = 0;
  };

  template <typename State>
  static behavior make(stateful_actor<State>* self, path p, BitmapIndex bmi) {
    self->state.path = std::move(p);
    self->state.bmi = std::move(bmi);
    self->trap_exit(true);
    // Materialize an existing index.
    if (exists(self->state.path)) {
      auto t = load(self->state.path, self->state.last_flush_,
                    self->state.bmi);
      if (!t) {
        VAST_ERROR_AT(self, "failed to load bitmap index");
        self->quit(exit::error);
        return {};
      }
      VAST_DEBUG_AT(self, "loaded bitmap index of size",
                    self->state.bmi.size());
    }
    // Flush bitmap index to disk.
    auto flush = [=] {
      if (self->state.bmi.size() == self->state.last_flush_)
        return nothing;
      VAST_DEBUG_AT(self, "flushes bitmap index",
                    "(" << (self->state.bmi.size() - self->state.last_flush_)
                      << '/' << self->state.bmi.size(), "new/total bits)");
      self->state.last_flush_ = self->state.bmi.size();
      return save(self->state.path, self->state.last_flush_, self->state.bmi);
    };
    return {
      [=](exit_msg const& msg) {
        if (msg.reason == exit::kill) {
          self->quit(exit::kill);
          return;
        }
        auto t = flush();
        if (!t)
          VAST_ERROR_AT(self, "failed to flush:", t.error());
        self->quit(msg.reason);
      },
      [=](flush_atom, actor const& task) {
        auto t = flush();
        self->send(task, done_atom::value);
        if (!t) {
          VAST_ERROR_AT(self, "failed to flush:", t.error());
          self->quit(exit::error);
        }
      },
      [=](std::vector<event> const& events, actor const& task) {
        VAST_DEBUG_AT(self, "got", events.size(), "events");
        for (auto& e : events)
          if (e.id() == invalid_event_id) {
            VAST_ERROR_AT(self, "ignores event with invalid ID:", e);
          } else if (!self->state.push_back(self->state.bmi, e)) {
            VAST_ERROR_AT(self, "failed to append event", e);
            self->quit(exit::error);
            return;
          }
        self->send(task, done_atom::value);
      },
      [=](expression const& pred, actor const& sink, actor const& task) {
        VAST_DEBUG_AT(self, "looks up predicate:", pred);
        auto p = get<predicate>(pred);
        VAST_ASSERT(p);
        auto d = get<data>(p->rhs);
        VAST_ASSERT(d);
        auto r = self->state.bmi.lookup(p->op, *d);
        if (r) {
          self->send(sink, pred, std::move(*r));
        } else {
          VAST_ERROR_AT(self, "failed to lookup:", pred,
                        '(' << r.error() << ')');
          self->quit(exit::error);
        }
        self->send(task, done_atom::value);
      }
    };
  }
};

template <typename Bitstream>
struct event_name_state
  : bitmap_indexer<string_bitmap_index<Bitstream>>::state {
  using bitmap_index_type = string_bitmap_index<Bitstream>;

  event_name_state(local_actor* self)
    : bitmap_indexer<bitmap_index_type>::state {
        self, "event-name-indexer"} {
  }

  bool push_back(bitmap_index_type& bmi, event const& e) override {
    return bmi.push_back(e.type().name(), e.id());
  }
};

template <typename Bitstream>
behavior event_name_indexer(stateful_actor<event_name_state<Bitstream>>* self,
                            path p) {
  using bmi_type = typename event_name_state<Bitstream>::bitmap_index_type;
  return bitmap_indexer<bmi_type>::make(self, std::move(p), bmi_type{});
}

template <typename Bitstream>
struct event_time_state
  : bitmap_indexer<arithmetic_bitmap_index<Bitstream, time::point>>::state {
  using bitmap_index_type = arithmetic_bitmap_index<Bitstream, time::point>;

  event_time_state(local_actor* self)
    : bitmap_indexer<bitmap_index_type>::state {
        self, "event-time-indexer"} {
  }

  bool push_back(bitmap_index_type& bmi, event const& e) override {
    return bmi.push_back(e.timestamp(), e.id());
  }
};

template <typename Bitstream>
behavior event_time_indexer(stateful_actor<event_time_state<Bitstream>>* self,
                            path p) {
  using bmi_type = typename event_time_state<Bitstream>::bitmap_index_type;
  return bitmap_indexer<bmi_type>::make(self, std::move(p), bmi_type{});
}

template <typename BitmapIndex>
struct event_data_state : bitmap_indexer<BitmapIndex>::state {
  event_data_state(local_actor* self)
    : bitmap_indexer<BitmapIndex>::state{self, "event-data-indexer"} {
  }

  bool push_back(BitmapIndex& bmi, event const& e) override {
    // Because chunks may contain events of different types, we may end up with
    // an event that's not intended for us. self is not an error but rather
    // occurrs by design: the events from a single chunk arrive at multiple
    // indexers, each of which pick their relevant subset.
    if (e.type() != event_type)
      return true;
    auto r = get<record>(e);
    if (!r)
      return bmi.push_back(e.data(), e.id());
    if (auto d = r->at(offset))
      return bmi.push_back(*d, e.id());
    // If there is no data at a given offset, it means that an intermediate
    // record is nil but we're trying to access a deeper field.
    return bmi.push_back(nil, e.id());
  }

  type event_type;
  vast::offset offset;
};

template <typename Bitstream>
struct event_data_indexer_factory {
  event_data_indexer_factory(path const& p, offset const& o, type const& t)
    : dir_{p},
      off_{o},
      event_type_{t} {
  }

  template <typename BitmapIndex, typename... Ts>
  actor make(Ts&&... xs) const {
    using state_type = event_data_state<BitmapIndex>;
    static auto indexer = [](stateful_actor<state_type>* self, path const& dir,
                             type const& event_type, offset const& off,
                             BitmapIndex bmi) -> behavior {
      self->state.offset = off;
      self->state.event_type = event_type;
      return bitmap_indexer<BitmapIndex>::make(self, dir, std::move(bmi));
    };
    return spawn(indexer, dir_, event_type_, off_,
                 BitmapIndex{std::forward<Ts>(xs)...});
  }

  template <typename T>
  actor operator()(T const&) const {
    return make<arithmetic_bitmap_index<Bitstream, type::to_data<T>>>();
  }

  actor operator()(type::address const&) const {
    return make<address_bitmap_index<Bitstream>>();
  }

  actor operator()(type::subnet const&) const {
    return make<subnet_bitmap_index<Bitstream>>();
  }

  actor operator()(type::port const&) const {
    return make<port_bitmap_index<Bitstream>>();
  }

  actor operator()(type::string const&) const {
    return make<string_bitmap_index<Bitstream>>();
  }

  actor operator()(type::enumeration const&) const {
    return make<string_bitmap_index<Bitstream>>();
  }

  actor operator()(type::vector const& t) const {
    return make<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  actor operator()(type::set const& t) const {
    return make<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  actor operator()(none const&) const {
    die("invalid type will neever be supported");
  }

  actor operator()(type::pattern const&) const {
    die("regular expressions not yet supported");
  }

  actor operator()(type::table const&) const {
    die("tables not yet supported");
  }

  actor operator()(type::record const&) const {
    die("records shall be unrolled");
  }

  actor operator()(type::alias const& a) const {
    return visit(*this, a.type());
  }

  path const& dir_;
  offset const& off_;
  type const& event_type_;
};

template <typename Bitstream>
actor spawn_data_bitmap_indexer(type const& data_type,
                                path const& dir,
                                offset const& off,
                                type const& event_type) {
  return visit(event_data_indexer_factory<Bitstream>{dir, off, event_type},
               data_type);
}

} // namespace detail

/// Indexes events of a fixed type.
template <typename Bitstream>
struct event_indexer {
  struct state : basic_state {
    state(local_actor* self)
      : basic_state{self, "event-indexer"} {
    }

    actor spawn_name_indexer() {
      auto p = dir / "meta" / "name";
      auto& a = indexers[p];
      if (!a) {
        VAST_DEBUG_AT(self, "spawns name indexer:", p);
        a = self->spawn<monitored>(detail::event_name_indexer<Bitstream>,
                                   std::move(p));
      }
      return a;
    }

    actor spawn_time_indexer() {
      auto p = dir / "meta" / "time";
      auto& a = indexers[p];
      if (!a) {
        VAST_DEBUG_AT(self, "spawns time indexer:", p);
        a = self->spawn<monitored>(detail::event_time_indexer<Bitstream>,
                                   std::move(p));
      }
      return a;
    }

    trial<actor> spawn_data_indexer(offset const& o) {
      auto p = dir / "data";
      auto r = get<type::record>(event_type);
      if (r) {
        if (o.empty())
          return error{"empty offset for record event ", event_type.name()};
        auto key = r->resolve(o);
        if (!key)
          return error{"invalid offset ", o, ": ", key.error()};
        for (auto& k : *key)
          p /= k;
      }
      auto& a = indexers[p];
      if (!a) {
        VAST_DEBUG_AT(self, "spawns data indexer:", p);
        type const* t;
        if (!r)
          t = &event_type;
        else if (auto x = r->at(o))
          t = x;
        else
          return error{"invalid offset for event ", event_type.name(), ": ", o};
        a = detail::spawn_data_bitmap_indexer<Bitstream>(*t, p, o, event_type);
        self->monitor(a);
      }
      return a;
    };

    void spawn_bitmap_indexers() {
      spawn_time_indexer();
      spawn_name_indexer();
      if (auto r = get<type::record>(event_type)) {
        for (auto& i : type::record::each{*r}) {
          if (!i.trace.back()->type.find_attribute(type::attribute::skip)) {
            auto a = spawn_data_indexer(i.offset);
            if (!a) {
              VAST_ERROR(self, "could not load indexer for ", i.offset);
              self->quit(exit::error);
              return;
            }
          }
        }
      } else if (!event_type.find_attribute(type::attribute::skip)) {
        auto a = spawn_data_indexer({});
        if (!a) {
          VAST_ERROR(self, "could not load indexer for ", event_type);
          self->quit(exit::error);
          return;
        }
      }
    };

    path dir;
    type event_type;
    std::map<path, actor> indexers;
  };

  struct loader {
    loader(state& s) : state_{s} { }

    template <typename T>
    std::vector<actor> operator()(T const&) {
      return {};
    }

    template <typename T, typename U>
    std::vector<actor> operator()(T const&, U const&) {
      return {};
    }

    std::vector<actor> operator()(predicate const& p) {
      op_ = p.op;
      return visit(*this, p.lhs, p.rhs);
    }

    std::vector<actor> operator()(event_extractor const&, data const&) {
      return {state_.spawn_name_indexer()};
    }

    std::vector<actor> operator()(time_extractor const&, data const&) {
      return {state_.spawn_time_indexer()};
    }

    std::vector<actor> operator()(type_extractor const& e, data const&) {
      std::vector<actor> result;
      if (auto r = get<type::record>(state_.event_type)) {
        for (auto& i : type::record::each{*r})
          if (i.trace.back()->type == e.type) {
            auto a = state_.spawn_data_indexer(i.offset);
            if (a) {
              result.push_back(std::move(*a));
            } else {
              VAST_ERROR(a.error());
              return {};
            }
          }
      } else if (state_.event_type == e.type) {
        auto a = state_.spawn_data_indexer({});
        if (a)
          result.push_back(std::move(*a));
        else
          VAST_ERROR(a.error());
      }
      return result;
    }

    std::vector<actor> operator()(schema_extractor const& e,
                                       data const& d) {
      std::vector<actor> result;
      if (auto r = get<type::record>(state_.event_type)) {
        for (auto& pair : r->find_suffix(e.key)) {
          auto& o = pair.first;
          auto lhs = r->at(o);
          VAST_ASSERT(lhs);
          if (!compatible(*lhs, op_, type::derive(d))) {
            VAST_WARN_AT(state_.self, "encountered type clash: LHS =",
                         *lhs, "<=> RHS =", type::derive(d));
            return {};
          }
          auto a = state_.spawn_data_indexer(o);
          if (!a)
            VAST_ERROR(a.error());
          else
            result.push_back(std::move(*a));
        }
      } else if (e.key.size() == 1) {
        if (pattern::glob(e.key[0]).match(state_.event_type.name())) {
          auto a = state_.spawn_data_indexer({});
          if (a)
            result.push_back(std::move(*a));
          else
            VAST_ERROR(a.error());
        }
      }
      return result;
    }

    template <typename T>
    std::vector<actor> operator()(data const& d, T const& e) {
      return (*this)(e, d);
    }

    relational_operator op_;
    state& state_;
  };

  /// Spawns an event indexer.
  /// @param dir The directory in which to create new state.
  /// @param event_Type The type of the event.
  static behavior make(stateful_actor<state>* self, path dir, type event_type) {
    self->state.dir = std::move(dir);
    self->state.event_type = std::move(event_type);
    self->trap_exit(true);
    // If the directory doesn't exist yet, we're in "construction" mode,
    // spawning all bitmap indexer to be able to handle new events directly.
    // Otherwise we just load the indexers specified in the query.
    if (!exists(self->state.dir))
      self->state.spawn_bitmap_indexers();
    auto remove_indexer = [=](actor_addr const& a) {
      auto i = std::find_if(self->state.indexers.begin(),
                            self->state.indexers.end(),
                            [=](auto& pair) { return pair.second == a; });
      if (i != self->state.indexers.end())
        self->state.indexers.erase(i);
    };
    return {
      [=](exit_msg const& msg) {
        for (auto& i : self->state.indexers)
          self->send_exit(i.second, msg.reason);
        if (self->state.indexers.empty())
          self->quit(msg.reason);
        else
          self->become(
            [reason=msg.reason, self, remove_indexer](down_msg const& msg) {
              remove_indexer(msg.source);
              if (self->state.indexers.empty())
                self->quit(reason);
            }
          );
      },
      [=](down_msg const& msg) {
        remove_indexer(msg.source);
      },
      [=](load_atom) {
        self->state.spawn_bitmap_indexers();
        VAST_DEBUG_AT(self, "spawned", self->state.indexers.size(), "indexers");
      },
      [=](std::vector<event> const&, actor const& task) {
        for (auto& i : self->state.indexers) {
          self->send(task, i.second);
          self->send(i.second, self->current_message());
        }
        self->send(task, done_atom::value);
      },
      [=](flush_atom, actor const& task) {
        VAST_DEBUG_AT(self, "flushes", self->state.indexers.size(), "indexers");
        for (auto& i : self->state.indexers) {
          self->send(task, i.second);
          self->send(i.second, self->current_message());
        }
        self->send(task, done_atom::value);
      },
      [=](expression const& pred, actor const&, actor const& task) {
        auto p = get<predicate>(pred);
        VAST_ASSERT(p);
        auto indexers = loader{self->state}(*p);
        if (indexers.empty())
          VAST_DEBUG_AT(self, "did not find matching indexers for", pred);
        for (auto& i : indexers) {
          self->send(task, i);
          self->send(i, self->current_message());
        }
        self->send(task, done_atom::value);
      }
    };
  }
};

} // namespace vast

#endif
