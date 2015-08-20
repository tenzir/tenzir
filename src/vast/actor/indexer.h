#ifndef VAST_ACTOR_INDEXER_H
#define VAST_ACTOR_INDEXER_H

#include <caf/all.hpp>

#include "vast/bitmap_index_polymorphic.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/offset.h"
#include "vast/actor/actor.h"
#include "vast/concept/serializable/io.h"
#include "vast/concept/serializable/vast/bitmap_index_polymorphic.h"
#include "vast/concept/serializable/vast/type.h"
#include "vast/util/assert.h"

namespace vast {
namespace detail {

/// Wraps a singleton bitmap index into an actor.
template <typename Bitstream, typename BitmapIndex>
class bitmap_indexer : public default_actor {
public:
  using bitstream_type = Bitstream;
  using bitmap_index_type = BitmapIndex;

  template <typename... Args>
  bitmap_indexer(char const* name, path p, Args&&... args)
    : default_actor{name},
      path_{std::move(p)},
      bmi_{std::forward<Args>(args)...} {
    trap_exit(true);
  }

  virtual bool push_back(BitmapIndex& bmi, event const& e) = 0;

  behavior make_behavior() override {
    // Materialize an existing index.
    if (exists(path_)) {
      using vast::load;
      auto t = load(path_, last_flush_, bmi_);
      if (!t) {
        VAST_ERROR(this, "failed to load bitmap index");
        quit(exit::error);
        return {};
      }
      VAST_DEBUG(this, "materialized bitmap index of size", bmi_.size());
    }
    return {
      [=](exit_msg const& msg) {
        if (msg.reason == exit::kill) {
          quit(exit::kill);
          return;
        }
        auto t = flush();
        if (!t)
          VAST_ERROR(this, "failed to flush:", t.error());
        quit(msg.reason);
      },
      [=](flush_atom, actor const& task) {
        auto t = flush();
        send(task, done_atom::value);
        if (!t) {
          VAST_ERROR(this, "failed to flush:", t.error());
          quit(exit::error);
        }
      },
      [=](std::vector<event> const& events, actor const& task) {
        VAST_DEBUG(this, "got", events.size(), "events");
        for (auto& e : events)
          if (e.id() == invalid_event_id) {
            VAST_ERROR(this, "ignores event with invalid ID:", e);
          } else if (!push_back(bmi_, e)) {
            VAST_ERROR(this, "failed to append event", e);
            quit(exit::error);
            return;
          }
        send(task, done_atom::value);
      },
      [=](expression const& pred, actor const& sink, actor const& task) {
        VAST_DEBUG(this, "looks up predicate:", pred);
        auto p = get<predicate>(pred);
        VAST_ASSERT(p);
        auto d = get<data>(p->rhs);
        VAST_ASSERT(d);
        auto r = bmi_.lookup(p->op, *d);
        if (r) {
          send(sink, pred, std::move(*r));
        } else {
          VAST_ERROR(this, "failed to lookup:", pred, '(' << r.error() << ')');
          quit(exit::error);
        }
        send(task, done_atom::value);
      }
    };
  }

private:
  trial<void> flush() {
    if (bmi_.size() == last_flush_)
      return nothing;
    VAST_DEBUG(this, "flushes bitmap index (" << (bmi_.size() - last_flush_)
               << '/' << bmi_.size(), "new/total bits)");
    last_flush_ = bmi_.size();
    using vast::save;
    auto t = save(path_, last_flush_, bmi_);
    return nothing;
  }

  path path_;
  BitmapIndex bmi_;
  uint64_t last_flush_ = 0;
};

/// Indexes the name of an event.
template <typename Bitstream>
struct event_name_indexer
  : bitmap_indexer<Bitstream, string_bitmap_index<Bitstream>> {
  using super = bitmap_indexer<Bitstream, string_bitmap_index<Bitstream>>;

  using typename super::bitmap_index_type;

  template <typename... Args>
  event_name_indexer(path p, Args&&... args)
    : super{"event-name-indexer", std::move(p), std::forward<Args>(args)...} {
  }

  bool push_back(bitmap_index_type& bmi, event const& e) override {
    return bmi.push_back(e.type().name(), e.id());
  }
};

/// Indexes the timestamp of an event.
template <typename Bitstream>
struct event_time_indexer
  : bitmap_indexer<Bitstream, arithmetic_bitmap_index<Bitstream, time::point>> {
  using super = bitmap_indexer<
    Bitstream,
    arithmetic_bitmap_index<Bitstream, time::point>
  >;

  using typename super::bitmap_index_type;

  template <typename... Args>
  event_time_indexer(path p, Args&&... args)
    : super{"event-time-indexer", std::move(p), std::forward<Args>(args)...} {
  }

  bool push_back(bitmap_index_type& bmi, event const& e) override {
    return bmi.push_back(e.timestamp(), e.id());
  }

  time::point ts_;
};

/// Indexes the timestamp of an event.
template <typename Bitstream, typename BitmapIndex>
struct event_data_indexer : public bitmap_indexer<Bitstream, BitmapIndex> {
  using super = bitmap_indexer<Bitstream, BitmapIndex>;
  using typename super::bitmap_index_type;

  template <typename... Args>
  event_data_indexer(path p, offset o, type t, Args&&... args)
    : super{"event-data-indexer", std::move(p), std::forward<Args>(args)...},
      event_type_{std::move(t)},
      offset_(std::move(o)) {
  }

  bool push_back(bitmap_index_type& bmi, event const& e) override {
    // Because chunks may contain events of different types, we may end up with
    // an event that's not intended for us. This is not an error but rather
    // occurrs by design: the events from a single chunk arrive at multiple
    // indexers, each of which pick their relevant subset.
    if (e.type() != event_type_)
      return true;
    auto r = get<record>(e);
    if (!r)
      return bmi.push_back(e.data(), e.id());
    if (auto d = r->at(offset_))
      return bmi.push_back(*d, e.id());
    // If there is no data at a given offset, it means that an intermediate
    // record is nil but we're trying to access a deeper field.
    return bmi.push_back(nil, e.id());
  }

  type const event_type_;
  offset const offset_;
};

template <typename Bitstream>
struct indexer_factory {
  indexer_factory(path const& p, offset const& o, type const& t)
    : path_{p}, off_{o}, event_type_{t} {
  }

  template <typename T>
  trial<actor> operator()(T const&) const {
    return make<arithmetic_bitmap_index<Bitstream, type::to_data<T>>>();
  }

  trial<actor> operator()(type::address const&) const {
    return make<address_bitmap_index<Bitstream>>();
  }

  trial<actor> operator()(type::subnet const&) const {
    return make<subnet_bitmap_index<Bitstream>>();
  }

  trial<actor> operator()(type::port const&) const {
    return make<port_bitmap_index<Bitstream>>();
  }

  trial<actor> operator()(type::string const&) const {
    return make<string_bitmap_index<Bitstream>>();
  }

  trial<actor> operator()(type::enumeration const&) const {
    return make<string_bitmap_index<Bitstream>>();
  }

  trial<actor> operator()(type::vector const& t) const {
    return make<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  trial<actor> operator()(type::set const& t) const {
    return make<sequence_bitmap_index<Bitstream>>(t.elem());
  }

  trial<actor> operator()(none const&) const {
    return error{"bitmap index for invalid type not supported"};
  }

  trial<actor> operator()(type::pattern const&) const {
    return error{"regular expressions not yet supported"};
  }

  trial<actor> operator()(type::table const&) const {
    return error{"tables not yet supported"};
  }

  trial<actor> operator()(type::record const&) const {
    return error{"records shall be unrolled"};
  }

  trial<actor> operator()(type::alias const& a) const {
    return visit(*this, a.type());
  }

  template <typename BitmapIndex, typename... Args>
  actor make(Args&&... args) const {
    using indexer_type = event_data_indexer<Bitstream, BitmapIndex>;
    return spawn<indexer_type>(path_, off_, event_type_,
                                    std::forward<Args>(args)...);
  }

  path const& path_;
  offset const& off_;
  type const& event_type_;
};

/// Factory to construct an indexer based on a given type.
/// @param t The type of the data.
/// @param p The directory where to store the indexer state under.
/// @param o The location of the non-record data to index.
/// @param e The e
template <typename Bitstream>
trial<actor> make_data_indexer(type const& t, path const& p,
                                    offset const& o, type const& e) {
  return visit(indexer_factory<Bitstream>{p, o, e}, t);
}

} // namespace detail

/// Indexes an event.
template <typename Bitstream>
struct event_indexer : default_actor {
  struct loader {
    loader(event_indexer& ei) : indexer_{ei} {
    }

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
      return {indexer_.load_name_indexer()};
    }

    std::vector<actor> operator()(time_extractor const&, data const&) {
      return {indexer_.load_time_indexer()};
    }

    std::vector<actor> operator()(type_extractor const& e, data const&) {
      std::vector<actor> indexes;
      if (auto r = get<type::record>(indexer_.type_)) {
        for (auto& i : type::record::each{*r})
          if (i.trace.back()->type == e.type) {
            auto a = indexer_.load_data_indexer(i.offset);
            if (a) {
              indexes.push_back(std::move(*a));
            } else {
              VAST_ERROR(a.error());
              return {};
            }
          }
      } else if (indexer_.type_ == e.type) {
        auto a = indexer_.load_data_indexer({});
        if (a)
          indexes.push_back(std::move(*a));
        else
          VAST_ERROR(a.error());
      }
      return indexes;
    }

    std::vector<actor> operator()(schema_extractor const& e,
                                       data const& d) {
      std::vector<actor> indexes;
      if (auto r = get<type::record>(indexer_.type_)) {
        for (auto& pair : r->find_suffix(e.key)) {
          auto& o = pair.first;
          auto lhs = r->at(o);
          VAST_ASSERT(lhs);
          if (!compatible(*lhs, op_, type::derive(d))) {
            VAST_WARN("type clash: LHS =", *lhs, "<=> RHS =", type::derive(d));
            return {};
          }
          auto a = indexer_.load_data_indexer(o);
          if (!a)
            VAST_ERROR(a.error());
          else
            indexes.push_back(std::move(*a));
        }
      } else if (e.key.size() == 1
                 && pattern::glob(e.key[0]).match(indexer_.type_.name())) {
        auto a = indexer_.load_data_indexer({});
        if (a)
          indexes.push_back(std::move(*a));
        else
          VAST_ERROR(a.error());
      }

      return indexes;
    }

    template <typename T>
    std::vector<actor> operator()(data const& d, T const& e) {
      return (*this)(e, d);
    }

    relational_operator op_;
    event_indexer& indexer_;
  };

  /// Spawns an event indexer.
  /// @param p The directory in which to create new state.
  /// @param t The type of the event. If invalid or given, the indexer attempts
  ///          runs in the (read-only) "query mode" to selectively lookup
  ///          certain bitmap indexes. If valid, the indexer runs in
  ///          (write-only) "construction mode" and spawns all bitmap indexes.
  event_indexer(path p, type t = {})
    : default_actor{"event-indexer"}, dir_{std::move(p)}, type_{std::move(t)} {
    trap_exit(true);
  }

  actor load_name_indexer() {
    auto p = dir_ / "meta" / "name";
    auto& a = indexers_[p];
    if (!a) {
      VAST_DEBUG(this, "loads name indexer:", p);
      a = spawn<detail::event_name_indexer<Bitstream>, monitored>(std::move(p));
    }
    return a;
  }

  actor load_time_indexer() {
    auto p = dir_ / "meta" / "time";
    auto& a = indexers_[p];
    if (!a) {
      VAST_DEBUG(this, "loads time indexer:", p);
      a = spawn<detail::event_time_indexer<Bitstream>, monitored>(std::move(p));
    }
    return a;
  }

  trial<actor> load_data_indexer(offset const& o) {
    auto p = dir_ / "data";
    auto r = get<type::record>(type_);
    if (r) {
      if (o.empty())
        return error{"empty offset for record event ", type_.name()};
      auto key = r->resolve(o);
      if (!key)
        return error{"invalid offset ", o, ": ", key.error()};
      for (auto& k : *key)
        p /= k;
    }
    auto& a = indexers_[p];
    if (!a) {
      VAST_DEBUG(this, "loads data indexer:", p);
      type const* t;
      if (!r)
        t = &type_;
      else if (auto x = r->at(o))
        t = x;
      else
        return error{"invalid offset for event ", type_.name(), ": ", o};
      auto i = detail::make_data_indexer<Bitstream>(*t, p, o, type_);
      if (!i)
        return i;
      else
        a = std::move(*i);
      monitor(a);
    }
    return a;
  }

  void on_exit() override {
    indexers_.clear();
  }

  behavior make_behavior() override {
    if (!exists(dir_))
      load_bitmap_indexers();
    auto on_down = [=](down_msg const& msg) {
      for (auto i = indexers_.begin(); i != indexers_.end(); ++i)
        if (i->second.address() == msg.source) {
          indexers_.erase(i);
          break;
        }
    };
    return {
      [=](exit_msg const& msg) {
        if (indexers_.empty()) {
          quit(msg.reason);
          return;
        }
        become([ reason = msg.reason, on_down, this ](
          down_msg const& msg) {
          on_down(msg);
          if (indexers_.empty())
            quit(reason);
        });
        for (auto& i : indexers_)
          send_exit(i.second, msg.reason);
      },
      on_down,
      [=](load_atom) {
        load_bitmap_indexers();
        VAST_DEBUG(this, "has loaded", indexers_.size(), "indexers");
      },
      [=](std::vector<event> const&, actor const& task) {
        for (auto& i : indexers_) {
          send(task, i.second);
          send_as(this, i.second, current_message());
        }
        send(task, done_atom::value);
      },
      [=](flush_atom, actor const& task) {
        VAST_DEBUG(this, "flushes", indexers_.size(), "indexers");
        for (auto& i : indexers_) {
          send(task, i.second);
          send_as(this, i.second, current_message());
        }
        send(task, done_atom::value);
      },
      [=](expression const& pred, actor const&, actor const& task) {
        VAST_ASSERT(is<predicate>(pred));
        auto indexers = visit(loader{*this}, pred);
        if (indexers.empty()) {
          VAST_DEBUG(this, "did not find matching indexers for", pred);
        } else {
          for (auto& i : indexers) {
            send(task, i);
            send_as(this, i, current_message());
          }
        }
        send(task, done_atom::value);
      }
    };
  }

private:
  void load_bitmap_indexers() {
    load_time_indexer();
    load_name_indexer();
    if (auto r = get<type::record>(type_)) {
      for (auto& i : type::record::each{*r}) {
        if (!i.trace.back()->type.find_attribute(type::attribute::skip)) {
          auto a = load_data_indexer(i.offset);
          if (!a) {
            VAST_ERROR(this, "could not load indexer for ", i.offset);
            quit(exit::error);
            return;
          }
        }
      }
    } else if (!type_.find_attribute(type::attribute::skip)) {
      auto a = load_data_indexer({});
      if (!a) {
        VAST_ERROR(this, "could not load indexer for ", type_);
        quit(exit::error);
        return;
      }
    }
  }

  path const dir_;
  type type_;
  std::map<path, actor> indexers_;
};

} // namespace vast

#endif
