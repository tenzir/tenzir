#ifndef VAST_EVENT_INDEX_H
#define VAST_EVENT_INDEX_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/offset.h"
#include "vast/search_result.h"
#include "vast/bitmap_index/string.h"
#include "vast/bitmap_index/time.h"
#include "vast/io/serialization.h"

namespace vast {

/// Indexes a certain aspect of events.
template <typename Derived>
class event_index : public actor<event_index<Derived>>
{
public:
  /// Spawns an event index.
  /// @param dir The absolute path on the file system.
  event_index(path dir)
    : dir_{std::move(dir)}
  {
  }

  void act()
  {
    using namespace cppa;

    this->chaining(false);
    this->trap_exit(true);

    derived()->scan();

    become(
        on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
        {
          derived()->store();
          this->quit(reason);
        },
        on(atom("flush")) >> [=]
        {
          derived()->store();
        },
        on_arg_match >> [=](event const& e)
        {
          if (! derived()->index(e))
          {
            VAST_LOG_ACTOR_ERROR("failed to index event " << e);
            this->quit(exit::error);
          }
        },
        on_arg_match >> [=](expr::ast const& ast, bitstream const& coverage,
                            actor_ptr const& sink)
        {
          derived()->load(ast);
          auto r = derived()->lookup(ast);
          send(sink, ast, search_result{std::move(r), coverage});
        });
  }

  char const* description()
  {
    return derived()->description();
  }

protected:
  path const dir_;

private:
  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  bool loadable_ = false;
};

class event_meta_index : public event_index<event_meta_index>
{
public:
  event_meta_index(path dir);

  char const* description() const;

  void scan();
  void load(expr::ast const& ast);
  void store();
  bool index(event const& e);
  bitstream lookup(expr::ast const& ast) const;

private:
  struct loader;
  struct querier;

  time_bitmap_index timestamp_;
  string_bitmap_index name_;
  bool exists_ = false;
};

class event_arg_index : public event_index<event_arg_index>
{
public:
  event_arg_index(path dir);

  char const* description() const;

  void scan();
  void load(expr::ast const& ast);
  void store();
  bool index(event const& e);
  bitstream lookup(expr::ast const& ast) const;

  path pathify(offset const& o) const;

private:
  struct loader;
  struct querier;

  bool index_record(record const& r, uint64_t id, offset& o);

  std::map<offset, std::shared_ptr<bitmap_index>> args_;
  std::multimap<value_type, std::shared_ptr<bitmap_index>> types_;
  std::multimap<value_type, path> files_;
};

} // namespace vast

#endif
