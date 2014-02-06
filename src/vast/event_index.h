#ifndef VAST_EVENT_INDEX_H
#define VAST_EVENT_INDEX_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/cow.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/offset.h"
#include "vast/search_result.h"
#include "vast/bitmap_index.h"
#include "vast/io/serialization.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Indexes a certain aspect of events.
template <typename Derived>
class event_index : public actor<event_index<Derived>>
{
public:
  /// Spawns an event index.
  /// @param dir The absolute path on the file system.
  event_index(path dir)
    : dir_{std::move(dir)},
      stats_{std::chrono::seconds{1}}
  {
  }

  void act()
  {
    using namespace cppa;

    this->trap_exit(true);

    derived()->scan();

    become(
        on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
        {
          if (reason != exit::kill)
            derived()->save();

          this->quit(reason);
        },
        on(atom("flush")) >> [=]
        {
          derived()->save();
        },
        on_arg_match >> [=](std::vector<cow<event>> const& v)
        {
          for (auto& e : v)
            if (! derived()->index(*e))
            {
              VAST_LOG_ACTOR_ERROR("failed to index event " << *e);
              this->quit(exit::error);
              return;
            }

          if (stats_.increment(v.size()))
          {
            VAST_LOG_ACTOR_INFO(
                "indexes at rate " << stats_.last() << " events/sec" <<
                " (mean " << stats_.mean() <<
                ", median " << stats_.median() <<
                ", standard deviation " << std::round(stats_.sd()) << ")");

            //send(last_sender(), atom("statistics"), stats_.last());
          }
        },
        on_arg_match >> [=](expr::ast const& ast, bitstream const& coverage,
                            actor_ptr const& sink)
        {
          derived()->load(ast);
          auto r = derived()->lookup(ast);
          assert(coverage);
          send(sink, ast, search_result{std::move(r), coverage});
        });
  }

  char const* description()
  {
    return derived()->description();
  }

protected:
  using bitstream_type = default_encoded_bitstream;

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

  util::rate_accumulator<uint64_t> stats_;
};

class event_meta_index : public event_index<event_meta_index>
{
public:
  event_meta_index(path dir);

  char const* description() const;

  void scan();
  void load(expr::ast const& ast);
  void save();
  bool index(event const& e);
  bitstream lookup(expr::ast const& ast) const;

private:
  struct loader;
  struct querier;

  arithmetic_bitmap_index<bitstream_type, time_point_type> timestamp_;
  string_bitmap_index<bitstream_type> name_;
  bool exists_ = false;
};

class event_arg_index : public event_index<event_arg_index>
{
public:
  event_arg_index(path dir);

  char const* description() const;

  void scan();
  void load(expr::ast const& ast);
  void save();
  bool index(event const& e);
  bitstream lookup(expr::ast const& ast) const;

private:
  struct loader;
  struct querier;

  /// Constructs a filesystem path from an offset.
  /// @param o The offset to convert.
  /// @returns The path corresponding to *o*.
  path pathify(offset const& o) const;

  /// Loads a path of an event argument index into memory.
  /// @param p The path to pointing to the index file.
  /// @param type If not `nullptr`, expect that *p* has this type.
  /// @returns The bitmap for *p* on success and `nullptr` otherwise.
  bitmap_index* load(path const& p, value_type const* type = nullptr);

  /// Index an event/record starting at a given offset.
  /// @param r The record to index.
  /// @param id The event ID to associate with values in *r*.
  /// @param o The current offset.
  /// @returns `true` on success.
  bool index_record(record const& r, uint64_t id, offset& o);

  std::multimap<value_type, path> files_;
  std::map<offset, std::unique_ptr<bitmap_index>> offsets_;
  std::multimap<value_type, bitmap_index*> types_;
  offset idx_off_;
};

} // namespace vast

#endif
