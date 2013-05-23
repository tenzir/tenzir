#include "vast/index.h"

#include "vast/bitmap_index.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/expression.h"

using namespace cppa;
namespace vast {
namespace {

/// Owns a several bitmap indexes for a arguments of a specific event.
template <typename Bitstream>
class filament : public sb_actor<filament<Bitstream>>
{
public:
  /// Spawns an event index with a given directory.
  /// @param dir The directory containing the event argument indexes.
  filament(std::string const& dir)
  {
    self->chaining(false);
    init_state = (
        on_arg_match >> [=](event const& e)
        {
          auto delta = e.id() - last_;
          for (auto& p : bitmaps_)
          {
            if (delta > 1)
              p.second->patch(delta - 1, false);
            p.second->push_back(e.at(p.first));
          }
          last_ = e.id();
        },
        on(atom("lookup")) >> [=]()
        {
        },
        on(atom("kill")) >> [=]()
        {
            // TODO: flush bitmap indexes.
        });
  }

  behavior init_state;

private:
  Bitstream lookup(std::vector<size_t> const& offsets,
                   value const& argument,
                   relational_operator op) const
  {
    auto i = bitmaps_.find(offsets);
    if (i == bitmaps_.end())
      return {};
    return i->second->lookup(argument, op);
  };

  std::map<std::vector<size_t>, std::unique_ptr<bitmap_index<Bitstream>>> bitmaps_;
  uint64_t last_ = 0;
};

/// Determines whether a query expression has clauses that may benefit from an
/// index lookup.
class checker : public expr::const_visitor
{
public:
  operator bool() const
  {
    return positive_;
  }

  virtual void visit(expr::node const&)
  {
    assert(! "should never happen");
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    positive_ = true;
  }

  virtual void visit(expr::name_extractor const&)
  {
    positive_ = true;
  }

  virtual void visit(expr::id_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::offset_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::type_extractor const&)
  {
    /* Do exactly nothing. */
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& op : conj.operands())
      if (! positive_)
        op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& op : disj.operands())
      if (! positive_)
        op->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(rel.operands().size() == 2);
    rel.operands()[0]->accept(*this);
  }

  virtual void visit(expr::constant const&)
  {
    /* Do exactly nothing. */
  }

private:
  bool positive_ = false;
};

} // namespace


index::index(std::string directory)
  : dir_(directory)
{
  chaining(false);
  init_state = (
      on(atom("load")) >> [=]
      {
        VAST_LOG_VERBOSE("spawning index @" << id());
        if (! exists(dir_))
        {
          VAST_LOG_INFO("index @" << id() << " creates new directory " << dir_);
          mkdir(dir_);
        }

        assert(exists(dir_));
        traverse(
            dir_,
            [&](path const& p) -> bool
            {
              VAST_LOG_VERBOSE("index @" << id() << " found file " << p);

              // TODO:
              //fs::ifstream file(p, std::ios::binary | std::ios::in);
              //serialization::stream_iarchive ia(file);
              //segment::header hdr;
              //ia >> hdr;
              //build(hdr);
              return true;
            });
      },
      on(atom("create"), arg_match) >> [=](schema const& sch)
      {
        for (auto& e : sch.events())
        {
          if (! e.indexed)
            continue;

          VAST_LOG_DEBUG("index @" << id() <<
                         " creates index for event " << e.name);

          for (auto& arg : e.args)
          {
            if (! arg.indexed)
              continue;

            VAST_LOG_DEBUG("index @" << id() <<
                           " creates index for argument " << arg.name);
          }
        }
      },
      on(atom("hit"), atom("all")) >> [=]
      {
        // TODO: see whether we still need this.
        reply(atom("miss"));
      },
      on(atom("hit"), arg_match) >> [=](expression const& expr)
      {
        checker chkr;
        expr.accept(chkr);
        if (! chkr)
        {
          reply(atom("impossible"));
          return;
        }

        // TODO: parse expression and hit indexes.
      },
      on(arg_match) >> [=](segment const& s)
      {
        // TODO: index each event in segment.
        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("kill")) >> [=]()
      {
        quit();
        VAST_LOG_VERBOSE("index @" << id() << " terminated");
      });
}

} // namespace vast
