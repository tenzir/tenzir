#include "vast/index.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"

using namespace cppa;

namespace vast {
namespace {

// Extracts all leaf relations from the AST.
class relationizer : public expr::default_const_visitor
{
public:
  relationizer(std::vector<expr::ast>& relations)
    : relations_{relations}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& op : conj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& op : disj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    relations_.emplace_back(rel);
  }

private:
  std::vector<expr::ast>& relations_;
};

} // namespace <anonymous>


index::index(path directory)
  : dir_{std::move(directory)}
{
}

char const* index::description() const
{
  return "index";
}

void index::act()
{
  chaining(false);
  become(
      on_arg_match >> [=](expr::ast const& ast)
      {
        std::vector<expr::ast> relations;
        relationizer visitor{relations};
        ast.accept(visitor);
        // FIXME: Support more than 1 partition.
        auto part = partitions_.begin()->second;
        VAST_LOG_ACTOR_DEBUG("sends " << relations.size() <<
                             " ASTs to partition @" << part->id());
        for (auto& rel : relations)
          send(part, std::move(rel), last_sender());
      },
      on(arg_match) >> [=](segment const& s)
      {
        assert(active_);
        reply(atom("segment"), atom("ack"), s.id());
        VAST_LOG_ACTOR_DEBUG("sending events from segment " << s.id() <<
                             " to " << VAST_ACTOR("partition", active_));
        forward_to(active_);
      });

  // FIXME: There's a libcppa problem with sync_send's following a become()
  // statement. If we copy the entire body of load() into this context, then
  // the sync_send replies won't arrive. But if we go through a function call,
  // it works. Weird. Race?
  load();
}

void index::load()
{
  if (! exists(dir_))
  {
    VAST_LOG_ACTOR_INFO("creates new directory " << dir_);
    if (! mkdir(dir_))
    {
      VAST_LOG_ACTOR_ERROR("failed to create " << dir_);
      quit(exit::error);
      return;
    }
  }
  else
  {
    auto latest = std::make_shared<time_point>(0);
    traverse(
        dir_,
        [&](path const& p) -> bool
        {
          VAST_LOG_ACTOR_VERBOSE("found partition " << p);
          auto part = spawn<partition, linked>(p);
          auto id = uuid{to_string(p.basename())};
          partitions_.emplace(id, part);

          sync_send(part, atom("timestamp")).then(
              on_arg_match >> [=](time_point tp)
              {
                if (tp >= *latest)
                {
                  VAST_LOG_ACTOR_DEBUG("marked partition " << p <<
                                       " as active (" << tp << ")");
                  *latest = tp;
                  active_ = part;
                }
              });

          return true;
        });
  }

  if (partitions_.empty())
  {
    auto id = uuid::random();
    auto p = spawn<partition, linked>(dir_ / to<string>(id));
    active_ = p;
    partitions_.emplace(std::move(id), std::move(p));
  }
}

} // namespace vast
