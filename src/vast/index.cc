#include "vast/index.h"

#include "vast/bitmap_index.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/expression.h"
#include "vast/partition.h"

using namespace cppa;

namespace vast {
namespace {

class query_builder : public expr::const_visitor
{
public:
  query_builder(actor_ptr sink, std::vector<any_tuple>& cmds)
    : sink_(sink),
      commands_(cmds)
  {
  }

  virtual void visit(expr::node const&) { }

  virtual void visit(expr::timestamp_extractor const&)
  {
    assert(current_value_);
    commands_.push_back(
        make_any_tuple(
            atom("lookup"),
            atom("time"),
            regex{".*"},
            current_operator_,
            current_value_,
            sink_));
  }

  virtual void visit(expr::name_extractor const&)
  {
    VAST_LOG_DEBUG("building lookup with name for '" << current_value_ << "'" <<
                   " under " << current_operator_);
    assert(current_value_);
    commands_.push_back(
        make_any_tuple(
            atom("lookup"),
            atom("name"),
            regex{".*"},
            current_operator_,
            current_value_,
            sink_));
  }

  virtual void visit(expr::id_extractor const&)
  {
    // TODO: construct bitstream that represents the RHS.
  }

  virtual void visit(expr::offset_extractor const& oe)
  {
    assert(current_value_);
    VAST_LOG_DEBUG("building lookup with offset " << oe.off() <<
                   " for '" << current_value_ << "'" <<
                   " under " << current_operator_);
    commands_.push_back(
        make_any_tuple(
            atom("lookup"),
            atom("offset"),
            regex{".*"},
            current_operator_,
            current_value_,
            oe.off(),
            sink_));
  }

  virtual void visit(expr::type_extractor const& te)
  {
    VAST_LOG_DEBUG("building lookup with type " << te.type() <<
                   " for '" << current_value_ << "'" <<
                   " under " << current_operator_);
    assert(current_value_);
    assert(te.type() == current_value_.which());
    commands_.push_back(
        make_any_tuple(
            atom("lookup"),
            atom("type"),
            regex{".*"},
            current_operator_,
            current_value_,
            sink_));
  }

  virtual void visit(expr::conjunction const& conj)
  {
    // TODO: create one actor per clause
    for (auto& op : conj.operands())
      op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    // TODO: create one actor per clause
    for (auto& op : disj.operands())
      op->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(rel.operands().size() == 2);
    current_operator_ = rel.type();
    // FIXME: We currently require that values appear on the RHS and extractor
    // nodes on the LHS of the clause.
    rel.operands()[1]->accept(*this);
    rel.operands()[0]->accept(*this);
    current_value_ = invalid;
  }

  virtual void visit(expr::constant const& c)
  {
    assert(c.ready());
    current_value_ = c.result();
  }

private:
  value current_value_;
  relational_operator current_operator_;
  actor_ptr sink_;
  std::vector<any_tuple>& commands_;
};


} // namespace <anonymous>


index::index(path directory)
  : dir_{std::move(directory)}
{
}

void index::init()
{
  VAST_LOG_ACT_VERBOSE("index", "spawned");

  become(
      on(atom("kill")) >> [=]
      {
        for (auto p : partitions_)
          p.second << last_dequeued();
        quit();
      },
      on(atom("lookup"), arg_match) >> [=](expression const& expr)
      {
        std::vector<any_tuple> commands;
        query_builder builder{last_sender(), commands};
        expr.accept(builder);
        // FIXME: we support only one partition for now.
        assert(! partitions_.empty());
        auto part = partitions_.begin()->second;
        VAST_LOG_ACT_DEBUG("index", "sends " << commands.size() <<
                           " commands to partition " <<
                           partitions_.begin()->first);
        for (auto& cmd : commands)
          part << cmd;
      },
      on(arg_match) >> [=](segment const& s)
      {
        assert(active_);
        VAST_LOG_ACT_DEBUG("index", "sending events from segment " << s.id() <<
                           " to " << VAST_ACTOR("partition", active_));

        segment::reader r{&s};
        event e;
        while (r.read(e))
          send(active_, std::move(e));

        reply(atom("segment"), atom("ack"), s.id());
      });

  // FIXME: There's a libcppa problem with sync_send's following a become()
  // statement. If we copy the entire body of load() into this context, then
  // the sync_send replies won't arrive. But if we go through a function call,
  // it works. Weird. Race?
  load();
}

void index::on_exit()
{
  VAST_LOG_ACT_VERBOSE("index", "terminated");
}

void index::load()
{
  if (! exists(dir_))
  {
    VAST_LOG_ACT_INFO("index", "creates new directory " << dir_);
    if (! mkdir(dir_))
    {
      VAST_LOG_ACT_ERROR("index", "failed to create " << dir_);
      quit();
    }
  }
  else
  {
    auto latest = std::make_shared<time_point>(0);
    traverse(
        dir_,
        [&](path const& p) -> bool
        {
          VAST_LOG_ACT_VERBOSE("index", "found partition " << p);
          auto part = spawn<partition>(p);
          auto id = uuid{to_string(p.basename())};
          partitions_.emplace(id, part);

          sync_send(part, atom("meta"), atom("timestamp")).then(
              on_arg_match >> [=](time_point tp)
              {
                if (tp >= *latest)
                {
                  VAST_LOG_ACT_DEBUG("index", "marked partition " << p <<
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
    auto p = spawn<partition>(dir_ / to<string>(id));
    active_ = p;
    partitions_.emplace(std::move(id), std::move(p));
  }
}

} // namespace vast
