#include "vast/index.h"

#include <ze.h>
#include "vast/logger.h"
#include "vast/fs/operations.h"
#include "vast/fs/fstream.h"
#include "vast/expression.h"

namespace vast {
namespace detail {

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

  virtual void visit(expr::name_extractor const& node)
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

  virtual void visit(expr::relational_operator const& op)
  {
    assert(op.operands().size() == 2);
    op.operands()[0]->accept(*this);
  }

  virtual void visit(expr::constant const& c)
  {
    /* Do exactly nothing. */
  }

private:
  bool positive_ = false;
};


/// Visits an expression and hits the meta index.
class hitter : public expr::const_visitor
{
public:
  /// Constructs a hitter with an index actor.
  hitter(index::meta const& m, std::vector<ze::uuid>& ids)
    : meta_(m)
    , ids_(ids)
  {
  }

  virtual void visit(expr::node const&)
  {
    assert(! "should never happen");
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    assert(rhs_.which() == ze::time_point_type);
    assert(op_ != nullptr);

    switch (op_->type())
    {
      default:
        assert(! "invalid time extractor operator");
        break;
      case expr::equal:
        for (auto& i : meta_.ranges)
          if (rhs_ >= i.first.first && rhs_ <= i.first.second)
            ids_.push_back(i.second);
        break;
      case expr::not_equal:
        for (auto& i : meta_.ranges)
          if (rhs_ < i.first.first || rhs_ > i.first.second)
            ids_.push_back(i.second);
        break;
      case expr::less:
        for (auto& i : meta_.ranges)
          if (rhs_ > i.first.first)
            ids_.push_back(i.second);
        break;
      case expr::less_equal:
        for (auto& i : meta_.ranges)
          if (rhs_ >= i.first.first)
            ids_.push_back(i.second);
        break;
      case expr::greater:
        for (auto& i : meta_.ranges)
          if (rhs_ < i.first.second)
            ids_.push_back(i.second);
        break;
      case expr::greater_equal:
        for (auto& i : meta_.ranges)
          if (rhs_ <= i.first.second)
            ids_.push_back(i.second);
        break;
    }
  }

  virtual void visit(expr::name_extractor const& node)
  {
    assert(rhs_.which() == ze::string_type || rhs_.which() == ze::regex_type);
    assert(op_ != nullptr);

    for (auto& i : meta_.names)
      if (op_->test(i.first, rhs_))
        ids_.push_back(i.second);
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
    std::vector<ze::uuid> ids;

    // The first intersection operand has a free shot.
    auto i = conj.operands().begin();
    hitter p(meta_, ids);
    (*i)->accept(p);
    std::sort(ids.begin(), ids.end());

    for (++i; i != conj.operands().end(); ++i)
    {
      std::vector<ze::uuid> result;
      hitter p(meta_, result);
      (*i)->accept(p);

      std::sort(result.begin(), result.end());
      std::vector<ze::uuid> intersection;

      std::set_intersection(ids.begin(), ids.end(),
                     result.begin(), result.end(),
                     std::back_inserter(intersection));

      intersection.swap(ids);
    }

    ids_.swap(ids);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    std::vector<ze::uuid> ids;
    for (auto& operand : disj.operands())
    {
      std::vector<ze::uuid> result;
      hitter p(meta_, result);
      operand->accept(p);

      std::sort(result.begin(), result.end());
      std::vector<ze::uuid> unification;

      std::set_union(ids.begin(), ids.end(),
                     result.begin(), result.end(),
                     std::back_inserter(unification));

      unification.swap(ids);
    }

    ids_.swap(ids);
  }

  virtual void visit(expr::relational_operator const& op)
  {
    assert(op_ == nullptr);
    assert(rhs_ == ze::invalid);
    assert(op.operands().size() == 2);

    op_ = &op;
    op.operands()[1]->accept(*this);
    op.operands()[0]->accept(*this);

    op_ = nullptr;
    rhs_ = ze::invalid;
  }

  virtual void visit(expr::constant const& c)
  {
    assert(c.ready());
    rhs_ = c.result();
  }

private:
  expr::relational_operator const* op_ = nullptr;
  ze::value rhs_ = ze::invalid;
  index::meta const& meta_;
  std::vector<ze::uuid>& ids_;
};

} // namespace detail

index::index(cppa::actor_ptr archive, std::string directory)
  : dir_(std::move(directory))
  , archive_(archive)
{
  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("load")) >> [=]
      {
        LOG(verbose, index) << "spawning index @" << id();
        if (! fs::exists(dir_))
        {
          LOG(info, index)
            << "index @" << id() << " creates new directory " << dir_;
          fs::mkdir(dir_);
        }

        assert(fs::exists(dir_));
        fs::each_file_entry(
            dir_,
            [&](fs::path const& p)
            {
              fs::ifstream file(p, std::ios::binary | std::ios::in);
              ze::serialization::stream_iarchive ia(file);
              segment::header hdr;
              ia >> hdr;
              build(hdr);
            });
      },
      on(atom("hit"), atom("all")) >> [=]
      {
        if (ids_.empty())
        {
          reply(atom("miss"));
          return;
        }

        reply(atom("hit"), ids_);
      },
      on(atom("hit"), arg_match) >> [=](expression const& expr)
      {
        detail::checker checker;
        expr.accept(checker);
        if (! checker || ids_.empty())
        {
          reply(atom("impossible"));
          return;
        }

        std::vector<ze::uuid> ids;
        detail::hitter hitter(meta_, ids);
        expr.accept(hitter);

        if (ids.empty())
          reply(atom("miss"));
        else
          reply(atom("hit"), std::move(ids));

      },
      on(arg_match) >> [=](segment const& s)
      {
        write(s);
        build(s.head());
        reply(atom("index"), atom("segment"), atom("ack"), s.id());
      },
      on(atom("shutdown")) >> [=]()
      {
        quit();
        LOG(verbose, index) << "index @" << id() << " terminated";
      });
}

void index::write(segment const& s)
{
  auto path = fs::path(dir_) / ze::to_string(s.id());
  fs::ofstream file(path, std::ios::binary | std::ios::out);
  ze::serialization::stream_oarchive oa(file);
  oa << s.head();

  LOG(verbose, index)
    << "index @" << id() << " wrote segment header to " << path;
}

void index::build(segment::header const& hdr)
{
  LOG(debug, index) << "index @" << id()
    << " builds in-memory indexes for segment " << hdr.id;

  assert(ids_.count(hdr.id) == 0);
  ids_.insert(hdr.id);

// TODO: Remove as soon as newer GCC versions have adopted r181022.
#ifdef __clang__
  for (auto& event : hdr.event_names)
    meta_.names.emplace(event, hdr.id);

#else
  for (auto& event : hdr.event_names)
    meta_.names.insert({event, hdr.id});
#endif

  meta_.ranges.insert({{hdr.start, hdr.end}, hdr.id});
}

} // namespace vast
