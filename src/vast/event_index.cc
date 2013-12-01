#include "vast/event_index.h"

#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/port.h"
#include "vast/util/convert.h"

using namespace cppa;

namespace vast {

struct event_meta_index::loader : expr::default_const_visitor
{
  loader(event_meta_index& idx)
    : idx{idx}
  {
  }

  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::name_extractor const&)
  {
    if (idx.exists_ && idx.name_.empty())
    {
      io::unarchive(idx.dir_ / "name.idx", idx.name_);
      VAST_LOG_DEBUG(
          "loaded name index (" << idx.name_.size() << " bits)");
    }
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    if (idx.exists_ && idx.timestamp_.empty())
    {
      io::unarchive(idx.dir_ / "timestamp.idx", idx.timestamp_);
      VAST_LOG_DEBUG(
          "loaded time index (" << idx.timestamp_.size() << " bits)");
    }
  }

  virtual void visit(expr::id_extractor const&)
  {
    assert(! "not yet implemented");
  }

  event_meta_index& idx;
};

struct event_meta_index::querier : expr::default_const_visitor
{
  querier(event_meta_index const& idx)
    : idx{idx}
  {
  }

  virtual void visit(expr::constant const& c)
  {
    val = &c.val;
  }

  virtual void visit(expr::predicate const& pred)
  {
    op = &pred.op;
    pred.rhs().accept(*this);
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::name_extractor const&)
  {
    assert(op);
    assert(val);
    if (auto r = idx.name_.lookup(*op, *val))
      result = std::move(*r);
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    assert(op);
    assert(val);
    if (auto r = idx.timestamp_.lookup(*op, *val))
      result = std::move(*r);
  }

  virtual void visit(expr::id_extractor const&)
  {
    assert(! "not yet implemented");
  }

  bitstream result;
  event_meta_index const& idx;
  value const* val = nullptr;
  relational_operator const* op = nullptr;
};


event_meta_index::event_meta_index(path dir)
  : event_index<event_meta_index>{std::move(dir)}
{
  // ID 0 is not a valid event.
  timestamp_.append(1, false);
  name_.append(1, false);
}

char const* event_meta_index::description() const
{
  return "event-meta-index";
}

void event_meta_index::scan()
{
  if (exists(dir_ / "name.idx") || exists(dir_ / "timestamp.idx"))
    exists_ = true;
}

void event_meta_index::load(expr::ast const& ast)
{
  loader visitor{*this};
  ast.accept(visitor);
}

void event_meta_index::store()
{
  if (timestamp_.appended() > 1)
  {
    if (! exists(dir_))
      mkdir(dir_);
    io::archive(dir_ / "timestamp.idx", timestamp_);
    VAST_LOG_ACTOR_DEBUG(
        "stored timestamp index (" << timestamp_.size() << " bits)");
  }
  if (name_.appended() > 1)
  {
    if (! exists(dir_))
      mkdir(dir_);
    io::archive(dir_ / "name.idx", name_);
    VAST_LOG_ACTOR_DEBUG(
        "stored name index (" << name_.size() << " bits)");
  }
}

bool event_meta_index::index(event const& e)
{
  return timestamp_.push_back(e.timestamp(), e.id())
      && name_.push_back(e.name(), e.id());
}

bitstream event_meta_index::lookup(expr::ast const& ast) const
{
  querier visitor{*this};
  ast.accept(visitor);
  return std::move(visitor.result);
}


struct event_arg_index::loader : expr::default_const_visitor
{
  loader(event_arg_index& idx, value_type type)
    : idx{idx},
      type_{type}
  {
  }

  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::offset_extractor const& oe)
  {
    if (idx.args_.count(oe.off))
      return;

    auto filename = idx.pathify(oe.off);
    if (! exists(filename))
      return;

    value_type vt;
    io::unarchive(filename, vt);
    if (vt != type_)
    {
      VAST_LOG_WARN("type mismatch: requested " << type_ << ", got " << vt);
      return;
    }

    std::shared_ptr<bitmap_index> bmi;
    io::unarchive(filename, vt, bmi);
    if (! bmi)
    {
      VAST_LOG_ERROR("got corrupt index: " << filename.basename());
      return;
    }

    VAST_LOG_DEBUG("loaded index " << filename.trim(-4) <<
                   " (" << bmi->size() << " bits)");

    idx.args_.emplace(oe.off, bmi);
  }

  virtual void visit(expr::type_extractor const& te)
  {
    auto t = te.type;
    if (idx.types_.count(t))
      return;

    auto er = idx.files_.equal_range(t);
    for (auto i = er.first; i != er.second; ++i)
    {
      offset o;
      auto str = i->second.basename(true).str().substr(1);
      auto start = str.begin();
      if (! extract(start, str.end(), o))
      {
        VAST_LOG_ERROR("got invalid offset in path: " << i->second);
        return;
      }

      if (idx.args_.count(o))
        // We have issued an offset query in the past and loaded the
        // corresponding index already.
        return;

      value_type vt;
      std::shared_ptr<bitmap_index> bmi;
      io::unarchive(i->second, vt, bmi);
      if (! bmi)
      {
        VAST_LOG_ERROR("got corrupt index: " << i->second.basename());
        return;
      }

      VAST_LOG_DEBUG("loaded index " << i->second.trim(-4) << " (" <<
                     bmi->size() << " bits)");

      assert(! idx.args_.count(o));
      idx.args_.emplace(o, bmi);
      idx.types_.emplace(vt, bmi);
    }
  }

  event_arg_index& idx;
  value_type type_;
};

struct event_arg_index::querier : expr::default_const_visitor
{
  querier(event_arg_index const& idx)
    : idx{idx}
  {
  }

  virtual void visit(expr::constant const& c)
  {
    val = &c.val;
  }

  virtual void visit(expr::predicate const& pred)
  {
    op = &pred.op;
    pred.rhs().accept(*this);
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::offset_extractor const& oe)
  {
    assert(op);
    assert(val);

    auto i = idx.args_.find(oe.off);
    if (i == idx.args_.end())
      return;

    if (auto r = i->second->lookup(*op, *val))
      result = std::move(*r);
  }

  virtual void visit(expr::type_extractor const& te)
  {
    assert(op);
    assert(val);
    assert(te.type == val->which());

    auto i = idx.types_.find(te.type);
    if (i == idx.types_.end())
      return;

    auto er = idx.types_.equal_range(te.type);
    for (auto j = er.first; j != er.second; ++j)
    {
      if (auto r = j->second->lookup(*op, *val))
      {
        if (result)
          result |= bitstream{std::move(*r)};
        else
          result = std::move(*r);
      }
    }
  }

  bitstream result;
  event_arg_index const& idx;
  value const* val = nullptr;
  relational_operator const* op = nullptr;
};


event_arg_index::event_arg_index(path dir)
  : event_index<event_arg_index>{std::move(dir)}
{
}

char const* event_arg_index::description() const
{
  return "event-arg-index";
}

void event_arg_index::scan()
{
  if (exists(dir_))
  {
    traverse(
        dir_,
        [&](path const& p) -> bool
        {
          value_type vt;
          io::unarchive(p, vt);
          files_.emplace(vt, p);
          return true;
        });

    assert(! files_.empty());
  }
}

namespace {

struct type_finder : expr::default_const_visitor
{
  virtual void visit(expr::predicate const& pred)
  {
    pred.rhs().accept(*this);
  }

  virtual void visit(expr::constant const& c)
  {
    type = c.val.which();
  }

  value_type type;
};

} // namespace <anonymous>

void event_arg_index::load(expr::ast const& ast)
{
  type_finder tf;
  ast.accept(tf);

  loader visitor{*this, tf.type};
  ast.accept(visitor);
}

void event_arg_index::store()
{
  VAST_LOG_ACTOR_DEBUG("saves indexes to filesystem");

  std::map<std::shared_ptr<bitmap_index>, value_type> inverse;
  for (auto& p : types_)
    if (inverse.find(p.second) == inverse.end())
      inverse.emplace(p.second, p.first);

  for (auto& p : args_)
  {
    if (p.second->empty() || p.second->appended() == 0)
      continue;
    if (! exists(dir_))
      mkdir(dir_);
    path const filename = pathify(p.first);
    assert(inverse.count(p.second));
    io::archive(filename, inverse[p.second], p.second);
    VAST_LOG_ACTOR_DEBUG("stored index " << filename.trim(-4) <<
                         " (" << p.second->size() << " bits)");
  }
}

bool event_arg_index::index(event const& e)
{
  if (e.empty())
    return true;
  offset o{0};
  return index_record(e, e.id(), o);
}

bitstream event_arg_index::lookup(expr::ast const& ast) const
{
  querier visitor{*this};
  ast.accept(visitor);
  return std::move(visitor.result);
}

path event_arg_index::pathify(offset const& o) const
{
  static string prefix{"@"};
  static string suffix{".idx"};
  return dir_ / (prefix + to<string>(o) + suffix);
}

bool event_arg_index::index_record(record const& r, uint64_t id, offset& o)
{
  if (o.empty())
    return true;
  for (auto& v : r)
  {
    if (v.which() == record_type && v)
    {
      auto& inner = v.get<record>();
      if (! inner.empty())
      {
        o.push_back(0);
        if (! index_record(inner, id, o))
          return false;
        o.pop_back();
      }
    }
    else if (! v.invalid() && v.which() != table_type)
    {
      bitmap_index* idx;
      auto i = args_.find(o);
      if (i != args_.end())
      {
        idx = i->second.get();
      }
      else
      {
        auto unique = bitmap_index::create(v.which());
        auto bmi = std::shared_ptr<bitmap_index>{unique.release()};
        idx = bmi.get();
        idx->append(1, false); // ID 0 is not a valid event.
        args_.emplace(o, bmi);
        types_.emplace(v.which(), std::move(bmi));
      }
      assert(idx != nullptr);
      if (! idx->push_back(v, id))
        return false;
    }
    ++o.back();
  }
  return true;
}

} // namespace vast
