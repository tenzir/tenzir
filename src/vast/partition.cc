#include "vast/partition.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/segment.h"
#include "vast/segment_pack.h"
#include "vast/task_tree.h"
#include "vast/io/serialization.h"

namespace vast {

path const partition::part_meta_file = "partition.meta";

//namespace {
//
//// If the given predicate is a name extractor, extracts the name.
//struct name_checker : expr::default_const_visitor
//{
//  virtual void visit(expr::predicate const& pred)
//  {
//    pred.lhs().accept(*this);
//    if (check_)
//      pred.rhs().accept(*this);
//  }
//
//  virtual void visit(expr::name_extractor const&)
//  {
//    check_ = true;
//  }
//
//  virtual void visit(expr::constant const& c)
//  {
//    value_ = c.val;
//  }
//
//  bool check_ = false;
//  value value_;
//};
//
//// Partitions the AST predicates into meta and data predicates. Moreover, it
//// assigns each data predicate a name if a it coexists with name-extractor
//// predicate that restricts the scope of the data predicate.
//struct predicatizer : expr::default_const_visitor
//{
//  virtual void visit(expr::conjunction const& conj)
//  {
//    std::vector<expr::ast> flat;
//    std::vector<expr::ast> deep;
//
//    // Partition terms into leaves and others.
//    for (auto& operand : conj.operands)
//    {
//      auto op = expr::ast{*operand};
//      if (op.is_predicate())
//        flat.push_back(std::move(op));
//      else
//        deep.push_back(std::move(op));
//    }
//
//    // Extract the name for all predicates in this conjunction.
//    //
//    // TODO: add a check to the semantic pass after constructing queries to
//    // flag multiple name/offset/time extractors in the same conjunction.
//    name_.clear();
//    for (auto& pred : flat)
//    {
//      name_checker checker;
//      pred.accept(checker);
//      if (checker.check_)
//        name_ = checker.value_.get<string>();
//    }
//
//    for (auto& pred : flat)
//      pred.accept(*this);
//
//    // Proceed with the remaining conjunctions deeper in the tree.
//    name_.clear();
//    for (auto& n : deep)
//      n.accept(*this);
//  }
//
//  virtual void visit(expr::disjunction const& disj)
//  {
//    for (auto& operand : disj.operands)
//      operand->accept(*this);
//  }
//
//  virtual void visit(expr::predicate const& pred)
//  {
//    if (! expr::ast{pred}.is_meta_predicate())
//      data_predicates_.emplace(name_, pred);
//    else
//      meta_predicates_.push_back(pred);
//  }
//
//  string name_;
//  std::multimap<string, expr::ast> data_predicates_;
//  std::vector<expr::ast> meta_predicates_;
//};
//
//} // namespace <anonymous>

partition::meta_data::meta_data(uuid id)
  : id{id}
{
}

void partition::meta_data::update(segment const& s)
{
  if (first_event == time_range{} || s.first() < first_event)
    first_event = s.first();

  if (last_event == time_range{} || s.last() > last_event)
    last_event = s.last();

  last_modified = now();
}

void partition::meta_data::serialize(serializer& sink) const
{
  sink << id << first_event << last_event << last_modified;
}

void partition::meta_data::deserialize(deserializer& source)
{
  source >> id >> first_event >> last_event >> last_modified;
}

bool operator==(partition::meta_data const& x, partition::meta_data const& y)
{
  return x.id == y.id
      && x.first_event == y.first_event
      && x.last_event == y.last_event
      && x.last_modified == y.last_modified;
}


partition::partition(uuid id)
  : meta_{std::move(id)}
{
}

void partition::update(segment const& s)
{
  meta_.update(s);
}

partition::meta_data const& partition::meta() const
{
  return meta_;
}

void partition::serialize(serializer& sink) const
{
  sink << meta_;
}

void partition::deserialize(deserializer& source)
{
  source >> meta_;
}


using namespace caf;

namespace {

struct dispatcher : expr::default_const_visitor
{
  dispatcher(partition_actor& pa)
    : actor_{pa}
  {
  }

  virtual void visit(expr::predicate const& pred)
  {
    op_ = pred.op;
    pred.rhs().accept(*this);
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    auto p = actor_.dir_ / "time.idx";
    auto& s = actor_.indexers_[p];
    if (! s.actor)
      s.actor = actor_.spawn<event_time_indexer<default_bitstream>, monitored>(
          std::move(p));

    indexes_.push_back(s.actor);
  }

  virtual void visit(expr::name_extractor const&)
  {
    auto p = actor_.dir_ / "name.idx";
    auto& s = actor_.indexers_[p];
    if (! s.actor)
      s.actor = actor_.spawn<event_name_indexer<default_bitstream>, monitored>(
          std::move(p));

    indexes_.push_back(s.actor);
  }

  virtual void visit(expr::type_extractor const& te)
  {
    for (auto& p : actor_.indexers_)
      if (is<type::record>(p.second.type))
      {
        assert(! "records shall be flattened");
      }
      else
      {
        if (p.second.type == te.type)
        {
          if (p.second.actor)
          {
            indexes_.push_back(p.second.actor);
          }
          else
          {
            auto a = actor_.load_data_indexer(p.first);
            if (! a)
              VAST_LOG_ERROR(a.error());
            else
              indexes_.push_back(p.second.actor);
          }
        }
      }
  }

  virtual void visit(expr::schema_extractor const& se)
  {
    for (auto& t : actor_.schema_)
      if (auto r = get<type::record>(t))
      {
        for (auto& pair : r->find_suffix(se.key))
        {
          path p = "types";
          for (auto& i : pair.second)
            p /= i;

          auto i = actor_.indexers_.find(p);
          if (i == actor_.indexers_.end())
          {
            VAST_LOG_WARN("no index for " << p);
          }
          else if (auto r = get<type::record>(i->second.type))
          {
            auto lhs_type = r->at(i->second.off);
            assert(lhs_type);
            if (! compatible(*lhs_type, op_, rhs_type_))
              VAST_LOG_WARN("incompatible types: LHS = " << *lhs_type <<
                            " <--> RHS = " << rhs_type_);
          }
          else if (! i->second.actor)
          {
            VAST_LOG_DEBUG("loading indexer for " << p);
            auto a = actor_.load_data_indexer(i->first);
            if (! a)
              VAST_LOG_ERROR(a.error());
            else
              indexes_.push_back(i->second.actor);
          }
          else
          {
            VAST_LOG_DEBUG("found loaded indexer for " << p);
            indexes_.push_back(i->second.actor);
          }
        }
      }
      else
      {
        assert(! "all events must currently be records");
      }
  }

  virtual void visit(expr::constant const& c)
  {
    rhs_type_ = c.val.type();
  }

  relational_operator op_;
  type rhs_type_;
  partition_actor& actor_;
  std::vector<actor> indexes_;
};

} // namespace <anonymous>

partition_actor::partition_actor(path dir, size_t batch_size, uuid id)
  : dir_{std::move(dir)},
    batch_size_{batch_size},
    partition_{std::move(id)}
{
}

message_handler partition_actor::act()
{
  trap_exit(true);

  if (exists(dir_))
  {
    auto t = io::unarchive(dir_ / partition::part_meta_file, partition_);
    if (! t)
    {
      VAST_LOG_ACTOR_ERROR("failed to load partition meta data from " <<
                           dir_ << ": " << t.error().msg());
      quit(exit::error);
      return {};
    }

    t = io::unarchive(dir_ / "schema", schema_);
    if (! t)
    {
      VAST_LOG_ACTOR_ERROR("failed to load schema: " << t.error());
      quit(exit::error);
      return {};
    }

    indexers_[dir_ / "name.idx"];
    indexers_[dir_ / "type.idx"];

    auto init_state = [this](path const& p, offset const& o, type const& t)
    {
      assert(indexers_.find(p) == indexers_.end());
      auto& state = indexers_[p];
      state.off = o;
      state.type = t;
    };

    for (auto& tp : schema_)
    {
      auto p = path{"types"} / tp.name();
      if (auto r = get<type::record>(tp))
        r->each(
            [&](type::record::trace const& t, offset const& o) -> trial<void>
            {
              auto ip = p;
              for (auto f : t)
                ip /= f->name;

              init_state(ip, o, t.back()->type);
              return nothing;
            });
      else
        init_state(p, {}, tp);
    }
  }

  attach_functor(
      [=](uint32_t reason)
      {
        if (unpacker_)
          anon_send_exit(unpacker_, reason);

        for (auto& p : indexers_)
          if (p.second.actor)
            anon_send_exit(p.second.actor, reason);

        indexers_.clear();
        unpacker_ = invalid_actor;
      });


  send(this, atom("stats"), atom("show"));

  return
  {
    [=](exit_msg const& e)
    {
      if (e.reason == exit::kill)
      {
        quit(e.reason);
        return;
      }

      // Wait for unpacker to finish.
      if (exit_reason_ == 0 && ! segments_.empty())
      {
        exit_reason_ = e.reason;
        return;
      }

      // FIXME: ensure this happens at most once.
      auto tree = spawn<task_tree>(this);
      send(tree, atom("notify"), this);
      send(tree, this, this);
      send(this, atom("flush"), tree);
      exit_reason_ = e.reason;
    },
    on(atom("done")) >> [=]
    {
      // We only spawn one task tree upon exiting. Once we get notified we can
      // safely terminate with the last exit reason.
      quit(exit_reason_);
    },
    [=](down_msg const&)
    {
      VAST_LOG_ACTOR_DEBUG("got DOWN from " << last_sender());

      for (auto& p : indexers_)
        if (p.second.actor == last_sender())
        {
          p.second.actor = invalid_actor;
          p.second.stats = {};
          return;
        }
    },
    [=](expr::ast const& pred, actor idx)
    {
      VAST_LOG_ACTOR_DEBUG("got predicate " << pred);

      dispatcher d{*this};
      pred.accept(d);

      uint64_t n = d.indexes_.size();
      send(idx, pred, partition_.meta().id, n);

      if (n == 0)
      {
        VAST_LOG_ACTOR_DEBUG("did not find a matching indexer for " << pred);
        send(idx, pred, partition_.meta().id, bitstream{});
      }
      else
      {
        auto t = make_message(pred, partition_.meta().id, idx);
        for (auto& a : d.indexes_)
          send_tuple(a, t);
      }
    },
    on(atom("flush"), arg_match) >> [=](actor tree)
    {
      VAST_LOG_ACTOR_DEBUG("got request to flush its indexes");

      for (auto& p : indexers_)
        if (p.second.actor)
        {
          send(tree, this, p.second.actor);
          send(p.second.actor, atom("flush"), tree);
        }

      auto t = io::archive(dir_ / "schema", schema_);
      if (! t)
      {
        VAST_LOG_ACTOR_ERROR("failed to save schema for " << dir_ << ": " <<
                             t.error().msg());
        quit(exit::error);
        return;
      }

      t = io::archive(dir_ / partition::part_meta_file, partition_);
      if (! t)
      {
        VAST_LOG_ACTOR_ERROR("failed to save partition " << dir_ <<
                             ": " << t.error().msg());
        quit(exit::error);
        return;
      }

      send(tree, atom("done"));
    },
    [=](segment const& s)
    {
      VAST_LOG_ACTOR_DEBUG("got segment with " << s.events() << " events");

      auto sch = schema::merge(schema_, s.schema());
      if (! sch)
      {
        VAST_LOG_ACTOR_ERROR("failed to merge schema: " << sch.error());
        VAST_LOG_ACTOR_ERROR("ignores segment " << s.id());
        quit(exit::error);
        return;
      }

      schema_ = std::move(*sch);

      auto create = [this](path const& p, offset const& o, type const& t)
        -> trial<void>
      {
        auto i = create_data_indexer(p, o, t);
        if (! i)
        {
          VAST_LOG_ACTOR_ERROR(i.error());
          quit(exit::error);
          return i;
        }

        return nothing;
      };

      for (auto& tp : schema_)
      {
        auto p = path{"types"} / tp.name();
        if (auto r = get<type::record>(tp))
          r->each(
              [&](type::record::trace const& t, offset const& o) -> trial<void>
              {
                auto ip = p;
                for (auto f : t)
                  ip /= f->name;

                return create(ip, o, t.back()->type);
              });
        else
          create(p, {}, tp);
      }

      auto p = dir_ / "name.idx";
      auto state = &indexers_[p];
      if (! state->actor)
        state->actor =
          spawn<event_name_indexer<default_bitstream>, monitored>(std::move(p));

      p = dir_ / "time.idx";
      state = &indexers_[p];
      if (! state->actor)
        state->actor =
          spawn<event_time_indexer<default_bitstream>, monitored>(std::move(p));

      segments_.push(last_dequeued());
      send(this, atom("unpack"));
    },
    [=](std::vector<event> const& events)
    {
      for (auto& p : indexers_)
        if (p.second.actor)
        {
          send_tuple(p.second.actor, last_dequeued());
          p.second.stats.backlog += events.size();
        }
    },
    on(atom("unpack")) >> [=]
    {
      if (! unpacker_)
      {
        if (! segments_.empty())
        {
          VAST_LOG_ACTOR_DEBUG("begins unpacking segment " <<
                               segments_.front().get_as<segment>(0).id());

          unpacker_ = spawn<unpacker>(segments_.front(), this, batch_size_);
          send(unpacker_, atom("run"));
        }
        else if (exit_reason_ != 0)
        {
          send_exit(this, exit_reason_);
        }
      }
    },
    on(atom("unpacked")) >> [=]
    {
      auto& s = segments_.front().get_as<segment>(0);

      VAST_LOG_ACTOR_DEBUG("finished unpacking segment " << s.id());
      partition_.update(s);

      segments_.pop();
      unpacker_ = invalid_actor;
      send(this, atom("unpack"));
    },
    on(atom("backlog")) >> [=]
    {
      uint64_t segments = segments_.size();
      return make_message(atom("backlog"), segments, max_backlog_);
    },
    [=](uint64_t processed, uint64_t indexed, uint64_t rate, uint64_t mean)
    {
      max_backlog_ = 0;
      for (auto& p : indexers_)
        if (p.second.actor == last_sender())
        {
          auto& stats = p.second.stats;

          if (stats.backlog > max_backlog_)
            max_backlog_ = stats.backlog;

          stats.backlog -= processed;
          stats.value_total += indexed;
          stats.value_rate = rate;
          stats.value_rate_mean = mean;

          break;
        }

      updated_ = true;
    },
    on(atom("stats"), atom("show")) >> [=]
    {
      delayed_send_tuple(this, std::chrono::seconds(3), last_dequeued());

      if (updated_)
        updated_ = false;
      else
        return;

      std::pair<uint64_t, actor> max_backlog = {0, invalid_actor};
      uint64_t value_total = 0;
      uint64_t value_rate = 0;
      uint64_t value_rate_mean = 0;
      uint64_t event_rate_min = -1;
      uint64_t event_rate_max = 0;

      auto n = 0;
      for (auto& p : indexers_)
        if (p.second.actor)
        {
          ++n;

          auto& stats = p.second.stats;
          if (stats.backlog > max_backlog.first)
            max_backlog = {stats.backlog, p.second.actor};

          if (stats.value_rate < event_rate_min)
            event_rate_min = stats.value_rate;
          if (stats.value_rate > event_rate_max)
            event_rate_max = stats.value_rate;

          value_total += stats.value_total;
          value_rate += stats.value_rate;
          value_rate_mean += stats.value_rate_mean;
        }

      if (value_rate > 0 || max_backlog.first > 0)
        VAST_LOG_ACTOR_VERBOSE(
            "indexes at " << value_rate << " values/sec" <<
            " (mean " << value_rate_mean << ") and " <<
            (value_rate / n) << " events/sec" <<
            " (" << event_rate_min << '/' << event_rate_max << '/' <<
            (value_rate_mean / n) << " min/max/mean) with max backlog of " <<
            max_backlog.first << " at " << max_backlog.second);
    }
  };
}

std::string partition_actor::describe() const
{
  return "partition";
}

} // namespace vast
