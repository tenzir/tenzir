#include "vast/partition.h"

#include <cppa/cppa.hpp>
#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/segment.h"
#include "vast/io/serialization.h"

namespace vast {

path const partition::part_meta_file = "partition.meta";
path const partition::event_data_dir = "data";

namespace {

// If the given predicate is a name extractor, extracts the name.
struct name_checker : expr::default_const_visitor
{
  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
    if (check_)
      pred.rhs().accept(*this);
  }

  virtual void visit(expr::name_extractor const&)
  {
    check_ = true;
  }

  virtual void visit(expr::constant const& c)
  {
    value_ = c.val;
  }

  bool check_ = false;
  value value_;
};

// Partitions the AST predicates into meta and data predicates. Moreover, it
// assigns each data predicate a name if a it coexists with name-extractor
// predicate that restricts the scope of the data predicate.
struct predicatizer : expr::default_const_visitor
{
  virtual void visit(expr::conjunction const& conj)
  {
    std::vector<expr::ast> flat;
    std::vector<expr::ast> deep;

    // Partition terms into leaves and others.
    for (auto& operand : conj.operands)
    {
      auto op = expr::ast{*operand};
      if (op.is_predicate())
        flat.push_back(std::move(op));
      else
        deep.push_back(std::move(op));
    }

    // Extract the name for all predicates in this conjunction.
    //
    // TODO: add a check to the semantic pass after constructing queries to
    // flag multiple name/offset/time extractors in the same conjunction.
    name_.clear();
    for (auto& pred : flat)
    {
      name_checker checker;
      pred.accept(checker);
      if (checker.check_)
        name_ = checker.value_.get<string>();
    }

    for (auto& pred : flat)
      pred.accept(*this);

    // Proceed with the remaining conjunctions deeper in the tree.
    name_.clear();
    for (auto& n : deep)
      n.accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& operand : disj.operands)
      operand->accept(*this);
  }

  virtual void visit(expr::predicate const& pred)
  {
    if (! expr::ast{pred}.is_meta_predicate())
      data_predicates_.emplace(name_, pred);
    else
      meta_predicates_.push_back(pred);
  }

  string name_;
  std::multimap<string, expr::ast> data_predicates_;
  std::vector<expr::ast> meta_predicates_;
};

} // namespace <anonymous>

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

  assert(s.coverage());
  coverage |= *s.coverage();
}

void partition::meta_data::serialize(serializer& sink) const
{
  sink << id << first_event << last_event << last_modified << coverage;
}

void partition::meta_data::deserialize(deserializer& source)
{
  source >> id >> first_event >> last_event >> last_modified >> coverage;
}

bool operator==(partition::meta_data const& x, partition::meta_data const& y)
{
  return x.id == y.id
      && x.first_event == y.first_event
      && x.last_event == y.last_event
      && x.last_modified == y.last_modified
      && x.coverage == y.coverage;
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


using namespace cppa;

namespace {

struct dispatcher : expr::default_const_visitor
{
  dispatcher(partition_actor& pa)
    : actor_{pa}
  {
  }

  virtual void visit(expr::predicate const& pred)
  {
    pred.lhs().accept(*this);
  }

  virtual void visit(expr::timestamp_extractor const&)
  {
    indexes_.push_back(actor_.time_indexer_);
  }

  virtual void visit(expr::name_extractor const&)
  {
    indexes_.push_back(actor_.name_indexer_);
  }

  virtual void visit(expr::type_extractor const& te)
  {
    for (auto& p0 : actor_.indexers_)
      for (auto& p1 : p0.second)
        if (p1.second.type == te.type)
        {
          if (p1.second.actor)
          {
            indexes_.push_back(p1.second.actor);
          }
          else
          {
            auto i = actor_.load_indexer(p0.first, p1.first);
            if (i.failed())
              VAST_LOG_ERROR(i.failure().msg());
            else if (i.empty())
              assert(! "file system inconsistency: index must exist");
            else
              indexes_.push_back(*i);
          }
        }
  }

  virtual void visit(expr::offset_extractor const& oe)
  {
    // TODO: restrict to the right name rather than going through all indexers.
    for (auto& p0 : actor_.indexers_)
    {
      auto i = p0.second.find(oe.off);
      if (i == p0.second.end())
      {
        VAST_LOG_WARN("no index for offset " << oe.off);
      }
      else if (! i->second.actor)
      {
        auto a = actor_.load_indexer(p0.first, oe.off);
        if (a.failed())
          VAST_LOG_ERROR(a.failure().msg());
        else if (a.empty())
          assert(! "file system inconsistency: index must exist");
        else
          indexes_.push_back(*a);
      }
      else
      {
        indexes_.push_back(i->second.actor);
      }
    }
  }

  partition_actor& actor_;
  std::vector<actor_ptr> indexes_;
};

} // namespace <anonymous>

partition_actor::partition_actor(path dir, size_t batch_size, uuid id)
  : dir_{std::move(dir)},
    batch_size_{batch_size},
    partition_{std::move(id)}
{
}

void partition_actor::act()
{
  chaining(false);
  trap_exit(true);

  if (exists(dir_))
  {
    if (! io::unarchive(dir_ / partition::part_meta_file, partition_))
    {
      VAST_LOG_ACTOR_ERROR("failed to load partition meta data from " << dir_);
      quit(exit::error);
      return;
    }

    std::map<string, std::map<offset, value_type>> types;
    if (! io::unarchive(dir_ / "types", types))
    {
      VAST_LOG_ACTOR_ERROR("failed to load type data from " << dir_ / "types");
      quit(exit::error);
      return;
    }

    for (auto& p0 : types)
      for (auto& p1 : p0.second)
        indexers_[p0.first][p1.first].type = p1.second;
  }

  traverse(
      dir_ / partition::event_data_dir,
      [&](path const& event_dir) -> bool
      {
        if (! event_dir.is_directory())
        {
          VAST_LOG_ACTOR_WARN("found unrecognized file in " << event_dir);
          return true;
        }

        auto event_name = event_dir.basename().str();
        traverse(
            event_dir,
            [&](path const& idx_file) -> bool
            {
              if (idx_file.extension() != ".idx")
                return true;

              VAST_LOG_ACTOR_DEBUG("found index " << idx_file);
              auto str = idx_file.basename(true).str();
              auto start = str.begin();
              offset o;
              if (! extract(start, str.end(), o))
              {
                VAST_LOG_ACTOR_ERROR("got invalid offset in " << idx_file);
                quit(exit::error);
                return false;
              }

              if (! indexers_[event_name].count(o))
              {
                VAST_LOG_ACTOR_ERROR("has no meta data for " << idx_file);
                quit(exit::error);
                return false;
              }

              return true;
            });

        return true;
      });

  auto submit_meta = [=](std::vector<cow<event>> events)
  {
    if (! name_indexer_)
    {
      auto p = dir_ / "name.idx";
      name_indexer_ =
        spawn<event_name_indexer<default_bitstream>>(std::move(p));
    }

    if (! time_indexer_)
    {
      auto p = dir_ / "time.idx";
      time_indexer_ =
        spawn<event_time_indexer<default_bitstream>>(std::move(p));
    }

    auto t = make_any_tuple(std::move(events));
    name_indexer_ << t;
    time_indexer_ << t;
  };

  auto submit_data = [=](event const& e, std::vector<cow<event>> events)
  {
    auto t = make_any_tuple(std::move(events));

    e.each_offset(
        [&](value const& v, offset const& o)
        {
          // We can't handle container types (yet).
          if (is_container_type(v.which()))
            return;

          actor_ptr indexer;

          auto i = load_indexer(e.name(), o);
          if (i.failed())
          {
            VAST_LOG_ACTOR_ERROR(i.failure().msg());
            quit(exit::error);
            return;
          }
          else if (i.empty())
          {
            auto a = create_indexer(e.name(), o, v.which());
            if (! a)
            {
              VAST_LOG_ACTOR_ERROR(a.failure().msg());
              quit(exit::error);
              return;
            }

            indexer = *a;
          }
          else
          {
            indexer = *i;
          }

          indexer << t;
        });
  };

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (reason != exit::kill)
          if (partition_.meta().coverage)
            if (! io::archive(dir_ / partition::part_meta_file, partition_))
            {
              VAST_LOG_ACTOR_ERROR("failed to save partition " << dir_);
              quit(exit::error);
              return;
            }

        send_exit(time_indexer_, reason);
        send_exit(name_indexer_, reason);
        for (auto& p0 : indexers_)
          for (auto& p1 : p0.second)
            if (p1.second.actor)
              send_exit(p1.second.actor, reason);

        quit(reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from " << VAST_ACTOR_ID(last_sender()));

        for (auto& p0 : indexers_)
          for (auto& p1 : p0.second)
            if (p1.second.actor == last_sender())
              p1.second.actor = {};

        stats_.erase(last_sender());
      },
      on_arg_match >> [=](expr::ast const& pred, actor_ptr const& sink)
      {
        VAST_LOG_ACTOR_DEBUG("got predicate " << pred <<
                             " for " << VAST_ACTOR_ID(sink));

        dispatcher d{*this};
        pred.accept(d);

        uint64_t n = d.indexes_.size();
        send(last_sender(), pred, partition_.meta().id, n);

        auto t = make_any_tuple(pred, partition_.meta().id, sink);
        for (auto& a : d.indexes_)
          a << t;
      },
      on(atom("flush")) >> [=]
      {
        std::map<string, std::map<offset, value_type>> types;

        send(name_indexer_, atom("flush"));
        send(time_indexer_, atom("flush"));
        for (auto& p0 : indexers_)
          for (auto& p1 : p0.second)
          {
            types[p0.first][p1.first] = p1.second.type;

            if (p1.second.actor)
              send(p1.second.actor, atom("flush"));
          }

        if (! io::archive(dir_ / "types", types))
        {
          VAST_LOG_ACTOR_ERROR("failed to save type data for " << dir_);
          quit(exit::error);
          return;
        }

        if (! io::archive(dir_ / partition::part_meta_file, partition_))
        {
          VAST_LOG_ACTOR_ERROR("failed to save partition " << dir_);
          quit(exit::error);
          return;
        }
      },
      on_arg_match >> [=](segment const& s)
      {
        VAST_LOG_ACTOR_DEBUG(
            "processes " << s.events() << " events from segment " << s.id());

        std::vector<cow<event>> all_events;
        all_events.reserve(batch_size_);
        std::unordered_map<string, std::vector<cow<event>>> groups;

        segment::reader r{&s};
        while (auto ev = r.read())
        {
          assert(ev);
          cow<event> e{std::move(*ev)};

          all_events.push_back(e);
          if (all_events.size() == batch_size_)
          {
            submit_meta(std::move(all_events));
            all_events = {};
          }

          auto& group = groups[e->name()];
          group.push_back(e);
          if (group.size() == batch_size_)
          {
            submit_data(*e, std::move(group));
            group = {};
          }
        }

        // Send away final events.
        submit_meta(std::move(all_events));
        for (auto& p : groups)
          if (! p.second.empty())
          {
            auto first = p.second[0];
            submit_data(*first, std::move(p.second));
          }

        // Record segment.
        partition_.update(s);

        // Flush partition meta data and data indexes.
        send(self, atom("flush"));
        send(self, atom("stats"), atom("show"));
      },
      on(atom("stats"), arg_match) >> [=](uint64_t n, uint64_t rate, uint64_t mean)
      {
        auto& s = stats_[last_sender()];
        s.values += n;
        s.rate = rate;
        s.mean = mean;
      },
      on(atom("stats"), atom("show")) >> [=]
      {
        uint64_t total_values = 0;
        uint64_t total_rate = 0;
        uint64_t total_mean = 0;
        for (auto& p : stats_)
        {
          total_values += p.second.values;
          total_rate += p.second.rate;
          total_mean += p.second.mean;
        }

        VAST_LOG_ACTOR_INFO(
            "indexed " << total_values << " values at rate " <<
            total_rate << " values/sec" << " (mean " << total_mean << ')');
      });
}

char const* partition_actor::description() const
{
  return "partition";
}

} // namespace vast
