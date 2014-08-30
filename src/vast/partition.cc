#include "vast/partition.h"

#include <caf/all.hpp>
#include "vast/bitmap_index.h"
#include "vast/event.h"
#include "vast/task_tree.h"
#include "vast/io/serialization.h"
#include "vast/source/dechunkifier.h"

using namespace caf;

namespace vast {

path const partition::part_meta_file = "partition.meta";

partition::meta_data::meta_data(uuid id)
  : id{id}
{
}

void partition::meta_data::update(chunk const& chk)
{
  if (first_event == time_range{} || chk.meta().first < first_event)
    first_event = chk.meta().first;

  if (last_event == time_range{} || chk.meta().last > last_event)
    last_event = chk.meta().last;

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

void partition::update(chunk const& chk)
{
  meta_.update(chk);
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


struct partition_actor::dispatcher
{
  dispatcher(partition_actor& pa)
    : actor_{pa}
  {
  }

  template <typename T>
  std::vector<actor> operator()(T const&)
  {
    return {};
  }

  template <typename T, typename U>
  std::vector<actor> operator()(T const&, U const&)
  {
    return {};
  }

  std::vector<actor> operator()(predicate const& p)
  {
    op_ = p.op;
    return visit(*this, p.lhs, p.rhs);
    
  }

  std::vector<actor> operator()(event_extractor const&, data const&)
  {
    return {actor_.load_name_indexer()};
  }

  std::vector<actor> operator()(time_extractor const&, data const&)
  {
    return {actor_.load_time_indexer()};
  }

  std::vector<actor> operator()(type_extractor const& e, data const&)
  {
    std::vector<actor> indexes;
    for (auto& t : actor_.schema_)
      if (auto r = get<type::record>(t))
      {
        auto attempt = r->each(
            [&](type::record::trace const& tr, offset const& o) -> trial<void>
            {
              if (tr.back()->type == e.type)
              {
                auto a = actor_.load_data_indexer(t, e.type, o);
                if (! a)
                  return a.error();

                indexes.push_back(std::move(*a));
              }

              return nothing;
            });

        if (! attempt)
        {
          VAST_LOG_ERROR(attempt.error());
          return {};
        }
      }
      else if (t == e.type)
      {
        auto a = actor_.load_data_indexer(t, t, {});
        if (! a)
        {
          VAST_LOG_ERROR(a.error());
          return {};
        }

        indexes.push_back(std::move(*a));
      }

    return indexes;
  }

  std::vector<actor> operator()(schema_extractor const& e, data const& d)
  {
    std::vector<actor> indexes;
    for (auto& t : actor_.schema_)
      if (auto r = get<type::record>(t))
      {
        for (auto& pair : r->find_suffix(e.key))
        {
          auto& o = pair.first;
          auto lhs_type = r->at(o);
          assert(lhs_type);

          if (! compatible(*lhs_type, op_, type::derive(d)))
          {
            VAST_LOG_WARN("incompatible types: LHS = " << *lhs_type <<
                          " <--> RHS = " << type::derive(d));
            return {};
          }

          auto a = actor_.load_data_indexer(t, *lhs_type, o);
          if (! a)
            VAST_LOG_ERROR(a.error());
          else
            indexes.push_back(std::move(*a));
        }
      }
      else if (e.key.size() == 1 && pattern::glob(e.key[0]).match(t.name()))
      {
        auto a = actor_.load_data_indexer(t, t, {});
        if (! a)
        {
          VAST_LOG_ERROR(a.error());
          return {};
        }

        indexes.push_back(std::move(*a));;
      }

    return indexes;
  }

  template <typename T>
  std::vector<actor> operator()(data const& d, T const& e)
  {
    return (*this)(e, d);
  }

  relational_operator op_;
  partition_actor& actor_;
};


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
  }

  attach_functor(
      [=](uint32_t reason)
      {
        if (dechunkifier_)
          anon_send_exit(dechunkifier_, reason);
        dechunkifier_ = invalid_actor;

        for (auto& p : indexers_)
          anon_send_exit(p.second, reason);
        indexers_.clear();
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
      if (exit_reason_ == 0 && ! chunks_.empty())
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
      if (last_sender() == dechunkifier_)
      {
        auto chk = chunks_.front();
        partition_.update(chk);
        chunks_.pop();
        dechunkifier_ = invalid_actor;
        send(this, atom("unpack"));
      }
      else
      {
        VAST_LOG_ACTOR_DEBUG("got DOWN from " << last_sender());

        stats_.erase(last_sender());
        for (auto i = indexers_.begin(); i != indexers_.end(); ++i)
          if (i->second == last_sender())
          {
            indexers_.erase(i);
            break;
          }
      }
    },
    [=](expression const& pred, actor idx)
    {
      VAST_LOG_ACTOR_DEBUG("got predicate " << pred);

      auto indexes = visit(dispatcher{*this}, pred);

      uint64_t n = indexes.size();
      send(idx, pred, partition_.meta().id, n);

      if (n == 0)
      {
        VAST_LOG_ACTOR_DEBUG("did not find a matching indexer for " << pred);
        send(idx, pred, partition_.meta().id, bitstream{});
      }
      else
      {
        auto t = make_message(pred, partition_.meta().id, idx);
        for (auto& a : indexes)
          send_tuple(a, t);
      }
    },
    on(atom("flush"), arg_match) >> [=](actor tree)
    {
      VAST_LOG_ACTOR_DEBUG("got request to flush its indexes");

      for (auto& p : indexers_)
        if (p.second)
        {
          send(tree, this, p.second);
          send(p.second, atom("flush"), tree);
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
    [=](chunk const& c)
    {
      VAST_LOG_ACTOR_DEBUG("got chunk with " << c.events() << " events");

      auto sch = schema::merge(schema_, c.meta().schema);
      if (! sch)
      {
        VAST_LOG_ACTOR_ERROR("failed to merge schema: " << sch.error());
        quit(exit::error);
        return;
      }
      else if (indexers_.empty() || *sch != schema_)
      {
        schema_ = std::move(*sch);

        load_time_indexer();
        load_name_indexer();

        for (auto& tp : c.meta().schema)
          // FIXME: Adjust after having switched to the new record indexer.
          if (auto r = get<type::record>(tp))
          {
            auto attempt = r->each(
                [&](type::record::trace const& t, offset const& o) -> trial<void>
                {
                  auto a = load_data_indexer(tp, t.back()->type, o);
                  if (! a)
                    return a.error();
                  return nothing;
                });

            if (! attempt)
            {
              VAST_LOG_ACTOR_ERROR(attempt.error());
              quit(exit::error);
              return;
            }
          }
      }

      chunks_.push(c);
      send(this, atom("unpack"));
    },
    [=](std::vector<event> const& events)
    {
      for (auto& p : indexers_)
        send_tuple(p.second, last_dequeued());

      for (auto& p : stats_)
        p.second.backlog += events.size();
    },
    on(atom("unpack")) >> [=]
    {
      if (! dechunkifier_)
      {
        if (! chunks_.empty())
        {
          VAST_LOG_ACTOR_DEBUG("begins unpacking a chunk (" <<
                               chunks_.size() << " remaining)");

          dechunkifier_ =
            spawn<source::dechunkifier, monitored>(chunks_.front());

          send(dechunkifier_, atom("sink"), this);
          send(dechunkifier_, atom("batch size"), batch_size_);
          send(dechunkifier_, atom("run"));
        }
        else if (exit_reason_ != 0)
        {
          send_exit(this, exit_reason_);
        }
      }
    },
    on(atom("backlog")) >> [=]
    {
      uint64_t n = chunks_.size();
      return make_message(atom("backlog"), n, max_backlog_);
    },
    [=](uint64_t processed, uint64_t indexed, uint64_t rate, uint64_t mean)
    {
      max_backlog_ = 0;
      auto i = stats_.find(last_sender());
      assert(i != stats_.end());
      auto& s = i->second;

      if (s.backlog > max_backlog_)
        max_backlog_ = s.backlog;

      s.backlog -= processed;
      s.value_total += indexed;
      s.value_rate = rate;
      s.value_rate_mean = mean;

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
      {
        ++n;

        auto& s = stats_[p.second.address()];
        if (s.backlog > max_backlog.first)
          max_backlog = {s.backlog, p.second};

        if (s.value_rate < event_rate_min)
          event_rate_min = s.value_rate;
        if (s.value_rate > event_rate_max)
          event_rate_max = s.value_rate;

        value_total += s.value_total;
        value_rate += s.value_rate;
        value_rate_mean += s.value_rate_mean;
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

actor partition_actor::load_time_indexer()
{
  auto p = dir_ / "meta" / "time" / "index";
  auto& s = indexers_[p];
  if (! s)
  {
    s = spawn<event_time_indexer<default_bitstream>>(std::move(p));
    monitor(s);
    stats_[s.address()];
  }

  return s;
}

actor partition_actor::load_name_indexer()
{
  auto p = dir_ / "meta" / "name" / "index";
  auto& s = indexers_[p];
  if (! s)
  {
    s = spawn<event_name_indexer<default_bitstream>>(std::move(p));
    monitor(s);
    stats_[s.address()];
  }

  return s;
}

trial<actor> partition_actor::load_data_indexer(
    type const& et, type const& t, offset const& o)
{
  auto abs = dir_ / path{"types"} / et.name();

  // FIXME: Remove after having switched to the new record indexer.
  auto r = get<type::record>(et);
  if (r)
  {
    auto fs = r->resolve(o);
    assert(fs);
    for (auto& f : *fs)
      abs /= f;
  }

  abs /= "index";

  auto& s = indexers_[abs];
  if (! s)
  {
    auto a = make_event_data_indexer<default_bitstream>(abs, et, t, o);
    if (! a)
      return a;

    s = *a;
    monitor(s);
    stats_[s.address()];
  }

  return s;
}

} // namespace vast
