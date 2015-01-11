#include "vast/actor/partition.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/actor/indexer.h"
#include "vast/actor/task_tree.h"
#include "vast/actor/source/dechunkifier.h"
#include "vast/io/serialization.h"

using namespace caf;

namespace vast {

struct partition::dispatcher
{
  dispatcher(partition& pa)
    : part_{pa}
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
    return {part_.load_name_indexer()};
  }

  std::vector<actor> operator()(time_extractor const&, data const&)
  {
    return {part_.load_time_indexer()};
  }

  std::vector<actor> operator()(type_extractor const& e, data const&)
  {
    std::vector<actor> indexes;
    for (auto& t : part_.schema_)
      if (auto r = get<type::record>(t))
      {
        auto attempt = r->each(
            [&](type::record::trace const& tr, offset const& o) -> trial<void>
            {
              if (tr.back()->type == e.type)
              {
                auto a = part_.load_data_indexer(t, e.type, o);
                if (! a)
                  return a.error();
                if (*a)
                  indexes.push_back(std::move(*a));
              }

              return nothing;
            });

        if (! attempt)
        {
          VAST_ERROR(attempt.error());
          return {};
        }
      }
      else
      {
        auto a = part_.load_data_indexer(t, t, {});
        if (! a)
        {
          VAST_ERROR(a.error());
          return {};
        }
        if (*a)
          indexes.push_back(std::move(*a));
      }

    return indexes;
  }

  std::vector<actor> operator()(schema_extractor const& e, data const& d)
  {
    std::vector<actor> indexes;
    for (auto& t : part_.schema_)
      if (auto r = get<type::record>(t))
      {
        for (auto& pair : r->find_suffix(e.key))
        {
          auto& o = pair.first;
          auto lhs_type = r->at(o);
          assert(lhs_type);

          if (! compatible(*lhs_type, op_, type::derive(d)))
          {
            VAST_WARN("incompatible types: LHS =", *lhs_type,
                      "<--> RHS =", type::derive(d));
            return {};
          }

          auto a = part_.load_data_indexer(t, *lhs_type, o);
          if (! a)
            VAST_ERROR(a.error());
          else if (*a)
            indexes.push_back(std::move(*a));
        }
      }
      else if (e.key.size() == 1 && pattern::glob(e.key[0]).match(t.name()))
      {
        auto a = part_.load_data_indexer(t, t, {});
        if (! a)
        {
          VAST_ERROR(a.error());
          return {};
        }

        if (*a)
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
  partition& part_;
};


partition::partition(path const& index_dir, uuid id, size_t batch_size)
  : dir_{index_dir / to_string(id)},
    id_{std::move(id)},
    batch_size_{batch_size}
{
}

void partition::at_down(down_msg const&)
{
  if (last_sender() == dechunkifier_)
  {
    dechunkifier_ = invalid_actor;
    chunks_.pop();

    if (chunks_.size() == 10 - 1)
    {
      VAST_DEBUG(this, "signals underload");
      for (auto& a : upstream())
        send(message_priority::high, a, flow_control::underload{});
    }

    send(this, atom("unpack"));
  }
  else
  {
    VAST_DEBUG(this, "got DOWN from", last_sender());

    stats_.erase(last_sender());
    for (auto i = indexers_.begin(); i != indexers_.end(); ++i)
      if (i->second == last_sender())
      {
        indexers_.erase(i);
        break;
      }
  }
}

void partition::at_exit(exit_msg const& msg)
{
  if (msg.reason == exit::kill)
  {
    quit(msg.reason);
  }
  else if (exit_reason_ == 0 && ! chunks_.empty())
  {
    exit_reason_ = msg.reason; // Wait for unpacker to finish.
  }
  else
  {
    auto tree = spawn<task_tree>(this);
    send(tree, atom("notify"), this);
    send(tree, this, this);
    send(this, atom("flush"), tree);
    exit_reason_ = msg.reason;
  }
}

message_handler partition::make_handler()
{
  trap_exit(true);

  if (exists(dir_))
  {
    auto t = io::unarchive(dir_ / "schema", schema_);
    if (! t)
    {
      VAST_ERROR(this, "failed to load schema:", t.error());
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
    on(atom("done")) >> [=]
    {
      // We only spawn one task tree upon exiting. Once we get notified we can
      // safely terminate with the last exit reason.
      quit(exit_reason_);
    },
    [=](expression const& pred, actor idx)
    {
      VAST_DEBUG(this, "got predicate", pred);

      auto indexers = visit(dispatcher{*this}, pred);
      uint64_t n = indexers.size();
      send(idx, pred, id_, n);

      if (n == 0)
      {
        VAST_DEBUG(this, "did not find a matching indexer for", pred);
        send(idx, pred, id_, bitstream{});
      }
      else
      {
        auto t = make_message(pred, id_, idx);
        for (auto& a : indexers)
          send_tuple(a, t);
      }
    },
    on(atom("flush"), arg_match) >> [=](actor tree)
    {
      VAST_DEBUG(this, "got request to flush indexes");

      for (auto& p : indexers_)
        if (p.second)
        {
          send(tree, this, p.second);
          send(p.second, atom("flush"), tree);
        }

      if (! schema_.empty())
      {
        auto t = io::archive(dir_ / "schema", schema_);
        if (! t)
        {
          VAST_ERROR(this, "failed to save schema:", t.error());
          quit(exit::error);
          return;
        }
      }

      send(tree, atom("done"));
    },
    [=](chunk const& c)
    {
      VAST_DEBUG(this, "got chunk with", c.events(), "events");

      auto sch = schema::merge(schema_, c.meta().schema);
      if (! sch)
      {
        VAST_ERROR(this, "failed to merge schema:", sch.error());
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
                  if (t.back()->type.find_attribute(type::attribute::skip))
                    return nothing;

                  auto a = create_data_indexer(tp, t.back()->type, o);
                  if (! a)
                    return a.error();
                  return nothing;
                });

            if (! attempt)
            {
              VAST_ERROR(this, attempt.error());
              quit(exit::error);
              return;
            }
          }
          else if (! tp.find_attribute(type::attribute::skip))
          {
            auto t = create_data_indexer(tp, tp, {});
            if (! t)
            {
              VAST_ERROR(this, t.error());
              quit(exit::error);
              return;
            }
          }
      }

      chunks_.push(c);
      send(this, atom("unpack"));

      if (chunks_.size() == 10)
      {
        VAST_DEBUG(this, "signals overload");
        for (auto& a : upstream())
          send(message_priority::high, a, flow_control::overload{});
      }
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
          VAST_DEBUG(this, "begins unpacking a chunk (" <<
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
    [=](uint64_t processed, uint64_t indexed, uint64_t rate, uint64_t mean)
    {
      auto i = stats_.find(last_sender());
      assert(i != stats_.end());
      auto& s = i->second;

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
        VAST_VERBOSE(this, 
            "indexes at " << value_rate << " values/sec" <<
            " (mean " << value_rate_mean << ") and " <<
            (value_rate / n) << " events/sec" <<
            " (" << event_rate_min << '/' << event_rate_max << '/' <<
            (value_rate_mean / n) << " min/max/mean) with max backlog of " <<
            max_backlog.first << " at " << max_backlog.second);
    }
  };
}

std::string partition::name() const
{
  return "partition";
}

actor partition::load_time_indexer()
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

actor partition::load_name_indexer()
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

trial<actor> partition::load_data_indexer(
    type const& et, type const& t, offset const& o)
{
  auto i = indexers_.find(dir_ / path{"types"} / et.name());
  if (i != indexers_.end())
  {
    assert(i->second);
    return i->second;
  }

  return create_data_indexer(et, t, o);
}

trial<actor> partition::create_data_indexer(
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
