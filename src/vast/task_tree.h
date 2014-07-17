#ifndef VAST_TASK_TREE_H
#define VAST_TASK_TREE_H

#include <cassert>
#include <map>
#include <caf/all.hpp>
#include "vast/actor.h"

namespace vast {

/// Manages progress in a hierarchical task tree.
class task_tree : public actor_base
{
public:
  /// Spawns a task tree.
  /// @param root The root node of the task hierarchy.
  task_tree(caf::actor root)
  {
    degree_[root.address()] = 0;
  }

  caf::message_handler act() final
  {
    using namespace caf;

    attach_functor(
        [=](uint32_t)
        {
          graph_.clear();
          degree_.clear();
          subscriber_ = invalid_actor;
          notifyee_ = invalid_actor;
        });

    return
    {
      [=](actor parent, actor child)
      {
        VAST_LOG_ACTOR_DEBUG("registers child-parent edge (" << child <<
                             " -> " << parent << ")");
        ++total_;
        ++remaining_;
        ++degree_[parent.address()];
        graph_[child.address()] = parent;
      },
      on(atom("done")) >> [=]
      {
        assert(remaining_ > 0);
        --remaining_;

        auto edge = graph_.find(last_sender());
        if (edge == graph_.end())
        {
          VAST_LOG_ACTOR_ERROR("got unregistered node: " << last_sender());
          quit(exit::error);
          return;
        }

        VAST_LOG_ACTOR_DEBUG(
            "removes completed node " << last_sender() <<
            " (" << remaining_ << '/' << total_ << " remaining" << ')');

        if (subscriber_)
          send(subscriber_, remaining_, total_);

        if (remaining_ == 0)
          send(notifyee_, atom("done"));

        auto parent = edge->second.address();
        graph_.erase(edge);

        auto i = degree_.find(parent);
        assert(i != degree_.end());
        assert(i->second > 0);
        if (--i->second == 0)
        {
          degree_.erase(i);
          if (degree_.empty())
            quit(exit::done);
        }
      },
      on(atom("notify"), arg_match) >> [=](actor whom)
      {
        notifyee_ = whom;
      },
      on(atom("subscribe"), arg_match) >> [=](actor subscriber)
      {
        subscriber_ = subscriber;
      },
      on(atom("progress")) >> [=]
      {
        return make_message(remaining_, total_);
      }
    };
  }

  std::string describe() const final
  {
    return "task-tree";
  }

private:
  uint64_t remaining_ = 0;
  uint64_t total_ = 0;
  std::map<caf::actor_addr, caf::actor> graph_;
  std::map<caf::actor_addr, size_t> degree_;
  caf::actor subscriber_;
  caf::actor notifyee_;
};

} // namespace vast

#endif
