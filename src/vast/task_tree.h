#ifndef VAST_TASK_TREE_H
#define VAST_TASK_TREE_H

#include <cassert>
#include <map>
#include <cppa/cppa.hpp>
#include "vast/actor.h"

namespace vast {

/// Manages progress in a hierarchical task tree.
class task_tree : public actor_base
{
public:
  /// Spawns a task tree.
  /// @param root The root node of the task hierarchy.
  /// @param exit_code The exit code to exit with when done.
  task_tree(cppa::actor root, uint32_t exit_code = exit::done)
    : exit_code_{exit_code}
  {
    degree_[root.address()] = 0;
  }

  cppa::partial_function act() final
  {
    using namespace cppa;

    attach_functor(
        [=](uint32_t)
        {
          graph_.clear();
          degree_.clear();
          tracker_ = invalid_actor;
        });

    return
    {
      [=](actor parent, actor child)
      {
        ++total_;
        ++remaining_;
        ++degree_[parent.address()];
        graph_[child.address()] = parent;
      },
      on(atom("done")) >> [=]
      {
        assert(remaining_ > 0);
        --remaining_;

        if (tracker_)
          send(tracker_, remaining_, total_);

        remove(last_sender());
      },
      on(atom("update"), arg_match) >> [=](actor tracker)
      {
        tracker_ = tracker;
      },
      on(atom("progress")) >> [=]
      {
        return make_any_tuple(remaining_, total_);
      }
    };
  }

  std::string describe() const final
  {
    return "task-tree";
  }

private:
  void remove(cppa::actor_addr node)
  {
    auto edge = graph_.find(node);
    assert(edge != graph_.end());

    auto parent = edge->second.address();
    graph_.erase(edge);

    auto i = degree_.find(parent);
    assert(i != degree_.end());
    assert(i->second > 0);
    if (--i->second == 0)
    {
      degree_.erase(i);
      if (degree_.empty())
        quit(exit_code_);
    }
  };

  uint32_t exit_code_ = exit::done;
  uint64_t remaining_ = 0;
  uint64_t total_ = 0;
  std::map<cppa::actor_addr, cppa::actor> graph_;
  std::map<cppa::actor_addr, size_t> degree_;
  cppa::actor tracker_;
};

} // namespace vast

#endif
