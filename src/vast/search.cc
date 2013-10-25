#include "vast/search.h"

#include "vast/exception.h"
#include "vast/optional.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"

namespace vast {

namespace {

// Deconstructs an entire query AST into its individual sub-trees and adds
// each to the state map.
class dissector : public expr::default_const_visitor
{
public:
  dissector(std::map<expr::ast, search::state>& state, expr::ast const& base)
    : state_{state},
      base_{base}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    expr::ast ast{conj};
    if (parent_)
      state_[ast].parents.insert(parent_);
    else if (ast == base_)
      state_.emplace(ast, search::state{});
    parent_ = std::move(ast);
    for (auto& clause : conj.operands)
      clause->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    expr::ast ast{disj};
    if (parent_)
      state_[ast].parents.insert(parent_);
    else if (ast == base_)
      state_.emplace(ast, search::state{});
    parent_ = std::move(ast);
    for (auto& term : disj.operands)
      term->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(parent_);
    expr::ast ast{rel};
    state_[ast].parents.insert(parent_);
  }

private:
  expr::ast parent_;
  std::map<expr::ast, search::state>& state_;
  expr::ast const& base_;
};


struct node_tester : public expr::default_const_visitor
{
  virtual void visit(expr::conjunction const&)
  {
    type = logical_and;
  }

  virtual void visit(expr::disjunction const&)
  {
    type = logical_or;
  }

  optional<boolean_operator> type;
};


// Computes the result of a conjunction by ANDing the results of all of its
// child nodes.
struct conjunction_evaluator : public expr::default_const_visitor
{
  conjunction_evaluator(std::map<expr::ast, search::state>& state)
    : state{state}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& operand : conj.operands)
      if (complete)
        operand->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    if (complete)
      update(disj);
  }

  virtual void visit(expr::relation const& rel)
  {
    if (complete)
      update(rel);
  }

  void update(expr::ast const& child)
  {
    auto i = state.find(child);
    assert(i != state.end());
    auto& child_result = i->second.result;
    if (! child_result)
    {
      complete = false;
      return;
    }
    result &= child_result;
  }

  bool complete = true;
  search_result result;
  std::map<expr::ast, search::state> const& state;
};

struct disjunction_evaluator : public expr::default_const_visitor
{
  disjunction_evaluator(std::map<expr::ast, search::state>& state)
    : state{state}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    update(conj);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& operand : disj.operands)
      operand->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    update(rel);
  }

  void update(expr::ast const& child)
  {
    auto i = state.find(child);
    assert(i != state.end());
    auto& child_result = i->second.result;
    if (child_result)
      result |= child_result;
  }

  search_result result;
  std::map<expr::ast, search::state> const& state;
};

} // namespace <anonymous>

expr::ast search::add_query(std::string const& str)
{
  expr::ast ast{str};
  if (! ast)
    return ast;
  dissector d{state_, ast};
  ast.accept(d);
  return ast;
}

std::vector<expr::ast> search::update(expr::ast const& ast,
                                      search_result const& result)
{
  assert(result);
  assert(state_.count(ast));
  auto& ast_state = state_[ast];
  node_tester nt;
  ast.accept(nt);
  // Phase 1: update the current AST node's state.
  if (nt.type)
  {
    if (*nt.type == logical_and)
    {
      VAST_LOG_VERBOSE("evaluating conjunction " << ast);
      conjunction_evaluator ce{state_};
      ast.accept(ce);
      if (ce.complete)
        ast_state.result = std::move(ce.result);
      VAST_LOG_DEBUG((ce.complete ? "" : "in") << "complete evaluation");
    }
    else if (*nt.type == logical_or)
    {
      VAST_LOG_VERBOSE("evaluating disjunction " << ast);
      disjunction_evaluator de{state_};
      ast.accept(de);
      if (! ast_state.result || (de.result && de.result != ast_state.result))
        ast_state.result = std::move(de.result);
    }
  }
  else if (! ast_state.result)
  {
    VAST_LOG_VERBOSE("assigning result to " << ast);
    ast_state.result = std::move(result);
  }
  else if (nt.type && *nt.type == logical_or)
  {
  }
  else if (ast_state.result == result)
  {
    assert(ast_state.result.hits());
    assert(ast_state.result == result);
    VAST_LOG_VERBOSE("ignoring unchanged result for " << ast);
  }
  else
  {
    VAST_LOG_VERBOSE("computing new result for " << ast);
    ast_state.result |= result;
  }
  // Phase 2: Update the parents recursively.
  std::vector<expr::ast> path;
  for (auto& parent : ast_state.parents)
  {
    auto pp = update(parent, ast_state.result);
    std::move(pp.begin(), pp.end(), std::back_inserter(path));
  }
  path.push_back(ast);
  std::sort(path.begin(), path.end());
  path.erase(std::unique(path.begin(), path.end()), path.end());
  return path;
}

search_result const* search::result(expr::ast const& ast) const
{
  auto i = state_.find(ast);
  return i != state_.end() ? &i->second.result : nullptr;
}

using namespace cppa;

search_actor::query_state::query_state(actor_ptr q, actor_ptr c)
  : query{q},
    client{c}
{
}

search_actor::search_actor(actor_ptr archive,
                           actor_ptr index,
                           actor_ptr schema_manager)
  : archive_{std::move(archive)},
    index_{std::move(index)},
    schema_manager_{std::move(schema_manager)}
{
}

void search_actor::act()
{
  trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        std::set<actor_ptr> doomed;
        for (auto& p : query_state_)
        {
          doomed.insert(p.second.query);
          doomed.insert(p.second.client);
        }
        for (auto& d : doomed)
          send_exit(d, exit::stop);
        quit(reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t)
      {
        VAST_LOG_ACTOR_DEBUG(
            "received DOWN from client @" << last_sender()->id());
        std::set<actor_ptr> doomed;
        for (auto& p : query_state_)
        {
          if (p.second.client == last_sender())
          {
            doomed.insert(p.second.query);
            doomed.insert(p.second.client);
          }
        }
        for (auto& d : doomed)
          send_exit(d, exit::stop);
      },
      on(atom("query"), atom("create"), arg_match) >> [=](std::string const& q)
      {
        auto ast = search_.add_query(q);
        if (! ast)
        {
          reply(actor_ptr{}, ast);
          return;
        }
        VAST_LOG_ACTOR_DEBUG("received new query: " << ast);
        monitor(last_sender());
        auto qry = spawn<query_actor>(archive_, last_sender(), ast);
        query_state_.emplace(ast, query_state{qry, last_sender()});
        // TODO: reuse results from existing queries before asking index again.
        send(index_, ast);
        reply(std::move(qry), std::move(ast));
      },
      on_arg_match >> [=](expr::ast const& ast, search_result const& result)
      {
        if (! result)
        {
          VAST_LOG_ACTOR_DEBUG("ignores empty result for: " << ast);
          return;
        }
        for (auto& node : search_.update(ast, result))
        {
          auto er = query_state_.equal_range(node);
          for (auto i = er.first; i != er.second; ++i)
          {
            auto r = search_.result(node);
            if (r && *r && *r != i->second.result)
            {
              VAST_LOG_ACTOR_DEBUG(
                  "propagates updated result to query @" <<
                  i->second.query->id() << " from " << i->first);
              i->second.result = *r;
              send(i->second.query, r->hits());
            }
          }
        }
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* search_actor::description() const
{
  return "search";
}

} // namespace vast
