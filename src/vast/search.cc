#include "vast/search.h"

#include "vast/exception.h"
#include "vast/optional.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"

namespace vast {

namespace {

// Extracts all (leaf) predicates from an AST.
class predicator : public expr::default_const_visitor
{
public:
  predicator(std::vector<expr::ast>& predicates)
    : predicates_{predicates}
  {
  }

  virtual void visit(expr::conjunction const& conj)
  {
    for (auto& op : conj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    for (auto& op : disj.operands)
      op->accept(*this);
  }

  virtual void visit(expr::predicate const& pred)
  {
    predicates_.emplace_back(pred);
  }

private:
  std::vector<expr::ast>& predicates_;
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

  virtual void visit(expr::predicate const& pred)
  {
    if (complete)
      update(pred);
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

// Computes the result of a disjunction by ORing the results of all of its
// child nodes.
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

  virtual void visit(expr::predicate const& pred)
  {
    update(pred);
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

// Deconstructs an entire query AST into its individual sub-trees and adds
// each sub-tree to the state map.
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

    assert(state_.find(ast) != state_.end());

    parent_ = ast;
    for (auto& clause : conj.operands)
      clause->accept(*this);

    conjunction_evaluator ce{state_};
    ast.accept(ce);
    if (ce.complete)
      state_[ast].result = std::move(ce.result);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    expr::ast ast{disj};

    if (parent_)
      state_[ast].parents.insert(parent_);
    else if (ast == base_)
      state_.emplace(ast, search::state{});

    assert(state_.find(ast) != state_.end());

    parent_ = ast;
    for (auto& term : disj.operands)
      term->accept(*this);

    disjunction_evaluator de{state_};
    ast.accept(de);
    state_[ast].result = std::move(de.result);
  }

  virtual void visit(expr::predicate const& pred)
  {
    assert(parent_);
    expr::ast ast{pred};
    state_[ast].parents.insert(parent_);
  }

private:
  expr::ast parent_;
  std::map<expr::ast, search::state>& state_;
  expr::ast const& base_;
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
  assert(state_.count(ast));
  if (! result)
  {
    VAST_LOG_DEBUG("aborting ast propagation due to empty result");
    return {};
  }

  // Phase 1: Update result of AST node (and potentially its direct children).
  auto& ast_state = state_[ast];
  if (ast.is_conjunction())
  {
    VAST_LOG_DEBUG("evaluating conjunction " << ast);
    conjunction_evaluator ce{state_};
    ast.accept(ce);
    if (ce.complete)
      ast_state.result = std::move(ce.result);
  }
  else if (ast.is_disjunction())
  {
    VAST_LOG_DEBUG("evaluating disjunction " << ast);
    disjunction_evaluator de{state_};
    ast.accept(de);
    if (! ast_state.result || (de.result && de.result != ast_state.result))
      ast_state.result = std::move(de.result);
  }
  else if (! ast_state.result)
  {
    VAST_LOG_DEBUG("assigning result to " << ast);
    ast_state.result = std::move(result);
  }
  else if (ast_state.result == result)
  {
    VAST_LOG_DEBUG("ignoring unchanged result for " << ast);
  }
  else
  {
    VAST_LOG_DEBUG("computing new result for " << ast);
    ast_state.result |= result;
  }

  // Phase 2: Propagate the result to the parents recursively.
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
            "received DOWN from client " << VAST_ACTOR_ID(last_sender()));
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
        auto i = query_state_.emplace(ast, query_state{qry, last_sender()});

        // Deconstruct the AST into its predicates and ask the index for those
        // we have no results for.
        std::vector<expr::ast> predicates;
        predicator visitor{predicates};
        ast.accept(visitor);
        for (auto& pred : predicates)
        {
          auto r = search_.result(pred);
          if (! r || ! *r)
          {
            VAST_LOG_ACTOR_DEBUG("hits index with " << pred);
            send(index_, pred);
          }
          else
          {
            VAST_LOG_ACTOR_DEBUG("reuses existing result for " << pred);
          }
        }

        auto r = search_.result(ast);
        if (r && *r)
        {
          VAST_LOG_ACTOR_DEBUG("could answer query from existing predicates");
          i->second.result = *r;
          send(i->second.query, r->hits());
        }

        reply(qry, ast);
      },
      on_arg_match >> [=](expr::ast const& ast, search_result const& result)
      {
        if (! result)
        {
          VAST_LOG_ACTOR_DEBUG(
              "ignores empty result from " << VAST_ACTOR_ID(last_sender()) <<
              " for: " << ast);
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
                  "propagates updated result to query " <<
                  VAST_ACTOR_ID(i->second.query) << " from " << i->first);
              i->second.result = *r;
              send(i->second.query, r->hits());
            }
          }
        }
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from " <<
                             VAST_ACTOR_ID(last_sender()) << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* search_actor::description() const
{
  return "search";
}

} // namespace vast
