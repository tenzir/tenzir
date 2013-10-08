#include "vast/search.h"

#include "vast/exception.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"

using namespace cppa;

namespace vast {

search::search(actor_ptr archive, actor_ptr index, actor_ptr schema_manager)
  : archive_{std::move(archive)},
    index_{std::move(index)},
    schema_manager_{std::move(schema_manager)}
{
}

namespace {

class dissector : public expr::const_visitor
{
public:
  dissector(std::map<expr::ast, search::state>& state_map, actor_ptr const& qry)
    : state_(state_map),
      query_{qry}
  {
  }

  virtual void visit(expr::constant const&) { }
  virtual void visit(expr::timestamp_extractor const&) { }
  virtual void visit(expr::name_extractor const&) { }
  virtual void visit(expr::id_extractor const&) { }
  virtual void visit(expr::offset_extractor const&) { }
  virtual void visit(expr::type_extractor const&) { }

  virtual void visit(expr::conjunction const& conj)
  {
    expr::ast ast{std::unique_ptr<expr::node>(conj.clone())};
    if (parent_ && state_.count(ast))
      state_[ast].parents.insert(parent_);
    if (! parent_)
      state_[ast].queries.insert(query_);
    parent_ = ast;
    for (auto& clause : conj.operands)
      clause->accept(*this);
  }

  virtual void visit(expr::disjunction const& disj)
  {
    expr::ast ast{std::unique_ptr<expr::node>(disj.clone())};
    if (parent_ && state_.count(ast))
      state_[ast].parents.insert(parent_);
    if (! parent_)
      state_[ast].queries.insert(query_);
    parent_ = ast;
    for (auto& term : disj.operands)
      term->accept(*this);
  }

  virtual void visit(expr::relation const& rel)
  {
    assert(parent_);
    expr::ast ast{std::unique_ptr<expr::node>(rel.clone())};
    auto& s = state_[ast];
    if (parent_ && state_.count(ast))
      s.parents.insert(parent_);
  }

private:
  expr::ast parent_;
  std::map<expr::ast, search::state>& state_;
  actor_ptr const& query_;
};

} // namespace <anonymous>

void search::act()
{
  become(
      on(atom("DOWN"), arg_match) >> [=](uint32_t)
      {
        auto& client = last_sender();
        VAST_LOG_ACTOR_INFO("removes queries from client @" << client->id());
        assert(clients_.count(client));
        auto er = clients_.equal_range(client);
        for (auto i = er.first; i != er.second; ++i)
        {
          assert(queries_.count(i->second));
          send_exit(queries_[i->second], exit::stop);
        }
        clients_.erase(client);
      },
      on(atom("query"), atom("create"), arg_match) >> [=](std::string const& q)
      {
        auto client = last_sender();
        expr::ast ast{q};
        if (! ast)
        {
          reply(actor_ptr{});
          return;
        }
        auto qry = spawn<query>(archive_, index_, client, ast);
        if (! queries_.count(ast))
          queries_.emplace(ast, qry);
        dissector d{state_, qry};
        ast.accept(d);
        auto found = false;
        auto er = clients_.equal_range(client);
        for (auto i = er.first; i != er.second; ++i)
          if (i->second == ast)
          {
            found = true;
            break;
          }
        if (! found)
          clients_.emplace(client, std::move(ast));
        monitor(client);
        reply(qry);
      });
}

char const* search::description() const
{
  return "search";
}

} // namespace vast
