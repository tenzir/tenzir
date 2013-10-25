#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/expression.h"
#include "vast/search_result.h"

namespace vast {

/// Manages queries and their relationships.
class search
{
public:
  struct state
  {
    search_result result;
    std::set<expr::ast> parents;
  };

  search() = default;

  /// Creates a new query for a given string and creates internal state.
  /// @param str The query string.
  /// @returns The AST respresentation of *str*.
  expr::ast add_query(std::string const& str);

  /// Updates the query state of a given AST with a new result.
  /// @param ast The predicate to update.
  /// @param sr The new result for *ast*.
  /// @returns The set of affected nodes.
  std::vector<expr::ast> update(expr::ast const& ast, search_result const& sr);

  /// Retrieves the result for a given AST.
  ///
  /// @param ast The AST to lookup.
  ///
  /// @returns A pointer to the result for *ast* or `nullptr` if *ast* does not
  /// exist.
  search_result const* result(expr::ast const& ast) const;

private:
  std::map<expr::ast, state> state_;
};

struct search_actor : actor<search_actor>
{
  struct query_state
  {
    query_state(cppa::actor_ptr q, cppa::actor_ptr c);

    cppa::actor_ptr query;
    cppa::actor_ptr client;
    search_result result;
  };

  search_actor(cppa::actor_ptr archive,
               cppa::actor_ptr index,
               cppa::actor_ptr schema_manager);

  void act();
  char const* description() const;
  
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  std::multimap<expr::ast, query_state> query_state_;
  search search_;
};

} // namespace vast

#endif
