#ifndef VAST_QUERY_QUERY_H
#define VAST_QUERY_QUERY_H

#include <string>
#include <ze/forward.h>
#include "vast/util/uuid.h"

namespace vast {
namespace query {

/// The query.
class query
{
    query(query const&) = delete;
    query& operator=(query const&) = delete;

public:
    /// Query state.
    enum state
    {
        invalid,    ///< Invalid.
        parsed,     ///< Successfully parsed.
        validated,  ///< Structure and types validated.
        canonified, ///< Transformed into a unique representation.
        optimized,  ///< Optimized for execution.
        running,    ///< Executing.
        paused,     ///< Halted.
        aborted,    ///< Aborted.
        completed   ///< Completed.
    };

    /// Query type.
    enum class type : uint8_t
    {
        meta        = 0x01, ///< Involves event meta data.
        type        = 0x02, ///< Asks for values of specific types.
        taxonomy    = 0x04  ///< Refers to specific arguments via a taxonomy.
    };

    /// Constructs a query from a query expression.
    /// @param str The query string.
    query(std::string const& str);

    /// Tests whether an event matches the query.
    /// @param event The event to match.
    /// @return @c true if @a event satisfies the query.
    bool match(ze::event_ptr event);

    /// Gets the query ID.
    /// @return The query ID.
    util::uuid id() const;

    /// Gets the query state.
    /// @return The current state of the query.
    state status() const;

    /// Sets the query state.
    /// @param state The query state
    void status(state s);

private:
    util::uuid id_;
    state state_;
};

/// Tests whether an event matches a query.
/// @param q The query.
/// @param e The event to test.
bool match(query const& q, ze::event_ptr e);

} // namespace query
} // namespace vast

#endif
