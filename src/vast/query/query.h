#ifndef VAST_QUERY_QUERY_H
#define VAST_QUERY_QUERY_H

#include <string>
#include <ze/forward.h>
#include <ze/uuid.h>
#include "vast/query/boolean_expression.h"

namespace vast {
namespace query {

/// The query.
class query
{
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

    /// Constructs a query from a string.
    /// @param str The query string.
    query(std::string str);

    query(query&& other);
    query& operator=(query other);

    /// Tests whether an event matches the query.
    /// @param event The event to match.
    /// @return @c true if @a event satisfies the query.
    bool match(ze::event_ptr event);

    /// Gets the query ID.
    /// @return The query ID.
    ze::uuid const& id() const;

    /// Gets the query state.
    /// @return The current state of the query.
    state status() const;

    /// Sets the query state.
    /// @param state The query state
    void status(state s);

private:
    ze::uuid id_;
    state state_;
    std::string str_;
    boolean_expression expr_;
};

} // namespace query
} // namespace vast

#endif
