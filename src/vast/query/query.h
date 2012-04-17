#ifndef VAST_QUERY_QUERY_H
#define VAST_QUERY_QUERY_H

#include <string>
#include <ze/vertex.h>
#include "vast/query/boolean_expression.h"

namespace vast {
namespace query {

/// The query.
class query : public ze::object
            , public ze::device<ze::subscriber<>, ze::serial_dealer<>>
{
    query(query const& other) = delete;
    query& operator=(query other) = delete;

    typedef ze::device<ze::subscriber<>, ze::serial_dealer<>> device;

public:
    typedef std::function<void(uint64_t, uint64_t)> batch_function;

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

    /// Constructs a query from a query expression.
    /// @param c The component this query belongs to.
    /// @param str The query expression.
    /// @param batch_size TODO
    /// @param each_batch TODO
    query(ze::component& c,
          std::string str,
          uint64_t batch_size = 0ull,
          batch_function each_batch = batch_function());

    // Links the query device frontend with the backend.
    // @todo Find out why we cannot get rid of this function and move its code
    // into the constructor.
    void relay();

    /// Tests whether an event matches the query.
    /// @param event The event to match.
    /// @return @c true if @a event satisfies the query.
    bool match(ze::event const& event);

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
    uint64_t matched_ = 0ull;   // TODO: Use boost::accumulators.
    uint64_t processed_ = 0ull; // TODO: Use boost::accumulators.
    uint64_t batch_size_;
    batch_function each_batch_;
};

} // namespace query
} // namespace vast

#endif
