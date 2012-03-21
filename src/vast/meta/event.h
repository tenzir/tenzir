#ifndef VAST_META_EVENT_H
#define VAST_META_EVENT_H

#include <iosfwd>
#include <string>
#include <vector>
#include <boost/operators.hpp>
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Event meta data.
class event : boost::equality_comparable<event>
{
    event(event const&) = delete;
    event& operator=(event const&) = delete;

public:
    /// Constructs an event.
    /// @param event The event declaration in the taxonomy AST.
    event(const std::string& name, const std::vector<argument_ptr>& args);

    /// Compares two events. Two events are considered equal if they have the
    /// same name and the same arguments.
    /// @param rhs The event to compare with.
    /// @return @c true if both events are equal.
    bool operator==(const event& rhs) const;

    /// Gets the event name.
    /// @return The event name.
    const std::string& name() const;

    /// Applies a function or functor to each argument.
    /// @param f Function to apply to each argument.
    template <typename Function>
    void each_arg(Function f) const
    {
        for (argument_ptr arg : args_)
            f(arg);
    }

    /// Creates a human-readable representation of the event.
    // @return A string describing event plus arguments.
    std::string to_string() const;

private:
    std::string name_;               ///< The event name.
    std::vector<argument_ptr> args_; ///< The event arguments.
};

} // namespace meta
} // namespace vast

#endif
