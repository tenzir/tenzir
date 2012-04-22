#ifndef VAST_META_EVENT_H
#define VAST_META_EVENT_H

#include <iosfwd>
#include <string>
#include <vector>
#include <boost/operators.hpp>
#include <ze/intrusive.h>
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Event meta data.
class event : ze::intrusive_base<event>,
    boost::equality_comparable<event>
{
    event(event const&) = delete;
    event& operator=(event) = delete;
    friend std::ostream& operator<<(std::ostream& out, event const& e);

public:
    /// Constructs an event.
    /// @param event The event declaration in the taxonomy AST.
    event(std::string const& name, std::vector<argument_ptr> const& args);

    ~event();

    /// Compares two events. Two events are considered equal if they have the
    /// same name and the same arguments.
    /// @param other The event to compare with.
    /// @return @c true if both events are equal.
    bool operator==(event const& other) const;

    /// Gets the event name.
    /// @return The event name.
    std::string const& name() const;

private:
    std::string name_;
    std::vector<argument_ptr> args_;
};

} // namespace meta
} // namespace vast

#endif
