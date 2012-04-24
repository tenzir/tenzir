#ifndef VAST_META_EVENT_H
#define VAST_META_EVENT_H

#include <iosfwd>
#include <string>
#include <vector>
#include <ze/intrusive.h>
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Event meta data.
class event : ze::intrusive_base<event>
{
    event(event const&) = delete;
    event& operator=(event) = delete;

public:
    /// Constructs an event.
    /// @param event The event declaration in the taxonomy AST.
    event(std::string const& name, std::vector<argument_ptr> const& args);

    ~event();

    /// Gets the event name.
    /// @return The event name.
    std::string const& name() const;

private:
    friend bool operator==(event const& x, event const& y);
    friend std::ostream& operator<<(std::ostream& out, event const& e);

    std::string name_;
    std::vector<argument_ptr> args_;
};

bool operator!=(event const& x, event const& y);

} // namespace meta
} // namespace vast

#endif
