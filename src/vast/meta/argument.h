#ifndef VAST_META_ARGUMENT_H
#define VAST_META_ARGUMENT_H

#include <iosfwd>
#include <string>
#include <vector>
#include <ze/intrusive.h>
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Contains meta information for one particular event argument.
class argument : ze::intrusive_base<argument>
{
    argument(argument const&) = delete;
    argument& operator=(argument) = delete;

public:
    /// Creates an argument of a specific type.
    /// @param name The name of the argument.
    /// @param type The type of the argument.
    argument(std::string const& name, type_ptr type);

    /// Gets the argument name.
    /// @return The argument name.
    std::string const& name() const;

    /// Gets the argument type.
    /// @return The type of the argument.
    type_ptr type() const;

private:
    std::string name_;
    type_ptr type_;
};

bool operator==(argument const& x, argument const& y);
bool operator!=(argument const& x, argument const& y);

std::ostream& operator<<(std::ostream& out, argument const& a);

} // namespace meta
} // namespace vast

#endif
