#ifndef VAST_META_ARGUMENT_H
#define VAST_META_ARGUMENT_H

#include <iosfwd>
#include <string>
#include <vector>
#include <boost/operators.hpp>
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Contains meta information for one particular event argument.
class argument : boost::equality_comparable<argument>
{
public:
    /// Creates an argument of a specific type.
    /// @param name The name of the argument.
    /// @param type The type of the argument.
    argument(const std::string& name, type_ptr type);

    /// Compares two arguments for equality. Two arguments are equal if they
    /// have the same name and type.
    /// @param rhs The argument to compare with.
    /// @return @c true iff both arguments are equal.
    bool operator==(const argument& rhs) const;

    /// Gets the argument name.
    /// @return The argument name.
    const std::string& name() const;

    /// Gets the argument type.
    /// @return The type of the argument.
    type_ptr type() const;

    /// Create a human-readable representation of the argument.
    // @return A string describing argument name and type.
    std::string to_string() const;

private:
    std::string name_;
    type_ptr type_;
};

} // namespace meta
} // namespace vast

#endif
