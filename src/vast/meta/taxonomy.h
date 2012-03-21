#ifndef VAST_META_TAXONOMY_H
#define VAST_META_TAXONOMY_H

#include <string>
#include <boost/noncopyable.hpp>
#include "vast/fs/path.h"
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Specifies and manages the event meta information.
class taxonomy : boost::noncopyable
{
public:
    /// Load the event taxonomy.
    /// \param contents The contents of a taxonomy file.
    void load(const std::string& contents);

    /// Load the event taxonomy.
    /// \param filename The taxonomy file.
    void load(const fs::path& filename);

    /// Save the current event taxonomy to a given filename.
    /// \param filename The taxonomy file.
    void save(const fs::path& filename) const;

    /// Create a string representation of the taxonomy.
    // \return A string of the parsed AST from the taxonomy file.
    std::string to_string() const;

private:
    std::string ast_;   ///< Normalized taxonomy AST.
    type_map types_;    ///< Global types indexed by name.
    event_map events_;  ///< Events indexed by name.
};

} // namespace meta
} // namespace vast

#endif
