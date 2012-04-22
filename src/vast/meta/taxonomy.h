#ifndef VAST_META_TAXONOMY_H
#define VAST_META_TAXONOMY_H

#include <string>
#include <unordered_map>
#include <ze/intrusive.h>
#include "vast/fs/path.h"
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Specifies and manages the event meta information.
class taxonomy
{
    taxonomy(taxonomy const&) = delete;
    taxonomy& operator=(taxonomy) = delete;

public:
    /// Constructs a taxonomy.
    taxonomy();

    ~taxonomy();

    /// Loads a taxonomy.
    /// @param contents The contents of a taxonomy file.
    void load(std::string const& contents);

    /// Loads a taxonomy from a file.
    /// @param filename The taxonomy file.
    void load(fs::path const& filename);

    /// Saves the taxonomy to a given filename.
    /// @param filename The taxonomy file.
    void save(fs::path const& filename) const;

    /// Create a string representation of the taxonomy.
    // @return A string of the parsed AST from the taxonomy file.
    std::string to_string() const;

private:
    // We keep the symbol "tables" as a vector to keep the symbols in the same
    // order as declared by the user, which becomes useful when transforming
    // and printing the taxonomy.
    std::vector<type_ptr> types_;
    std::vector<event_ptr> events_;
};

} // namespace meta
} // namespace vast

#endif
