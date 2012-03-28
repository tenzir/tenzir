#ifndef VAST_STORE_LOADER_H
#define VAST_STORE_LOADER_H

#include <ze/component.h>
#include <ze/source.h>
#include "vast/fs/path.h"

namespace vast {
namespace store {

/// Reads events from the archive on disk.
class loader : public ze::component<ze::event>::source
{
    loader(loader const&) = delete;
    loader& operator=(loader const&) = delete;

public:
    /// Constructs an loader.
    /// @param c The component the loader belongs to.
    loader(ze::component<ze::event>& c);

    /// Initializes the loader.
    /// @param directory The directory from which to load events.
    void init(fs::path const& directory);

    /// Starts the loader and blocks.
    void run();

private:
    void load(fs::path const& dir);
    void dispatch(ze::event_ptr&& event);

    fs::path dir_;
};

} // namespace store
} // namespace vast

#endif
