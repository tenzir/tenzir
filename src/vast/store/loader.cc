#include "vast/store/loader.h"

#include <ze/event.h>
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/exception.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

loader::loader(ze::component<ze::event>& c)
  : ze::component<ze::event>::source(c)
{
}

void loader::init(fs::path const& directory)
{
    LOG(verbose, store) << "initializing loader from archive " << directory;
    if (! fs::exists(directory))
        throw archive_exception("archive directory not found");

    dir_ = directory;
}

void loader::run()
{
    load(dir_);
}

void loader::load(fs::path const& dir)
{
    fs::each_dir_entry(
        dir_,
        [&](fs::path const& p)
        {
            if (fs::is_directory(p))
                load(dir);

            if (! fs::is_file(p))
                return;

            LOG(verbose, store) << "loading events from file " << p;

            try
            {
                fs::ifstream file(p, std::ios::binary | std::ios::in);
                isegment segment(file);
                segment.get([&](ze::event_ptr&& event)
                            {
                                forward(std::move(event));
                            });
            }
            catch (segment_exception const& e)
            {
                LOG(error, store) << e.what();
            }
            catch (ze::serialization::exception const& e)
            {
                LOG(error, store) << e.what();
            }
        });
}

void loader::forward(ze::event_ptr&& event)
{
    send(event);
}

} // namespace store
} // namespace vast
