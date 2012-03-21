#include "vast/fs/operations.h"

#include <boost/filesystem.hpp>
#include "vast/fs/exception.h"

namespace vast {
namespace fs {

bool exists(const path& p)
{
    return boost::filesystem::exists(p);
}

void mkdir(const path& p)
{
    if (! boost::filesystem::create_directories(p))
        throw dir_exception("mkdir", p.string().data());
}

} // namespace fs
} // namespace vast
