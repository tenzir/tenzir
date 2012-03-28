#include "vast/store/emit.h"

#include "vast/util/logger.h"

namespace vast {
namespace store {

emit::emit(ze::io& io)
  : ze::component<ze::event>(io)
  , loader_(*this)
{
}

void emit::init(fs::path const& directory)
{
    loader_.init(directory);
}

void emit::run()
{
    loader_.run();
}

} // namespace store
} // namespace vast
