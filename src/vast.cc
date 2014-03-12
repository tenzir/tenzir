#include "vast.h"
#include "vast/logger.h"
#include "vast/type_info.h"
#include "vast/detail/type_manager.h"

namespace vast {

bool initialize()
{
  announce_builtin_types();

  return true;
}

bool cleanup()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  detail::type_manager::destruct();
  logger::destruct();

  return true;
}

} // namespace vast
