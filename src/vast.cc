#include "vast.h"
#include "vast/logger.h"
#include "vast/detail/type_manager.h"

namespace vast {

bool cleanup()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  detail::type_manager::destruct();
  logger::destruct();

  return true;
}

} // namespace vast
