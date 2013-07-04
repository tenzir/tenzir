#include "vast/shutdown.h"

#include "vast/logger.h"
#include "vast/detail/type_manager.h"

namespace vast {

void shutdown()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  detail::type_manager::destruct();
  logger::destruct();
}

} // namespace vast
