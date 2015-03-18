#include "vast/logger.h"

namespace vast {

void cleanup()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  logger::destruct();
}

} // namespace vast
