#include "vast/detail/singleton_manager.h"
#include "vast/logger.h"

namespace vast {
namespace detail {
namespace {

std::atomic<logger*> singleton_logger;

} // namespace <anonymous>

logger* singleton_manager::get_logger()
{
  return lazy_get(singleton_logger);
}

void singleton_manager::shutdown()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  destroy(singleton_logger);
}

} // namespace detail

void shutdown()
{
  detail::singleton_manager::shutdown();
}

} // namespace vast
