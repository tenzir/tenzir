#ifndef VAST_DETAIL_SINGLETON_MANAGER_H
#define VAST_DETAIL_SINGLETON_MANAGER_H

#include <atomic>

namespace vast {

class logger;

namespace detail {

/// Manages all singletons in VAST.
/// In order to operate as a singleton, a type `T` must provide the
/// following functions:
///
/// - `T* create()`: constructs an intance of type `T`. (static)
/// - `void initialize()`: Initializes a successfully created `T`.
/// - `void dispose()`: Destroys an unsucessfully created `T`.
/// - `void destroy()`: Destroys a successfully created `T`.
///
/// The constructor of `T` shall not perform expensive operations because the
/// singleton_manager constructs more than one instance of `T` during object
/// creation.
///
/// @note This singleton design originally comes from Dominik Charousset's
/// libcppa. We use the same CAS-style implementation.
class singleton_manager
{
public:
  static logger* get_logger();

  /// Destroys all singletons.
  static void shutdown();

private:
  template <typename T>
  static T* lazy_get(std::atomic<T*>& ptr)
  {
    T* result = ptr.load();
    while (result == nullptr)
    {
      auto tmp = T::create();
      if (ptr.compare_exchange_weak(result, tmp))
      {
        tmp->initialize();
        result = tmp;
      }
      else
      {
        tmp->dispose();
      }
    }
    return result;
  }

  template <typename T>
  static void destroy(std::atomic<T*>& ptr)
  {
    do
    {
      auto p = ptr.load();
      if (p == nullptr)
        return;
      if (ptr.compare_exchange_weak(p, nullptr))
      {
        p->destroy();
        ptr = nullptr;
        return;
      }
    }
    while (true);
  }
};

} // namespace detail
} // namespace vast

#endif
