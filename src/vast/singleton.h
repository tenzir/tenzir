#ifndef VAST_SINGLETON_H
#define VAST_SINGLETON_H

#include <atomic>

namespace vast {

/// A singleton mixin.
/// Clients must provide the following functions:
///
/// - `static T* create()`: constructs an intance of type `T`.
/// - `void initialize()`: Initializes a successfully created `T`.
/// - `void dispose()`: Destroys an unsucessfully created `T`.
/// - `void destroy()`: Destroys a successfully created `T`.
///
/// These shall be implemented as private methods (as they do not constistute
/// part of the interface) and the client should befriend the singleton base.
///
/// The constructor of `T` shall not perform expensive operations because the
/// singleton_manager may construct more than one instance of `T` during object
/// creation.
///
/// @note This CAS-style singleton implementation originally comes from Dominik
/// Charousset's libcppa.
template <typename T>
class singleton
{
  singleton(singleton const&) = delete;
  singleton& operator=(singleton const&) = delete;

public:
  /// Retrieves the one-and-only instance of `T`.
  /// @return A pointer to access the instance of `T`.
  static T* instance()
  {
    T* result = ptr.load();
    while (result == nullptr)
    {
      T* tmp = T::create();
      if (ptr.load() == nullptr)
      {
        tmp->initialize();
        if (ptr.compare_exchange_weak(result, tmp))
          result = tmp;
        else
          tmp->dispose();
      }
    }
    return result;
  }

  /// Destroys the instance of `T`.
  static void destruct()
  {
    do
    {
      T* p = ptr.load();
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

protected:
  singleton() = default;

private:
  static std::atomic<T*> ptr;
};

template <typename T>
std::atomic<T*> singleton<T>::ptr;

} // namespace vast

#endif
