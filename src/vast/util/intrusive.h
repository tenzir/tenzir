#ifndef VAST_UTIL_INTRUSIVE_H
#define VAST_UTIL_INTRUSIVE_H

#include <atomic>
#include <cassert>
#include <iosfwd>
#include <functional>

namespace vast {
namespace util {

/// The base class for intrusively reference-counted objects.
/// @see http://drdobbs.com/article/print?articleId=229218807&dept_url=/cpp/.
/// @relates intrusive_ptr
template <typename Derived>
class intrusive_base
{
public:
  intrusive_base() = default;

  intrusive_base(intrusive_base const&)
  {
    // Do not copy the counter.
  }

  intrusive_base& operator=(intrusive_base const&)
  {
    // Do not copy the counter.
    return *this;
  }

  ~intrusive_base() = default;

  void swap(intrusive_base const&)
  {
    // Do not copy the counter.
  }

  size_t ref_count() const
  {
    return count_;
  }

  bool unique() const
  {
    return ref_count() == 1;
  }

  friend inline void ref(Derived* ptr)
  {
    ++((intrusive_base*)ptr)->count_;
  }

  friend inline void unref(Derived* ptr)
  {
    if (--((intrusive_base*)ptr)->count_ == 0)
      delete ptr;
  }

private:
  std::atomic_size_t count_{0};
};

/// An intrusive smart pointer.
/// @relates intrusive_base
template <typename T>
class intrusive_ptr
{
public:
  typedef T element_type;

  intrusive_ptr() = default;

  intrusive_ptr(T* p, bool add_ref = true)
    : ptr_(p)
  {
    if (ptr_ && add_ref)
      ref(ptr_);
  }

  intrusive_ptr(intrusive_ptr const& other)
    : ptr_(other.get())
  {
    if (ptr_)
      ref(ptr_);
  }

  template <typename U>
  intrusive_ptr(intrusive_ptr<U> other)
    : ptr_(other.release())
  {
    static_assert(std::is_convertible<T*, U*>::value,
                  "U* cannot be assigned to T*");
  }

  intrusive_ptr(intrusive_ptr&& other)
    : ptr_(other.release())
  {
  }

  ~intrusive_ptr()
  {
    if (ptr_)
      unref(ptr_);
  }

  intrusive_ptr& operator=(intrusive_ptr other)
  {
    other.swap(*this);
    return *this;
  }

  intrusive_ptr& operator=(T* other)
  {
    reset(other);
    return *this;
  }

  void swap(intrusive_ptr& other)
  {
    std::swap(ptr_, other.ptr_);
  }

  void reset(T* other = nullptr)
  {
    if (ptr_)
      unref(ptr_);
    ptr_ = other;
    if (other)
      ref(other);
  }

  T* release()
  {
    auto p = ptr_;
    ptr_ = nullptr;
    return p;
  }

  void adopt(T* p)
  {
    reset();
    ptr_ = p;
  }

  T* get() const
  {
    return ptr_;
  }

  T& operator*() const
  {
    assert(ptr_ != nullptr);
    return *ptr_;
  }

  T* operator->() const
  {
    assert(ptr_ != nullptr);
    return ptr_;
  }

  explicit operator bool() const
  {
    return ptr_ != nullptr;
  }

private:
  T* ptr_ = nullptr;
};

template <typename T, typename U>
inline bool operator==(intrusive_ptr<T> const& x, intrusive_ptr<U> const& y)
{
  return x.get() == y.get();
}

template <typename T, typename U>
inline bool operator!=(intrusive_ptr<T> const& x, intrusive_ptr<U> const& y)
{
  return x.get() != y.get();
}

template <typename T, typename U>
inline bool operator==(intrusive_ptr<T> const& x, U* y)
{
  return x.get() == y;
}

template <typename T, typename U>
inline bool operator!=(intrusive_ptr<T> const& x, U* y)
{
  return x.get() != y;
}

template <typename T, typename U>
inline bool operator==(T* x, intrusive_ptr<U> const& y)
{
  return x == y.get();
}

template <typename T, typename U>
inline bool operator!=(T* x, intrusive_ptr<U> const& y)
{
  return x != y.get();
}

template <typename T>
inline bool operator<(intrusive_ptr<T> const& x, intrusive_ptr<T> const& y)
{
  return std::less<T*>()(x.get(), y.get());
}

template <typename T>
void swap(intrusive_ptr<T>& first, intrusive_ptr<T>& second)
{
  first.swap(second);
}

template <typename T>
std::ostream& operator<<(std::ostream& out, intrusive_ptr<T> const& x)
{
  out << x.get();
  return out;
}

/// Helper function to create a an intrusive_ptr in an exception-safe way.
/// @relates intrusive_ptr
template<typename T, typename ...Args>
intrusive_ptr<T> make_intrusive(Args&& ...args)
{
  return intrusive_ptr<T>(new T(std::forward<Args>(args)...));
}

} // namespace util

using util::intrusive_base;
using util::intrusive_ptr;
using util::make_intrusive;

} // namespace vast

#endif
