#ifndef VAST_UTIL_CACHE
#define VAST_UTIL_CACHE

#include <functional>
#include <list>
#include <unordered_map>

#include "vast/util/assert.h"
#include "vast/util/iterator.h"

namespace vast {
namespace util {

#if 0
/// The concept class defining a cache algorithm policy.
/// @tparam T The key type of the cache.
template <typename T>
struct cache_policy
{
  using iterator = __unspecified__;
  using const_iterator = __unspecified__;

  /// Accesses the key pointed to by an iterator.
  void access(iterator i);

  /// Inserts a key.
  iterator insert(T key);

  /// Erases a key.
  size_t erase(T key);

  /// Evicts the next element and returns it.
  T evict() const;

  // For range semantics.
  const_iterator begin() const;
  const_iterator end() const;
};
#endif

namespace detail {

template <typename T>
class list_eviction_policy
{
  using tracker = std::list<T>;

public:
  using iterator = typename tracker::iterator;
  using const_iterator = typename tracker::const_iterator;

  size_t erase(T const& key)
  {
    auto i = std::find(tracker_.begin(), tracker_.end(), key);
    if (i == tracker_.end())
      return 0;
    tracker_.erase(i);
    return 1;
  }

  T evict()
  {
    VAST_ASSERT(! tracker_.empty());
    T victim{std::move(tracker_.front())};
    tracker_.pop_front();
    return victim;
  }

  const_iterator begin() const
  {
    return tracker_.begin();
  }

  const_iterator end() const
  {
    return tracker_.end();
  }

protected:
  tracker tracker_;
};

} // namespace detail

/// A *least recently used* (LRU) cache eviction policy.
template <typename T>
class lru : public detail::list_eviction_policy<T>
{
public:
  using typename detail::list_eviction_policy<T>::iterator;
  using typename detail::list_eviction_policy<T>::const_iterator;

  void access(iterator i)
  {
    this->tracker_.splice(this->tracker_.end(), this->tracker_, i);
  }

  iterator insert(T key)
  {
    return this->tracker_.insert(this->tracker_.end(), std::move(key));
  }
};

/// A *most recently used* (MRU) cache eviction policy.
template <typename T>
class mru : public detail::list_eviction_policy<T>
{
public:
  using typename detail::list_eviction_policy<T>::iterator;
  using typename detail::list_eviction_policy<T>::const_iterator;

  void access(iterator i)
  {
    this->tracker_.splice(this->tracker_.begin(), this->tracker_, i);
  }

  iterator insert(T key)
  {
    return this->tracker_.insert(this->tracker_.begin(), std::move(key));
  }
};

/// A direct-mapped cache with fixed capacity.
template <
  typename Key,
  typename Value,
  template <typename> class Policy = lru
>
class cache
{
public:
  using key_type = Key;
  using mapped_type = Value;
  using policy = Policy<key_type>;

  /// The callback to invoke for evicted elements.
  using evict_callback = std::function<void(key_type const&, mapped_type&)>;

  /// The cache cache_map holding the hot entries.
  using cache_map = std::unordered_map<
    key_type,
    std::pair<mapped_type, typename policy::iterator>
  >;

  class const_iterator :
    public iterator_facade<
       const_iterator,
       std::forward_iterator_tag,
       std::pair<key_type const&, mapped_type const&>,
       std::pair<key_type const&, mapped_type const&>
     >
  {
    friend cache;
    friend iterator_access;

    const_iterator(cache const* c, bool end)
      : cache_{c}
    {
      i_ = end ? cache_->policy_.end() : cache_->policy_.begin();
    }

    bool equals(const_iterator const& other) const
    {
      return i_ == other.i_;
    }

    void increment()
    {
      ++i_;
    }

    std::pair<key_type const&, mapped_type const&> dereference() const
    {
      auto i = cache_->cache_.find(*i_);
      return std::make_pair(i->first, i->second.first);
    }

    cache const* cache_;
    typename policy::const_iterator i_;
  };

  /// Constructs an LRU cache with a maximum number of elements.
  /// @param capacity The maximum number of elements in the cache.
  /// @pre `capacity > 0`
  cache(size_t capacity = 100)
    : capacity_{capacity}
  {
    VAST_ASSERT(capacity_ > 0);
  }

  /// Sets a callback for elements to be evicted.
  /// @param fun The function to invoke with the element being evicted.
  void on_evict(evict_callback fun)
  {
    on_evict_ = fun;
  }

  /// Accesses the value for a given key. If key does not exists,
  /// the function default-constructs a value of `mapped_type`.
  /// @param key The key to lookup.
  /// @returns The value corresponding to *key*.
  mapped_type& operator[](key_type const& key)
  {
    auto i = find(key);
    return i == cache_.end() ? *insert(key, {}).first : i->second.first;
  }

  /// Retrieves a value for a given key. If the key exists in the cache, the
  /// function returns the corresponding iterator and registers the key as
  /// accessed.
  /// @param key The key to lookup.
  /// @returns An iterator for *key* or the end iterator if *key* is not hot.
  mapped_type* lookup(key_type const& key)
  {
    auto i = find(key);
    return i == cache_.end() ? nullptr : &i->second.first;
  }

  /// Checks whether a given key has a cache entry *without* involving the
  /// eviction policy.
  /// @param key The key to lookup.
  /// @returns `true` iff *key* exists in the cache.
  bool contains(key_type const& key) const
  {
    return cache_.find(key) != cache_.end();
  }

  /// Inserts a fresh entry in the cache.
  /// @param key The key mapping to *value*.
  /// @param value The value for *key*.
  /// @returns An pair of an iterator and boolean flag. If the flag is `true`,
  ///          the iterator points to *value*. If the flag is `false`, the
  ///          iterator points to the existing value for *key*.
  std::pair<mapped_type*, bool> insert(key_type key, mapped_type value)
  {
    auto i = find(key);
    if (i != cache_.end())
      return {&i->second.first, false};
    if (cache_.size() == capacity_)
      evict();
    auto k = policy_.insert(key);
    auto j = cache_.emplace(std::move(key), make_pair(std::move(value), k));
    return {&j.first->second.first, true};
  }

  /// Removes an entry for a given key without invoking the eviction callback.
  /// @param key The key to remove.
  /// @returns The number of entries removed.
  size_t erase(key_type const& key)
  {
    auto i = cache_.find(key);
    if (i == cache_.end())
      return 0;
    policy_.erase(key);
    cache_.erase(i);
    return 1;
  }

  /// Retrieves the maximum number elements the cache can hold.
  /// @returns The cache's capacity.
  size_t capacity() const
  {
    return capacity_;
  }

  /// Adjusts the cache capacity and evicts elements if the new capacity is
  /// smaller than the previous one.
  /// @param c the new capacity.
  /// @pre `c > 0`
  void capacity(size_t c)
  {
    VAST_ASSERT(c > 0);
    auto victims = std::min(cache_.size(), capacity_ - c);
    for (size_t i = 0; i < victims; ++i)
      evict();
    capacity_ = c;
  }

  /// Retrieves the current number of elements in the cache.
  /// @returns The number of elements in the cache.
  size_t size() const
  {
    return cache_.size();
  }

  /// Checks whether the cache is empty.
  /// @returns `true` iff the cache holds no elements.
  bool empty() const
  {
    return cache_.empty();
  }

  /// Removes all elements from the cache.
  void clear()
  {
    policy_ = {};
    cache_.clear();
  }

  const_iterator begin() const
  {
    return const_iterator{this, false};
  }

  const_iterator end() const
  {
    return const_iterator{this, true};
  }

private:
  typename cache_map::iterator find(key_type const& key)
  {
    auto i = cache_.find(key);
    if (i != cache_.end())
      policy_.access(i->second.second);
    return i;
  }

  void evict()
  {
    auto i = cache_.find(policy_.evict());
    VAST_ASSERT(i != cache_.end());
    if (on_evict_)
      on_evict_(i->first, i->second.first);
    cache_.erase(i);
  }

  policy policy_;
  size_t capacity_;
  evict_callback on_evict_;
  cache_map cache_;
};

} // namespace vast
} // namespace util

#endif
