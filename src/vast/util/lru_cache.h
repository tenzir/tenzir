#ifndef VAST_UTIL_LRU_CACHE
#define VAST_UTIL_LRU_CACHE

#include <cassert>
#include <functional>
#include <unordered_map>
#include <list>
#include "vast/serialization.h"
#include "vast/util/iterator.h"

namespace vast {
namespace util {

// A fixed-size cache with LRU eviction policy.
// @tparam Key The key type when performing cache lookups.
// @tparam Value The value type of the cache lookup table.
// @tparam Map The map type used as cache table.
template <
  typename Key,
  typename Value,
  template <typename...> class Map = std::unordered_map
>
class lru_cache
{
public:
  using key_type = Key;
  using mapped_type = Value;

  /// Monitors key usage, with the most recently accessed key at the back and
  /// key to be evicted next at the front.
  using tracker = std::list<key_type>;

  /// The cache table holding the hot entries.
  using cache = Map<
    key_type,
    std::pair<mapped_type, typename tracker::iterator>
  >;

  /// The callback to invoke for evicted elements.
  using evict_callback = std::function<void(key_type const&, mapped_type&)>;

  class const_iterator
    : public iterator_adaptor<
        const_iterator,
        typename cache::const_iterator,
        std::forward_iterator_tag,
        std::tuple<key_type const&, mapped_type const&>
      >
  {
    using super = iterator_adaptor<
      const_iterator,
      typename cache::const_iterator,
        std::forward_iterator_tag,
        std::tuple<key_type const&, mapped_type const&>
    >;

  public:
    using super::super;

  private:
    friend util::iterator_access;

    std::tuple<key_type const&, mapped_type const&> dereference() const
    {
      return std::tie(this->base()->first, this->base()->second.first);
    }
  };

  /// Constructs an LRU cache with a fixed number of elements.
  /// @param capacity The maximum number of elements in the cache.
  lru_cache(size_t capacity)
    : capacity_{capacity}
  {
    assert(capacity_ > 0);
  }

  /// Sets a callback for elements to be evicted.
  /// @param fun The function to invoke with the element being evicted.
  void on_evict(evict_callback fun)
  {
    on_evict_ = fun;
  }

  /// Retrieves a value for a given key. If the key exists in the cache, the
  /// function returns the corresponding iterator and registers the key as
  /// accessed.
  /// @param key The key to lookup
  /// @returns An iterator for *key* or the end iterator if *key* is not hot.
  mapped_type* lookup(key_type const& key)
  {
    auto i = find(key);
    return i == cache_.end() ? nullptr : &i->second.first;
  }

  /// Inserts a fresh entry in the cache.
  /// @param key The key mapping to *value*.
  /// @param value The value for *key*.
  /// @returns An pair of an iterator and boolean flag. If the flag is `true`,
  ///          the iterator points to *value*. If the flag is `false`, the
  ///          iterator points to the existing value for *key*.
  std::pair<mapped_type*, bool> insert(key_type const& key, mapped_type value)
  {
    auto i = find(key);
    if (i != cache_.end())
      return {&i->second.first, false};
    if (cache_.size() == capacity_) 
      evict();
    auto t = tracker_.insert(tracker_.end(), key);
    auto j = cache_.emplace(key, make_pair(std::move(value), std::move(t)));
    return {&j.first->second.first, true};
  }

  const_iterator begin() const
  {
    return const_iterator{cache_.begin()};
  }

  const_iterator end() const
  {
    return const_iterator{cache_.end()};
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
    tracker_.clear();
    cache_.clear();
  }

private:
  typename cache::iterator find(key_type const& key)
  {
    auto i = cache_.find(key);
    if (i != cache_.end())
      // Move accessed key to the back.
      tracker_.splice(tracker_.end(), tracker_, i->second.second);
    return i;
  }

  // Purges the least-recently-used element in the cache.
  void evict()
  {
    assert(! tracker_.empty());
    auto i = cache_.find(tracker_.front());
    assert(i != cache_.end());
    if (on_evict_)
      on_evict_(i->first, i->second.first);
    cache_.erase(i);
    tracker_.pop_front();
  }

  size_t const capacity_;
  tracker tracker_;
  cache cache_;
  evict_callback on_evict_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << capacity_;
    sink << static_cast<uint64_t>(tracker_.size());
    for (auto& key : tracker_)
      sink << key << cache_[key].first;
  }

  void deserialize(deserializer& source)
  {
    source >> capacity_;
    uint64_t size;
    source >> size;
    key_type k;
    mapped_type v;
    for (uint64_t i = 0; i < size; ++i)
    {
      source >> k >> v;
      auto it = tracker_.insert(tracker_.end(), k);
      cache_.emplace(std::move(k), {std::move(v), it});
    }
  }
};

} // namespace vast
} // namespace util

#endif
