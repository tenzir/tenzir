#ifndef VAST_UTIL_LRU_CACHE
#define VAST_UTIL_LRU_CACHE

#include <cassert>
#include <unordered_map>
#include <list>
#include "vast/serialization.h"

namespace vast {
namespace util {

// A fixed-size cache with LRU eviction policy.
// @tparam K The key type when performing cache lookups.
// @tparam V The value type of the cache lookup table.
// @tparam Map The map type used as cache table.
template <
  typename K,
  typename V,
  template <typename...> class Map = std::unordered_map
>
class lru_cache
{
public:
  using key_type = K;
  using value_type = V;

  /// Invoked for each cache miss to retrieve a value for a given key.
  using miss_function = std::function<value_type(key_type const&)>;

  /// Monitors key usage, with the most recently accessed key at the back.
  using tracker = std::list<key_type>;

  /// The cache table holding the hot entries.
  using cache = Map<
    key_type,
    std::pair<value_type, typename tracker::iterator>
  >;

  using iterator = typename cache::iterator;
  using const_iterator = typename cache::const_iterator;

  /// Constructs an LRU cache with a fixed number of elements.
  /// @param capacity The maximum number of elements in the cache.
  /// @param f The function to invoke for each cache miss.
  lru_cache(size_t capacity, miss_function f)
    : capacity_{capacity},
      miss_function_{f}
  {
    assert(capacity_ > 0);
  }

  /// Retrieves a value for a given key. If the key does not exist in the
  /// cache, the function invokes the `miss_function` given at construction
  /// time.
  ///
  /// @param key The key to lookup
  ///
  /// @returns A reference to the value corresponding to *key*.
  value_type& retrieve(key_type const& key)
  {
    auto i = cache_.find(key);
    if (i == cache_.end())
      return insert(key, miss_function_(key))->second.first;

    // Move accessed key to end of tracker.
    tracker_.splice(tracker_.end(), tracker_, i->second.second);
    return i->second.first;
  }

  /// Retrieves the most recently accessed value.
  /// @returns A reference to the value which has been accessed most recently.
  /// @pre `! empty()`
  value_type& retrieve_latest()
  {
    assert(! empty());
    return retrieve(tracker_.back());
  }

  /// Inserts a fresh entry in the cache.
  /// @param key The key mapping to *value*.
  /// @param value The value for *key*.
  /// @returns An iterator to the freshly inserted element.
  /// @pre `key` must not exist in the cache.
  iterator insert(key_type const& key, value_type value)
  {
    assert(cache_.find(key) == cache_.end());

    if (cache_.size() == capacity_) 
      evict();

    auto t = tracker_.insert(tracker_.end(), key);
    auto i = cache_.emplace(key, make_pair(std::move(value), std::move(t)));

    assert(i.second);
    return i.first;
  }

  iterator begin()
  {
    return cache_.begin();
  }

  const_iterator begin() const
  {
    return cache_.begin();
  }

  iterator end()
  {
    return cache_.end();
  }

  const_iterator end() const
  {
    return cache_.end();
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
  // Purges the least-recently-used element in the cache.
  void evict()
  {
    assert(! tracker_.empty());
    auto i = cache_.find(tracker_.front());
    assert(i != cache_.end());
    cache_.erase(i);
    tracker_.pop_front();
  }

  size_t const capacity_;
  miss_function miss_function_;
  tracker tracker_;
  cache cache_;

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
    value_type v;
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
