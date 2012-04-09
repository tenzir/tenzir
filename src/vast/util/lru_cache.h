#ifndef VAST_UTIL_LRU_CACHE
#define VAST_UTIL_LRU_CACHE

#include <cassert>
#include <unordered_map>
#include <list>

namespace vast {
namespace util {

// A fixed-size cache with LRU eviction policy.
// @tparam K The key type when performing cache lookups.
// @tparam V The value type of the cache lookup table.
// @tparam Map The map type used as cache table.
template <
    typename K
  , typename V
  , template <typename...> class Map = std::unordered_map
>
class lru_cache
{
public:
    typedef K key_type;
    typedef V value_type;

    /// Invoked for each cache miss to retrieve a value for a given key.
    typedef std::function<value_type(key_type const&)> miss_function;

    /// Monitors key usage, with the most recently accessed key at the back.
    typedef std::list<key_type> tracker;

    /// The cache table holding the hot entries.
    typedef Map<
      key_type
    , std::pair<value_type, typename tracker::iterator>
    > cache;

    typedef typename cache::iterator iterator;
    typedef typename cache::const_iterator const_iterator;

    /// Constructs an LRU cache with a fixed number of elements.
    /// @param capacity The maximum number of elements in the cache.
    /// @param f The function to invoke for each cache miss.
    lru_cache(size_t capacity, miss_function f)
      : capacity_(capacity)
      , miss_function_(f)
    {
        assert(capacity_);
    }

    // Retrieves a value of the cached function for k
    value_type& retrieve(key_type const& key)
    {
        auto i = cache_.find(key);
        if (i == cache_.end())
            return insert(key, miss_function_(key))->second.first;

        // Move accessed key to end of tracker.
        tracker_.splice(tracker_.end(), tracker_, i->second.second);
        return i->second.first;
    }

    // Inserts a fresh entry in the cache.
    typename cache::iterator insert(key_type const& key, value_type val)
    {
        assert(cache_.find(key) == cache_.end());

        if (cache_.size() == capacity_) 
            evict();

        auto t = tracker_.insert(tracker_.end(), key);
        auto i = cache_.emplace(key,
                                std::make_pair(std::move(val), std::move(t)));

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

private:
    // Purges the least-recently-used element in the cache.
    void evict()
    {
        assert(!tracker_.empty());

        auto i = cache_.find(tracker_.front());
        assert(i != cache_.end());

        cache_.erase(i);
        tracker_.pop_front();
    }

    size_t const capacity_;
    miss_function miss_function_;

    tracker tracker_;
    cache cache_;
};

} // namespace vast
} // namespace util

#endif
