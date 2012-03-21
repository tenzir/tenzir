/// Interim std::make_unique until the standard ships with an appropriate
/// implementation.

#ifndef VAST_UTIL_MAKE_UNIQUE_H
#define VAST_UTIL_MAKE_UNIQUE_H

#include <memory>
#include <utility>

namespace std
{
    template<typename T, typename ...Args>
    std::unique_ptr<T> make_unique(Args&& ...args)
    {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
}

#endif
