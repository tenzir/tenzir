#include <boost/test/unit_test.hpp>
#include "vast/util/lru_cache.h"

BOOST_AUTO_TEST_CASE(lru_cache)
{
    typedef vast::util::lru_cache<std::string, size_t> lru_cache;

    lru_cache c(2, [](std::string const& str) { return str.length(); });

    // Perform some accesses.
    c["x"];
    c["fu"];
    c["foo"];
    c["quux"];
    c["corge"];
    c["foo"];

    std::vector<std::string> v;
    std::transform(
        c.begin(),
        c.end(),
        std::back_inserter(v),
        [](lru_cache::cache::value_type const& pair) { return pair.first; });

    std::sort(v.begin(), v.end());
    decltype(v) expected{"corge", "foo"};
    BOOST_CHECK_EQUAL_COLLECTIONS(
        v.begin(), v.end(),
        expected.begin(), expected.end());
}
