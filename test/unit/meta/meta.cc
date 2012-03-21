#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE meta

#include <boost/test/unit_test.hpp>
#include "vast/fs/fstream.h"
#include "vast/meta/taxonomy.h"
#include "vast/util/logger.h"

// Setting up a dummy logger.
namespace vast {
namespace util {
logger* LOGGER = new logger(0, 0, "/dev/null");
} // namespace util
} // namespace logger

// Bring the contents of a file into a std::string.
std::string load(const vast::fs::path& path)
{
    vast::fs::ifstream in(path);

    std::string storage;
    in.unsetf(std::ios::skipws); // No white space skipping.
    std::copy(std::istream_iterator<char>(in),
            std::istream_iterator<char>(),
            std::back_inserter(storage));

    return storage;
}

#define DEFINE_TAXONOMY_TEST_CASE(name, input, expected)                \
    BOOST_AUTO_TEST_CASE(name)                                          \
    {                                                                   \
        vast::meta::taxonomy t;                                         \
        vast::fs::path p(input);                                        \
        t.load(p);                                                      \
        auto expected_output = load(expected);                          \
                                                                        \
        BOOST_CHECK(t.to_string() == expected_output);                  \
    }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/meta/taxonomy_test_cases.h"
