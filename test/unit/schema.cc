#include "test.h"
#include "vast/schema.h"
#include "vast/to_string.h"
#include "vast/fs/fstream.h"
#include "vast/fs/path.h"

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

#define DEFINE_SCHEMA_TEST_CASE(name, input)                        \
  BOOST_AUTO_TEST_CASE(name)                                        \
  {                                                                 \
    vast::schema s0, s1;                                            \
    s0.read(input);                                                 \
                                                                    \
    auto str = vast::to_string(s0);                                 \
    s1.load(str);                                                   \
    BOOST_CHECK_EQUAL(str, vast::to_string(s1));                    \
  }

// Contains the test case defintions for all taxonomy test files.
#include "test/unit/schema_test_cases.h"
