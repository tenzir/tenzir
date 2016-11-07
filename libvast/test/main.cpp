#include <caf/detail/scope_guard.hpp>

#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/logger.hpp"

int main(int argc, char** argv) {
  if (!vast::detail::adjust_resource_consumption())
    return 1;
  if (!vast::logger::file(vast::logger::debug, "vast-unit-test.log"))
    return 1;
  auto destroyer = caf::detail::make_scope_guard([&] {
    vast::logger::destruct();
  });
  return caf::test::main(argc, argv);
}
