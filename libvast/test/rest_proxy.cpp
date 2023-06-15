//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/node.hpp>
#include <vast/test/fixtures/node.hpp>
#include <vast/test/test.hpp>

namespace {

struct fixture : public fixtures::node {
  fixture() : fixtures::node(VAST_PP_STRINGIFY(SUITE)) {
  }

  ~fixture() override = default;
};

} // namespace

FIXTURE_SCOPE(rest_api_tests, fixture)

TEST(proxy request) {
  auto desc = vast::http_request_description{
    .canonical_path = "POST /status (v0)",
    .params = {},
  };
  auto rp = self->request(test_node, caf::infinite, vast::atom::proxy_v, desc);
  run();
  rp.receive(
    [](vast::rest_response& response) {
      CHECK_EQUAL(response.code(), size_t{200});
      auto body = std::move(response).release();
      auto parsed = vast::from_json(body);
      CHECK(parsed);
    },
    [](const caf::error& e) {
      FAIL(e);
    });
}

FIXTURE_SCOPE_END()
