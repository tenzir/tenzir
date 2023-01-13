//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE flatbuffer

#include "vast/flatbuffer.hpp"

#include "vast/data.hpp"
#include "vast/fbs/type.hpp"
#include "vast/flatbuffer.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

namespace vast {

TEST(lifetime) {
  auto fbt = flatbuffer<fbs::Type>{};
  int counter = 0;
  {
    auto rt = record_type{
      {"foo", ip_type{}},
    };
    auto chunk = chunk::copy(rt);
    chunk->add_deletion_step([&]() noexcept {
      ++counter;
    });
    auto maybe_fbt = flatbuffer<fbs::Type>::make(std::move(chunk));
    REQUIRE_NOERROR(maybe_fbt);
    fbt = std::move(*maybe_fbt);
    CHECK_EQUAL(counter, 0);
  }
  auto fbrt = fbt.slice(*fbt->type_as_record_type());
  REQUIRE_EQUAL(fbrt->fields()->size(), 1u);
  auto fbrtf = fbrt.slice(*fbrt->fields()->Get(0));
  CHECK_EQUAL(fbrtf->name()->string_view(), "foo");
  auto fbrtft = fbrtf.slice(*fbrtf->type_nested_root(), *fbrtf->type());
  CHECK_EQUAL(as_bytes(fbrtft.chunk()), as_bytes(ip_type{}));
  CHECK_EQUAL(counter, 0);
  fbt = {};
  CHECK_EQUAL(counter, 0);
  fbrt = {};
  CHECK_EQUAL(counter, 0);
  fbrtf = {};
  CHECK_EQUAL(counter, 0);
  fbrtft = {};
  CHECK_EQUAL(counter, 1);
}

namespace {

struct fixture : public fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(flatbuffer_fixture, fixture)

TEST(serialization) {
  auto fbt = flatbuffer<fbs::Type>{};
  {
    auto rt = record_type{
      {"foo", ip_type{}},
    };
    auto chunk = chunk::copy(rt);
    auto maybe_fbt = flatbuffer<fbs::Type>::make(std::move(chunk));
    REQUIRE_NOERROR(maybe_fbt);
    fbt = std::move(*maybe_fbt);
    auto fbt2 = roundtrip(fbt);
    CHECK_EQUAL(as_bytes(fbt.chunk()), as_bytes(fbt2.chunk()));
  }
  auto fbrt = fbt.slice(*fbt->type_as_record_type());
  auto fbrtf = fbrt.slice(*fbrt->fields()->Get(0));
  auto fbrtft = fbrtf.slice(*fbrtf->type_nested_root(), *fbrtf->type());
  auto fbrtft2 = roundtrip(fbrtft);
  CHECK_EQUAL(as_bytes(fbrtft.chunk()), as_bytes(fbrtft2.chunk()));
}

FIXTURE_SCOPE_END()

} // namespace vast
