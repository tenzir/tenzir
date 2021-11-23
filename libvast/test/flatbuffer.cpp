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
      {"foo", address_type{}},
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
  auto fbrt = fbt.slice(*fbt->type_as_record_type_v0());
  REQUIRE_EQUAL(fbrt->fields()->size(), 1u);
  auto fbrtf = fbrt.slice(*fbrt->fields()->Get(0));
  CHECK_EQUAL(fbrtf->name()->string_view(), "foo");
  auto fbrtft = fbrtf.slice(*fbrtf->type_nested_root(), *fbrtf->type());
  CHECK_EQUAL(as_bytes(fbrtft), as_bytes(address_type{}));
  CHECK_EQUAL(counter, 0);
  fbt = nullptr;
  CHECK_EQUAL(counter, 0);
  fbrt = nullptr;
  CHECK_EQUAL(counter, 0);
  fbrtf = nullptr;
  CHECK_EQUAL(counter, 0);
  fbrtft = nullptr;
  CHECK_EQUAL(counter, 1);
}

FIXTURE_SCOPE(flatbuffer_fixture, fixtures::deterministic_actor_system)

TEST(serialization) {
  auto fbt = flatbuffer<fbs::Type>{};
  {
    auto rt = record_type{
      {"foo", address_type{}},
    };
    auto chunk = chunk::copy(rt);
    auto maybe_fbt = flatbuffer<fbs::Type>::make(std::move(chunk));
    REQUIRE_NOERROR(maybe_fbt);
    fbt = std::move(*maybe_fbt);
    CHECK_ROUNDTRIP(fbt);
  }
  auto fbrt = fbt.slice(*fbt->type_as_record_type_v0());
  auto fbrtf = fbrt.slice(*fbrt->fields()->Get(0));
  auto fbrtft = fbrtf.slice(*fbrtf->type_nested_root(), *fbrtf->type());
  CHECK_ROUNDTRIP(fbrtft);
}

FIXTURE_SCOPE_END()

} // namespace vast
