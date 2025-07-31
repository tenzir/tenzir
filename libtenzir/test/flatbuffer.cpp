//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/flatbuffer.hpp"

#include "tenzir/detail/logger_formatters.hpp"
#include "tenzir/fbs/type.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <caf/binary_serializer.hpp>

namespace tenzir {

TEST("lifetime") {
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

} // namespace tenzir
