//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/model.hpp"

#include "tenzir/blob.hpp"
#include "tenzir/test/test.hpp"

namespace tenzir {

TEST("frequency-table merge checkpoints preserve nested key types") {
  const auto key_type = type{list_type{int64_type{}}};
  const auto checkpoint = record{
    {"model", "frequency_table"},
    {"version", uint64_t{1}},
    {"input_count", uint64_t{2}},
    {"count", uint64_t{2}},
    {"null_count", uint64_t{0}},
    {"value_type", "list<int64>"},
    {"values",
     list{
       record{{"value", list{}}, {"count", uint64_t{1}}},
       record{{"value", list{int64_t{1}}}, {"count", uint64_t{1}}},
     }},
    {"_checkpoint_key_type", blob{as_bytes(key_type)}},
  };
  auto envelope = parse_model_envelope(checkpoint);
  REQUIRE(envelope.is_ok());
  auto provider = find_model_plugin(envelope.unwrap());
  REQUIRE(provider.is_ok());
  auto state = provider.unwrap()->make_model_merge_state(checkpoint);
  REQUIRE(state.is_ok());
  auto merge_state = std::move(state).unwrap();
  auto visible = merge_state->get();
  CHECK(as<record>(visible).find("_checkpoint_key_type")
        == as<record>(visible).end());
  auto restored = merge_state->get_for_checkpoint();
  auto const& restored_record = as<record>(restored);
  auto const field = restored_record.find("_checkpoint_key_type");
  REQUIRE(field != restored_record.end());
  CHECK_EQUAL(as<blob>(field->second), blob{as_bytes(key_type)});
}

} // namespace tenzir
