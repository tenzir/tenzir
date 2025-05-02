//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret.hpp"

#include "tenzir/fbs/data.hpp"

#include <arrow/array/array_binary.h>

namespace tenzir {

namespace detail::secrets {

template class secret_common<owning_fbs_buffer>;
// template class secret_common<viewing_fbs_buffer>;

} // namespace detail::secrets

namespace {
auto create_buffer_with(std::string_view name, std::string_view operations,
                        bool is_literal) {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets
    = std::vector<flatbuffers::Offset<fbs::data::StructuredSecretElement>>{};
  element_offsets.emplace_back(fbs::data::CreateStructuredSecretElement(
    fbb, fbb.CreateString(name), fbb.CreateString(operations), is_literal));
  const auto count = fbs::data::CreateSecretDirect(fbb, &element_offsets);
  fbb.Finish(count);
  auto buffer = detail::secrets::owning_root_fbs_buffer::make(fbb.Release());
  TENZIR_ASSERT(buffer);
  return std::move(*buffer).as_child();
}

auto create_buffer_with(const fbs::data::Secret* ptr) {
  TENZIR_ASSERT(ptr);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets
    = std::vector<flatbuffers::Offset<fbs::data::StructuredSecretElement>>{};
  for (const auto* e : *(ptr->elements())) {
    const auto name = e->name()->string_view();
    const auto operations = e->operations()->string_view();
    const auto is_literal = e->is_literal();
    element_offsets.emplace_back(fbs::data::CreateStructuredSecretElement(
      fbb, fbb.CreateString(name), fbb.CreateString(operations), is_literal));
  }
  const auto count = fbs::data::CreateSecretDirect(fbb, &element_offsets);
  fbb.Finish(count);
  auto buffer = detail::secrets::owning_root_fbs_buffer::make(fbb.Release());
  TENZIR_ASSERT(buffer);
  return std::move(*buffer).as_child();
}

} // namespace

secret::secret(std::string_view name, std::string_view operations,
               bool is_literal)
  : impl{create_buffer_with(name, operations, is_literal)} {
}

auto secret::make_literal(std::string_view value) -> secret {
  return secret{value, {}, true};
}

auto secret::make_managed(std::string_view name) -> secret {
  return secret{name, {}, false};
}

auto secret::from_fb(const fbs::data::Secret* ptr) -> secret {
  return secret{create_buffer_with(ptr)};
}

secret_view::secret_view(const secret& s) : impl{s.buffer} {
}

} // namespace tenzir
