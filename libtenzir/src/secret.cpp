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

namespace {
using elements_offsets_t
  = std::vector<flatbuffers::Offset<fbs::data::StructuredSecretElement>>;

auto create_element(flatbuffers::FlatBufferBuilder& fbb, std::string_view name,
                    std::string_view operations, bool is_literal) {
  return fbs::data::CreateStructuredSecretElement(
    fbb, fbb.CreateString(name), fbb.CreateString(operations), is_literal);
}

auto create_buffer(flatbuffers::FlatBufferBuilder& fbb,
                   elements_offsets_t* element_offsets) {
  const auto count = fbs::data::CreateSecretDirect(fbb, element_offsets);
  fbb.Finish(count);
  auto buffer = detail::secrets::owning_root_fbs_buffer::make(fbb.Release());
  TENZIR_ASSERT(buffer);
  return std::move(*buffer).as_child();
}

auto create_buffer_with(std::string_view name, std::string_view operations,
                        bool is_literal) {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets = elements_offsets_t{};
  element_offsets.emplace_back(
    create_element(fbb, name, operations, is_literal));
  return create_buffer(fbb, &element_offsets);
}

auto copy_from_to(flatbuffers::FlatBufferBuilder& fbb,
                  const fbs::data::Secret* ptr, elements_offsets_t& out) {
  for (const auto* e : *(ptr->elements())) {
    const auto name = e->name()->string_view();
    const auto operations = e->operations()->string_view();
    const auto is_literal = e->is_literal();
    out.emplace_back(create_element(fbb, name, operations, is_literal));
  }
}

auto create_buffer_with(const fbs::data::Secret* ptr) {
  TENZIR_ASSERT(ptr);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets = elements_offsets_t{};
  copy_from_to(fbb, ptr, element_offsets);
  return create_buffer(fbb, &element_offsets);
}

auto create_buffer_with_prepend(std::string_view literal,
                                const fbs::data::Secret* ptr) {
  TENZIR_ASSERT(ptr);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets = elements_offsets_t{};
  element_offsets.emplace_back(create_element(fbb, literal, {}, true));
  copy_from_to(fbb, ptr, element_offsets);
  return create_buffer(fbb, &element_offsets);
}

auto create_buffer_with_append(const fbs::data::Secret* ptr,
                               std::string_view literal) {
  TENZIR_ASSERT(ptr);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets = elements_offsets_t{};
  copy_from_to(fbb, ptr, element_offsets);
  element_offsets.emplace_back(create_element(fbb, literal, {}, true));
  return create_buffer(fbb, &element_offsets);
}

auto create_joined_buffer(const fbs::data::Secret* left,
                          const fbs::data::Secret* right) {
  TENZIR_ASSERT(left);
  TENZIR_ASSERT(right);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto element_offsets = elements_offsets_t{};
  copy_from_to(fbb, left, element_offsets);
  copy_from_to(fbb, right, element_offsets);
  return create_buffer(fbb, &element_offsets);
}

} // namespace
namespace detail::secrets {

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_prepended(
  std::string_view literal) const -> secret {
  return {create_buffer_with_prepend(literal, &*buffer)};
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(std::string_view literal) const
  -> secret {
  return {create_buffer_with_append(&*buffer, literal)};
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(
  const secret_common<viewing_fbs_buffer>& other) const -> secret {
  return {create_joined_buffer(&*buffer, &*other.buffer)};
}

template class secret_common<owning_fbs_buffer>;
// template class secret_common<viewing_fbs_buffer>;

} // namespace detail::secrets

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
