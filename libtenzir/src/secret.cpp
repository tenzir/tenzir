//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret.hpp"

#include "tenzir/fbs/data.hpp"
#include "tenzir/logger.hpp"

#include <arrow/array/array_binary.h>

namespace tenzir {
namespace detail::secrets {

namespace {

auto finish_literal(flatbuffers::FlatBufferBuilder& fbb,
                    const flatbuffers::Offset<flatbuffers::String>& offset)
  -> secret_offset_t {
  const auto lit_offset = fbs::data::CreateSecretLiteral(fbb, offset);
  return fbs::data::CreateSecret(fbb, fbs::data::SecretUnion::literal,
                                 lit_offset.Union());
}

auto finish_name(flatbuffers::FlatBufferBuilder& fbb,
                 const flatbuffers::Offset<flatbuffers::String>& offset)
  -> secret_offset_t {
  const auto lit_offset = fbs::data::CreateSecretLiteral(fbb, offset);
  return fbs::data::CreateSecret(fbb, fbs::data::SecretUnion::name,
                                 lit_offset.Union());
}

using concat_offsets_t = std::vector<secret_offset_t>;
auto finish_concatenation(flatbuffers::FlatBufferBuilder& fbb,
                          const concat_offsets_t& offsets) -> secret_offset_t {
  const auto vec_offset = fbb.CreateVector(offsets);
  const auto concat_offset
    = fbs::data::CreateSecretConcatenation(fbb, vec_offset);
  return fbs::data::CreateSecret(fbb, fbs::data::SecretUnion::concatenation,
                                 concat_offset.Union());
}

auto finish_transformation(flatbuffers::FlatBufferBuilder& fbb,
                           const secret_offset_t inner_offset,
                           const fbs::data::SecretTransformations trafo)
  -> secret_offset_t {
  const auto transformed_offset
    = fbs::data::CreateSecretTransformed(fbb, inner_offset, trafo);
  return fbs::data::CreateSecret(fbb, fbs::data::SecretUnion::transformed,
                                 transformed_offset.Union());
}

template <typename... StringViews>
  requires(std::same_as<StringViews, std::string_view> && ...)
auto make_literal(flatbuffers::FlatBufferBuilder& fbb, StringViews... parts)
  -> secret_offset_t {
  auto str = std::string{};
  str.reserve((std::size(parts) + ...));
  (str.append(parts), ...);
  const auto str_offset = fbb.CreateString(str);
  return finish_literal(fbb, str_offset);
}

auto make_name(flatbuffers::FlatBufferBuilder& fbb, std::string_view name)
  -> secret_offset_t {
  const auto str_offset = fbb.CreateString(name);
  return finish_name(fbb, str_offset);
}

auto copy_parts(flatbuffers::FlatBufferBuilder& fbb,
                const fbs::data::SecretConcatenation& concat,
                concat_offsets_t& offsets) -> void {
  auto& secrets = deref(concat.secrets());
  offsets.reserve(offsets.size() + secrets.size());
  for (auto* child : secrets) {
    offsets.push_back(copy(fbb, *child));
  }
}

auto finish_builder(flatbuffers::FlatBufferBuilder& fbb, secret_offset_t offset)
  -> secret {
  fbb.Finish(offset);
  auto new_buffer
    = detail::secrets::owning_root_fbs_buffer::make(fbb.Release());
  TENZIR_ASSERT(new_buffer);
  return {std::move(*new_buffer).as_child()};
}

constexpr auto inverse(fbs::data::SecretTransformations trafo)
  -> fbs::data::SecretTransformations {
  switch (trafo) {
    using enum fbs::data::SecretTransformations;
    case decode_base64:
      return encode_base64;
    case encode_base64:
      return decode_base64;
  }
}

template <typename T>
concept not_concat_or_invalid
  = concepts::one_of<T, fbs::data::SecretLiteral, fbs::data::SecretName,
                     fbs::data::SecretTransformed>;

template <typename T>
concept not_transformation_or_invalid
  = concepts::one_of<T, fbs::data::SecretLiteral, fbs::data::SecretName,
                     fbs::data::SecretConcatenation>;

} // namespace

auto copy(flatbuffers::FlatBufferBuilder& fbb, const fbs::data::Secret& s)
  -> secret_offset_t {
  const auto f = detail::overload{
    [](const std::monostate&) -> secret_offset_t {
      TENZIR_UNREACHABLE();
    },
    [&](const fbs::data::SecretLiteral& lit) -> secret_offset_t {
      return make_literal(fbb, deref(lit.value()).string_view());
    },
    [&](const fbs::data::SecretName& name) -> secret_offset_t {
      return make_name(fbb, deref(name.value()).string_view());
    },
    [&](const fbs::data::SecretConcatenation& concat) -> secret_offset_t {
      auto offsets = concat_offsets_t{};
      copy_parts(fbb, concat, offsets);
      return finish_concatenation(fbb, offsets);
    },
    [&](const fbs::data::SecretTransformed& transformed) -> secret_offset_t {
      const auto inner_offset = copy(fbb, deref(transformed.secret()));
      return finish_transformation(fbb, inner_offset,
                                   transformed.transformation());
    },
  };
  return match(s, f);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::is_all_literal() const -> bool {
  constexpr static auto f = detail::overload{
    [](const std::monostate&) -> bool {
      TENZIR_UNREACHABLE();
    },
    [](const fbs::data::SecretLiteral&) -> bool {
      return true;
    },
    [](const fbs::data::SecretName&) -> bool {
      return false;
    },
    [](this auto&& self, const fbs::data::SecretConcatenation& concat) -> bool {
      for (const auto* child : deref(concat.secrets())) {
        if (not match(deref(child), self)) {
          return false;
        }
      }
      return true;
    },
    [](this auto&& self,
       const fbs::data::SecretTransformed& transformed) -> bool {
      return match(deref(transformed.secret()), self);
    }};
  return match(*buffer, f);
}

namespace {

template <bool prepend>
auto prepend_append_impl(const fbs::data::Secret& s, std::string_view literal)
  -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto f = detail::overload{
    [](const std::monostate&) -> secret_offset_t {
      TENZIR_UNREACHABLE();
    },
    // literal + literal -> literal
    [&](const fbs::data::SecretLiteral& lit) {
      if constexpr (prepend) {
        return make_literal(fbb, literal, deref(lit.value()).string_view());
      } else {
        return make_literal(fbb, deref(lit.value()).string_view(), literal);
      }
    },
    // literal + name -> concat
    // name + literal -> concat
    [&](const fbs::data::SecretName& name) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(2);
      if constexpr (prepend) {
        secrets.push_back(make_literal(fbb, literal));
        secrets.push_back(make_name(fbb, deref(name.value()).string_view()));
      } else {
        secrets.push_back(make_name(fbb, deref(name.value()).string_view()));
        secrets.push_back(make_literal(fbb, literal));
      }
      return finish_concatenation(fbb, secrets);
    },
    // concat + literal -> concat
    // literal + concat -> concat
    [&](const fbs::data::SecretConcatenation& concat) {
      concat_offsets_t secrets;
      secrets.reserve(deref(concat.secrets()).size() + 1);
      if constexpr (prepend) {
        secrets.push_back(make_literal(fbb, literal));
      }
      copy_parts(fbb, concat, secrets);
      if constexpr (not prepend) {
        secrets.push_back(make_literal(fbb, literal));
      }
      return finish_concatenation(fbb, secrets);
    },
    // trafo + literal -> concat
    // literal + trafo -> concat
    [&](const fbs::data::SecretTransformed&) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(2);
      if constexpr (prepend) {
        secrets.push_back(make_literal(fbb, literal));
        secrets.push_back(copy(fbb, s));
      } else {
        secrets.push_back(copy(fbb, s));
        secrets.push_back(make_literal(fbb, literal));
      }
      return finish_concatenation(fbb, secrets);
    },
  };
  const auto offset = match(s, f);
  return finish_builder(fbb, offset);
}

} // namespace

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_prepended(
  std::string_view literal) const -> secret {
  return prepend_append_impl<true>(*buffer, literal);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(std::string_view literal) const
  -> secret {
  return prepend_append_impl<false>(*buffer, literal);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(
  const secret_common<viewing_fbs_buffer>& other) const -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto f = detail::overload{
    // Any monostate
    []<typename L, typename R>
      requires(std::same_as<std::monostate, L>
               or std::same_as<std::monostate, R>)
    (const L&, const R&) -> secret_offset_t {
      TENZIR_UNREACHABLE();
    },
    // literal + literal -> literal
    [&](const fbs::data::SecretLiteral& l, const fbs::data::SecretLiteral& r) {
      return make_literal(fbb, deref(l.value()).string_view(),
                          deref(r.value()).string_view());
    },
    // concat + concat -> concat
    [&](const fbs::data::SecretConcatenation& l,
        const fbs::data::SecretConcatenation& r) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(deref(l.secrets()).size() + deref(r.secrets()).size());
      copy_parts(fbb, l, secrets);
      copy_parts(fbb, r, secrets);
      return finish_concatenation(fbb, secrets);
    },
    // concat + any -> concat
    [&](const fbs::data::SecretConcatenation& l,
        const not_concat_or_invalid auto&) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(deref(l.secrets()).size() + 1);
      copy_parts(fbb, l, secrets);
      secrets.push_back(copy(fbb, *other.buffer));
      return finish_concatenation(fbb, secrets);
    },
    // any + concat -> concat
    [&](const not_concat_or_invalid auto&,
        const fbs::data::SecretConcatenation& r) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(deref(r.secrets()).size() + 1);
      secrets.push_back(copy(fbb, *other.buffer));
      copy_parts(fbb, r, secrets);
      return finish_concatenation(fbb, secrets);
    },
    // any + any -> concat
    [&](const not_concat_or_invalid auto&, const not_concat_or_invalid auto&) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(2);
      secrets.push_back(copy(fbb, *buffer));
      secrets.push_back(copy(fbb, *other.buffer));
      return finish_concatenation(fbb, secrets);
    },
  };
  const auto offset = match(std::tie(*buffer, *other.buffer), f);
  return finish_builder(fbb, offset);
}

template <typename FlatbufferType>
template <fbs::data::SecretTransformations operation>
auto secret_common<FlatbufferType>::with_operation() const -> secret {
  constexpr auto inversion = inverse(operation);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto f = detail::overload{
    [](const std::monostate&) -> secret_offset_t {
      TENZIR_UNREACHABLE();
    },
    // any -> trafo
    [&](const not_transformation_or_invalid auto&) {
      const auto inner_offset = copy(fbb, *buffer);
      return finish_transformation(fbb, inner_offset, operation);
    },
    // trafo -> secret | trafo
    [&](const fbs::data::SecretTransformed& transformed) {
      if (transformed.transformation() == inversion) {
        return copy(fbb, deref(transformed.secret()));
      }
      const auto inner_offset = copy(fbb, *buffer);
      return finish_transformation(fbb, inner_offset, operation);
    },
  };
  const auto offset = match(*buffer, f);
  return finish_builder(fbb, offset);
}

#define INSTANTIATE(OPERATION)                                                 \
  template auto secret_common<owning_fbs_buffer>::with_operation<OPERATION>()  \
    const -> secret
INSTANTIATE(fbs::data::SecretTransformations::decode_base64);
INSTANTIATE(fbs::data::SecretTransformations::encode_base64);
#undef INSTANTIATE

template class secret_common<owning_fbs_buffer>;

} // namespace detail::secrets

auto secret::make_literal(std::string_view value) -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto offset = detail::secrets::make_literal(fbb, value);
  return detail::secrets::finish_builder(fbb, offset);
}

auto secret::make_managed(std::string_view name) -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto offset = detail::secrets::make_name(fbb, name);
  return detail::secrets::finish_builder(fbb, offset);
}

auto secret::from_fb(const fbs::data::Secret& fb) -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  auto offset = detail::secrets::copy(fbb, fb);
  return detail::secrets::finish_builder(fbb, offset);
}

secret_view::secret_view(const secret& s) : impl{s.buffer} {
}

} // namespace tenzir
