//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/replace_columns.hpp"
#include "tenzir/series.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/view3.hpp"

#include <arrow/array/array_binary.h>

#include <concepts>

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

constexpr static auto npos = static_cast<uint32_t>(-1);

auto copy_parts(flatbuffers::FlatBufferBuilder& fbb,
                const fbs::data::SecretConcatenation& concat,
                concat_offsets_t& offsets, uint32_t start = 0,
                uint32_t end = npos) -> void {
  const auto& secrets = deref(concat.secrets());
  offsets.reserve(offsets.size() + secrets.size());
  end = std::min(end, secrets.size());
  for (auto i = start; i < end; ++i) {
    const auto& child = deref(secrets[i]);
    offsets.push_back(copy(fbb, child));
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
    case decode_url:
      return encode_url;
    case encode_url:
      return decode_url;
    case decode_base58:
      return encode_base58;
    case encode_base58:
      return decode_base58;
    case encode_hex:
      return decode_hex;
    case decode_hex:
      return encode_hex;
  }
  TENZIR_UNREACHABLE();
}

} // namespace

auto copy(flatbuffers::FlatBufferBuilder& fbb, const fbs::data::Secret& s)
  -> secret_offset_t {
  const auto f = detail::overload{
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
  return tenzir::match(s, f);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::is_all_literal() const -> bool {
  constexpr static auto f = detail::overload{
    [](const fbs::data::SecretLiteral&) -> bool {
      return true;
    },
    [](const fbs::data::SecretName&) -> bool {
      return false;
    },
    [](this const auto& self,
       const fbs::data::SecretConcatenation& concat) -> bool {
      for (const auto* child : deref(concat.secrets())) {
        if (not tenzir::match(deref(child), self)) {
          return false;
        }
      }
      return true;
    },
    [](this const auto& self,
       const fbs::data::SecretTransformed& transformed) -> bool {
      return tenzir::match(deref(transformed.secret()), self);
    },
  };
  return tenzir::match(*buffer, f);
}

namespace {

template <bool prepend>
auto prepend_append_literal_impl(flatbuffers::FlatBufferBuilder& fbb,
                                 const fbs::data::Secret& s,
                                 std::string_view literal) -> secret_offset_t {
  const auto f = detail::overload{
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
    // Also performs a check for `concat[0]` or `concat[size-1]` respectively,
    // to join these.
    [&](const fbs::data::SecretConcatenation& concat) {
      concat_offsets_t offsets;
      if constexpr (prepend) {
        const auto& secrets = deref(concat.secrets());
        TENZIR_ASSERT(secrets.size() > 0);
        const auto& s0 = deref(secrets[0]);
        if (const auto* lit_0 = try_as<fbs::data::SecretLiteral>(s0)) {
          offsets.reserve(deref(concat.secrets()).size());
          const auto lit_0_text = deref(lit_0->value()).string_view();
          offsets.push_back(make_literal(fbb, literal, lit_0_text));
          copy_parts(fbb, concat, offsets, 1);
        } else {
          offsets.reserve(deref(concat.secrets()).size() + 1);
          offsets.push_back(make_literal(fbb, literal));
          copy_parts(fbb, concat, offsets, 0);
        }
      } else {
        const auto& secrets = deref(concat.secrets());
        TENZIR_ASSERT(secrets.size() > 0);
        const auto& sn = deref(secrets[secrets.size() - 1]);
        if (const auto* lit_n = try_as<fbs::data::SecretLiteral>(sn)) {
          offsets.reserve(deref(concat.secrets()).size());
          const auto lit_n_text = deref(lit_n->value()).string_view();
          copy_parts(fbb, concat, offsets, 0, secrets.size() - 1);
          offsets.push_back(make_literal(fbb, lit_n_text, literal));
        } else {
          offsets.reserve(deref(concat.secrets()).size() + 1);
          copy_parts(fbb, concat, offsets);
          offsets.push_back(make_literal(fbb, literal));
        }
      }
      return finish_concatenation(fbb, offsets);
    },
    // trafo + literal -> concat
    // literal + trafo -> concat
    [&](const fbs::data::SecretTransformed&) {
      auto offsets = concat_offsets_t{};
      offsets.reserve(2);
      if constexpr (prepend) {
        offsets.push_back(make_literal(fbb, literal));
        offsets.push_back(copy(fbb, s));
      } else {
        offsets.push_back(copy(fbb, s));
        offsets.push_back(make_literal(fbb, literal));
      }
      return finish_concatenation(fbb, offsets);
    },
  };
  return tenzir::match(s, f);
}

} // namespace

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_prepended(
  std::string_view literal) const -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto offset = prepend_append_literal_impl<true>(fbb, *buffer, literal);
  return finish_builder(fbb, offset);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(std::string_view literal) const
  -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto offset = prepend_append_literal_impl<false>(fbb, *buffer, literal);
  return finish_builder(fbb, offset);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_appended(
  const secret_common<viewing_fbs_buffer>& other) const -> secret {
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto f = detail::overload{
    // literal + literal -> literal
    [&](const fbs::data::SecretLiteral& l,
        const fbs::data::SecretLiteral& r) -> secret_offset_t {
      return make_literal(fbb, deref(l.value()).string_view(),
                          deref(r.value()).string_view());
    },
    // literal + any -> literal|concat
    [&](const fbs::data::SecretLiteral& l, const auto&) -> secret_offset_t {
      return prepend_append_literal_impl<true>(fbb, *other.buffer,
                                               deref(l.value()).string_view());
    },
    // any + literal -> literal|concat
    [&](const auto&, const fbs::data::SecretLiteral& r) -> secret_offset_t {
      return prepend_append_literal_impl<false>(fbb, *buffer,
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
        const concepts::none_of<fbs::data::SecretLiteral> auto&) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(deref(l.secrets()).size() + 1);
      copy_parts(fbb, l, secrets);
      secrets.push_back(copy(fbb, *other.buffer));
      return finish_concatenation(fbb, secrets);
    },
    // any + concat -> concat
    [&](const concepts::none_of<fbs::data::SecretLiteral> auto&,
        const fbs::data::SecretConcatenation& r) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(deref(r.secrets()).size() + 1);
      secrets.push_back(copy(fbb, *other.buffer));
      copy_parts(fbb, r, secrets);
      return finish_concatenation(fbb, secrets);
    },
    // any + any -> concat
    [&](const concepts::none_of<fbs::data::SecretLiteral,
                                fbs::data::SecretConcatenation> auto&,
        const concepts::none_of<fbs::data::SecretLiteral,
                                fbs::data::SecretConcatenation> auto&) {
      auto secrets = concat_offsets_t{};
      secrets.reserve(2);
      secrets.push_back(copy(fbb, *buffer));
      secrets.push_back(copy(fbb, *other.buffer));
      return finish_concatenation(fbb, secrets);
    },
  };
  const auto offset = tenzir::match(std::tie(*buffer, *other.buffer), f);
  return finish_builder(fbb, offset);
}

template <typename FlatbufferType>
auto secret_common<FlatbufferType>::with_operation(
  fbs::data::SecretTransformations operation) const -> secret {
  const auto inversion = inverse(operation);
  auto fbb = flatbuffers::FlatBufferBuilder{};
  const auto f = detail::overload{
    // any -> trafo
    [&](const concepts::none_of<fbs::data::SecretTransformed> auto&) {
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
  const auto offset = tenzir::match(*buffer, f);
  return finish_builder(fbb, offset);
}

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

auto replace_secrets(table_slice slice) -> std::pair<bool, table_slice> {
  auto f = detail::overload{
    [](const basic_series<secret_type>& s) -> std::optional<series> {
      auto b = arrow::StringBuilder{};
      check(b.Reserve(s.length()));
      for (auto i = int64_t{0}; i < s.array->length(); ++i) {
        if (s.array->IsNull(i)) {
          check(b.AppendNull());
          continue;
        }
        check(b.Append("***"));
      }
      return series{string_type{}, finish(b)};
    },
    [](const auto&) -> std::optional<series> {
      return std::nullopt;
    },
  };
  return replace(std::move(slice), f);
}

} // namespace tenzir
