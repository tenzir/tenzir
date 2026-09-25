//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ecc.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/fundamental_array_builder.hpp"
#include "tenzir/nova/stringify.hpp"
#include "tenzir/tql2/ast.hpp"

#include <span>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

struct Replacement {
  /// The stringified rows that do not hold a top-level secret.
  Array<String> text;
  /// The rows that hold a top-level secret, if any.
  Option<MaskedArray<Array<Secret>>> secrets;
};

using Segment = variant<std::string, Replacement>;

} // namespace

auto _::EvalRun::eval(ast::format_expr const& x, EvalFrame frame)
  -> Array<Data> {
  auto const& mask = frame.mask();
  auto segments = std::vector<Segment>{};
  segments.reserve(x.segments.size());
  // Rows that contain at least one top-level secret become secrets themselves.
  // Secrets nested in lists or records are stringified, and thus redacted.
  auto secret_rows = storage::BitMap{mask.length(), false};
  for (auto const& segment : x.segments) {
    match(
      segment,
      [&](std::string const& literal) {
        segments.emplace_back(literal);
      },
      [&](ast::format_expr::replacement const& replacement) {
        auto value = frame.eval(replacement.expr);
        auto secrets = value.get_alternative<Secret>();
        auto text_mask = mask;
        if (secrets) {
          secret_rows = std::move(secret_rows) | (mask & secrets->present);
          text_mask = mask.and_not(secrets->present);
        }
        segments.emplace_back(Replacement{
          .text = stringify(value, text_mask),
          .secrets = std::move(secrets),
        });
      });
  }
  if (not secret_rows.any()) {
    auto builder = ArrayBuilder<String>{};
    auto buffer = std::string{};
    for (auto index : storage::bitmap_iteration(mask)) {
      if (not index) {
        builder.skip();
        continue;
      }
      for (auto const& segment : segments) {
        match(
          segment,
          [&](std::string const& literal) {
            buffer.append(literal);
          },
          [&](Replacement const& replacement) {
            buffer.append(*replacement.text.get(*index));
          });
      }
      builder.data(buffer);
      buffer.clear();
    }
    return Array<Data>{builder.finish()};
  }
  auto builder = ArrayBuilder<Data>{};
  auto buffer = std::string{};
  // Plaintext secret bytes must only ever live in cleansing memory, so secret
  // rows are assembled here instead of in `buffer`.
  auto secret_buffer = ecc::cleansing_blob{};
  auto append_bytes = [&](std::span<const std::byte> bytes) {
    secret_buffer.insert(secret_buffer.end(), bytes.begin(), bytes.end());
  };
  for (auto index : storage::bitmap_iteration(mask)) {
    if (not index) {
      builder.skip();
      continue;
    }
    if (not secret_rows.get(*index)) {
      for (auto const& segment : segments) {
        match(
          segment,
          [&](std::string const& literal) {
            buffer.append(literal);
          },
          [&](Replacement const& replacement) {
            buffer.append(*replacement.text.get(*index));
          });
      }
      builder.data(std::string_view{buffer});
      buffer.clear();
      continue;
    }
    for (auto const& segment : segments) {
      match(
        segment,
        [&](std::string const& literal) {
          append_bytes(std::as_bytes(std::span{literal}));
        },
        [&](Replacement const& replacement) {
          if (replacement.secrets
              and replacement.secrets->present.get(*index)) {
            append_bytes(*replacement.secrets->data.get(*index));
            return;
          }
          append_bytes(std::as_bytes(std::span{*replacement.text.get(*index)}));
        });
    }
    builder.data(SecretView{secret_buffer.data(), secret_buffer.size()});
    secret_buffer.clear();
  }
  return builder.finish();
}

} // namespace tenzir::nova
