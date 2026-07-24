//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/location.hpp"
#include "tenzir/plugin/base.hpp"

#include <string>
#include <string_view>
#include <utility>

namespace tenzir {

// -- operator plugin ----------------------------------------------------------

using operator_serialization_plugin = serialization_plugin<operator_base>;

template <class Operator>
using operator_inspection_plugin = inspection_plugin<operator_base, Operator>;

/// This plugin registers the (de)serialization for an operator type. Derive
/// from it to make an operator's state inspectable and serializable.
template <class Operator>
class operator_plugin : public virtual operator_inspection_plugin<Operator> {};

/// Builds a `where` legacy operator from an already normalized and validated
/// expression.
class where_factory_plugin : public virtual plugin {
public:
  virtual auto make_where_operator(located<expression> expr) const
    -> operator_ptr
    = 0;
};

} // namespace tenzir
