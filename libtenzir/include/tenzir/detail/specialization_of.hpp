//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace tenzir::detail {

/// This concept is satisfied if `Instance` is a specialization
/// of `Template`. Note that this does not work if `Template`
/// has any non-type template parameters.
template <class Instance, template <class...> class Template>
concept specialization_of = requires(Instance instance) {
  {
    []<class... Args>(const Template<Args...>&) {}(instance)
  };
};

} // namespace tenzir::detail
