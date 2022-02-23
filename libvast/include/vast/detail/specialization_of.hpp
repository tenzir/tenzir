//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::detail {

/// This concept is satisfied if `Instance` is a specialization
/// of `Template`. Note that this does not work if `Template`
/// has any non-type template parameters.
template <class Instance, template <class...> class Template>
concept specialization_of = requires(Instance instance) {
  {[]<class... Args>(const Template<Args...>&){}(instance)};
};

} // namespace vast::detail
