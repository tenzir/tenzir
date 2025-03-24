//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/assert.hpp>

#include <caf/flow/observable.hpp>

namespace tenzir {

template <class Input>
class trampoline_base {
public:
  virtual ~trampoline_base() = default;

  virtual auto parent() const -> caf::flow::coordinator& = 0;

  virtual void request(size_t n) = 0;

  virtual void on_next(const Input& what) = 0;

  virtual void on_subscribe(caf::flow::subscription sub) = 0;

  virtual void on_complete() = 0;

  virtual void on_error(const caf::error& what) = 0;

  virtual auto disposed() const noexcept -> bool = 0;

  virtual void do_dispose(bool from_external) = 0;
};

template <class Input, class Output>
class trampoline : public trampoline_base<Input> {
public:
  virtual void activate(caf::flow::observer<Output> out) = 0;
};

template <class Input>
class trampoline_sub final : public caf::flow::observer_impl<Input>,
                             public caf::flow::subscription::impl_base {
public:
  explicit trampoline_sub(trampoline_base<Input>& trampolined)
    : trampolined_{trampolined} {
  }

  void ref_coordinated() const noexcept override {
    ref();
  }

  void deref_coordinated() const noexcept override {
    deref();
  }

  auto disposed() const noexcept -> bool override {
    return trampolined_.disposed();
  }

  void do_dispose(bool from_external) override {
    trampolined_.do_dispose(from_external);
  }

  auto parent() const noexcept -> caf::flow::coordinator* override {
    return &trampolined_.parent();
  }

  void request(size_t n) override {
    trampolined_.request(n);
  }

  void on_next(const Input& what) override {
    trampolined_.on_next(what);
  }

  void on_subscribe(caf::flow::subscription sub) override {
    trampolined_.on_subscribe(sub);
  }

  void on_complete() override {
    trampolined_.on_complete();
  }

  void on_error(const caf::error& what) override {
    trampolined_.on_error(what);
  }

private:
  trampoline_base<Input>& trampolined_;
};

template <class Input, class Output>
class trampoline_op final : public caf::detail::atomic_ref_counted,
                            public caf::flow::op::base<Output> {
public:
  trampoline_op(trampoline<Input, Output>& trampolined,
                caf::flow::observable<Input> input)
    : trampolined_{trampolined}, input_{std::move(input)} {
  }

  void ref_coordinated() const noexcept override {
    this->ref();
  }

  void deref_coordinated() const noexcept override {
    this->deref();
  }

  auto parent() const noexcept -> caf::flow::coordinator* override {
    return &trampolined_.parent();
  }

  auto subscribe(caf::flow::observer<Output> out) -> caf::disposable override {
    auto sub = caf::make_counted<trampoline_sub<Input>>(trampolined_);
    input_.subscribe(sub->as_observer());
    out.on_subscribe(caf::flow::subscription{sub});
    trampolined_.activate(std::move(out));
    return sub->as_disposable();
  }

private:
  trampoline<Input, Output>& trampolined_;
  caf::flow::observable<Input> input_;
};

} // namespace tenzir
