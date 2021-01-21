/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <array>
#include <cstdint>
#include <iostream>

namespace vast::format::bench {

/// A collapsable benchmark mixin.
/// Defines all methods empty.
struct noop_benchmark_mixin {
    /// An iteration tracker that has noop implementation.
    struct iteration_tracker{
        constexpr void next_step() const noexcept {}
    };

    constexpr iteration_tracker make_iteration_tracker() const noexcept {
        return {};
    }
};

/// A real measuring  benchmark mixin.
/// Coniders the number of steps is equal to N.
template <int N>
class cycleclock_benchmark_mixin {
public:
    ~cycleclock_benchmark_mixin() {
        std::cout << "cycleclock benchmark results\n";
        for( auto i = 0; i < N; ++i) {
            std::cout << "step_" << i << ": "
                      <<  events_per_step[i] << " events in "
                      <<  events_durations[i] << " cycles";

            if(events_per_step[i] > 0) {
                const auto cycles_on_event =
                    static_cast<double>(events_durations[i])/events_per_step[i];
                std::cout << " => " << cycles_on_event << " cycles/event";
            }
            std::cout << "\n";
        }
        std::cout << std::endl;
    }

    friend class iteration_tracker;

    /// An iteration tracker that has noop implementation.
    class iteration_tracker {
        static std::int64_t rdtsc() noexcept {
            uint64_t low, high;
            __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
            return (high << 32) | low;
        }

      public:
        explicit iteration_tracker( cycleclock_benchmark_mixin & totals) noexcept
            : totals_{totals}{}

        ~iteration_tracker() noexcept {
            for( auto i = 0; i < current_step_; ++i) {
                totals_.events_per_step[i] += 1;
                totals_.events_durations[i] += durations_[i];
            }
        }

        constexpr void next_step() noexcept {
            durations_[current_step_++]= rdtsc() - current_step_started_at_;
            current_step_started_at_ = rdtsc();
        }

        std::array<std::uint64_t, N> durations_;
        int current_step_ = 0;
        std::int64_t current_step_started_at_ = rdtsc();

        cycleclock_benchmark_mixin & totals_;
    };

    constexpr iteration_tracker make_iteration_tracker() noexcept {
        return iteration_tracker{ *this };
    }

private:
    std::array<std::size_t, N> events_per_step{};
    std::array<std::uint64_t, N> events_durations{};
};

} // namespace vast::format::bench
