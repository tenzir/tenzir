//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// FIXME
// #include "vast/tui/sink.hpp"

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/dom/node.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/screen.hpp>
#include <spdlog/async.h>

#include <cstdlib>

// FIXME: move into separate file
#include <vast/detail/assert.hpp>
#include <vast/logger.hpp>

#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <spdlog/sinks/base_sink.h>

#include <deque>
#include <memory>
#include <mutex>

namespace vast::tui {

// A spdlog sink for FTXUI.
template <class Mutex>
class sink : public spdlog::sinks::base_sink<Mutex> {
public:
  /// Constructs a spdlog sink that flushes its logs into a buffer owned by
  /// FTXUI.
  sink(ftxui::ScreenInteractive& screen, std::vector<ftxui::Element>& logs)
    : screen_{screen}, logs_{logs} {
  }

protected:
  // Turns logs into first-class FTXUI citizens. Formatting should take place
  // here.
  void sink_it_(const spdlog::details::log_msg& msg) override {
    spdlog::memory_buf_t formatted;
    spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
    auto line = ftxui::text(fmt::to_string(formatted));
    buffer_.push_back(std::move(line));
    flush_(); // FIXME: testing only
  }

  // Flushing is equivalent to a screen redraw, which FTXUI does when it
  // receives a custom event.
  void flush_() override {
    screen_.Post([this, lines = std::move(buffer_)]() {
      // Now we're back in the UI thread that owns the logs. So no additional
      // locking needed.
      logs_.emplace_back(ftxui::text("dummy"));
      logs_.insert(logs_.end(), std::make_move_iterator(lines.begin()),
                   std::make_move_iterator(lines.end()));
      // Trigger a screen redraw.
      screen_.PostEvent(ftxui::Event::Custom);
    });
    buffer_ = {};
  }

private:
  ftxui::ScreenInteractive& screen_;
  std::vector<ftxui::Element>& logs_;
  std::vector<ftxui::Element> buffer_;
};

using sink_mt = sink<std::mutex>;

// Super basic logger setup.
void setup_logger(ftxui::ScreenInteractive& screen,
                  std::vector<ftxui::Element>& logs) {
  std::vector<spdlog::sink_ptr> sinks;
  sinks.emplace_back(std::make_shared<sink_mt>(screen, logs));
  // Replace VAST's logger.
  auto queue_size = 1;
  auto num_threads = 1;
  spdlog::init_thread_pool(queue_size, num_threads);
  detail::logger() = std::make_shared<spdlog::async_logger>(
    "vast", sinks.begin(), sinks.end(), spdlog::thread_pool(),
    spdlog::async_overflow_policy::block);
  detail::logger()->set_level(spdlog::level::trace);
  spdlog::register_logger(detail::logger());
}

void loop() {
  using namespace ftxui;
  auto screen = ScreenInteractive::Fullscreen();
  // Bring in the logger output.
  std::vector<ftxui::Element> logs;
  logs.emplace_back(text("dummy"));
  setup_logger(screen, logs);
  VAST_INFO("testing some stuff: {}", 42);
  VAST_DEBUG("more logs!");
  VAST_DEBUG("more logs!");
  VAST_DEBUG("more logs!");
  VAST_DEBUG("more logs!");
  VAST_DEBUG("more logs!");
  auto middle = Renderer([] {
    return text("middle") | center;
  });
  auto left = Renderer([] {
    return text("Left") | center;
  });
  auto right = Renderer([] {
    return text("right") | center;
  });
  auto top = Renderer([] {
    return text("top") | center;
  });
  auto bottom = Renderer([&] {
    return vbox(logs);
  });
  int left_size = 20;
  int right_size = 20;
  int top_size = 10;
  int bottom_size = 10;
  auto container = middle;
  container = ResizableSplitLeft(left, container, &left_size);
  container = ResizableSplitRight(right, container, &right_size);
  container = ResizableSplitTop(top, container, &top_size);
  container = ResizableSplitBottom(bottom, container, &bottom_size);
  auto renderer = Renderer(container, [&] {
    return container->Render() | border;
  });
  screen.Loop(renderer);
}

} // namespace vast::tui

int main(int /* argc */, char** /* argv */) {
  vast::tui::loop();
  return EXIT_SUCCESS;
}
