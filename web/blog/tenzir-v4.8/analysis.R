library(tidyverse)
library(scales)
library(patchwork)

# Before:
#
#     operator #1 (fluent-bit)
#       total: 42.78s
#       scheduled: 42.75s (99.91%)
#       processing: 42.69s (99.78%)
#       runs: 93763 (96.06% processing / 0.00% input / 96.06% output)
#       outbound:
#         events: 2149357 at a rate of 50237.24/s
#         bytes: 1521111771 at a rate of 35553169.67/s (estimate)
#         batches: 90065 (23.86 events/batch)
#
# After:
#
#    operator #1 (fluent-bit)
#      total: 10.41s
#      scheduled: 10.0s (96.05%)
#      processing: 9.86s (94.67%)
#      runs: 1658909 (0.00% processing / 0.00% input / 0.00% output)
#      outbound:
#        events: 1658700 at a rate of 159269.33/s
#        bytes: 3147068950 at a rate of 302183367.44/s (estimate)
#        batches: 27 (61433.33 events/batch)

data <-
  tribble(
    ~Metric, ~v4.7, ~v4.8,
    "EPS", 50237, 159269,
    "Runtime", 42.69, 9.86,
  ) |>
  pivot_longer(!Metric, names_to = "Version")


theme_set(theme_minimal(base_size = 28))

eps_plot <- data |>
  filter(Metric == "EPS") |>
  ggplot(aes(x = factor(Version), y = value, fill = Metric)) +
    geom_col(fill = "#0086E5") +
    guides(fill = "none") +
    scale_y_continuous(labels = label_number(scale_cut = cut_short_scale())) +
    labs(x = "Tenzir Version", y = "Events Per Second (EPS)")

runtime_plot <- data |>
  filter(Metric == "Runtime") |>
  ggplot(aes(x = factor(Version), y = value, fill = Metric)) +
    geom_col(fill = "#A102C8") +
    guides(fill = "none") +
    labs(x = "Tenzir Version", y = "Runtime (seconds)")

combined_plot <- eps_plot + runtime_plot

ggsave(filename = "fluent-bit-performance.svg",
       plot = combined_plot,
       bg = "transparent",
       width = 16,
       height = 9)
