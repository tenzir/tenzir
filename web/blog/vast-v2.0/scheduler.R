library(dplyr)
library(ggplot2)
library(scales)
library(ggdark)

plot <- read.csv("scheduler.csv") |>
  rename(Scheduler = scheduler) |>
  filter(num_queries > 1) |>
  ggplot(aes(x = factor(num_queries, labels = unique(comma(num_queries))),
             y = duration, fill = Scheduler, color = Scheduler)) +
    geom_bar(stat = "identity", position = "dodge") +
    scale_y_continuous(label = comma) +
    labs(x = "Number of Queries", y = "Aggregate Query Latency (seconds)") +
    theme_minimal(base_size = 16)

# Wraps ggsave to produce both light and dark themed plot using ggdark.
dark_save <- function(filename, plot, sep = "-", ...) {
  require(tools)
  require(ggdark)
  base <- file_path_sans_ext(filename)
  ext <- file_ext(filename)
  base_light <- paste(base, "light", sep = "-")
  base_dark <- paste(base, "dark", sep = "-")
  filename_light <- paste(base_light, ext, sep = ".")
  filename_dark <- paste(base_dark, ext, sep = ".")
  ggsave(filename = filename_light, plot, ...)
  ggsave(filename = filename_dark, plot + dark_mode(plot$theme), ...)
}

dark_save("scheduler.png", plot, width = 10, height = 7, bg = "white")
