library(tidyverse)
library(scales)
library(ggdark)

data <-
  tribble(
    ~Metric, ~v2.0, ~v2.1,
    "Index", 14.791, 5.721, # MiB
    "Store", 37.656, 8.491, # MiB
    "Rate", 101680, 693273, # events/second
    "Ingestion", 1650, 242  # seconds
  ) |>
  pivot_longer(!Metric, names_to = "Version")

storage.plot <- data |>
  filter(Metric == "Index" | Metric == "Store") |>
  ggplot(aes(x = factor(Version), y = value, fill = Metric)) +
    geom_bar(stat = "identity", position = "stack") +
    scale_y_continuous(label = comma) +
    labs(x = "VAST Version", y = "Database size (MiB)") +
    theme_minimal(base_size = 16)

rate.plot <- data |>
  filter(Metric == "Rate") |>
  ggplot(aes(x = factor(Version), y = value, fill = Metric)) +
    geom_bar(stat = "identity", position = "dodge") +
    scale_y_continuous(label = comma) +
    labs(x = "VAST Version", y = "Ingest rate (events/second)") +
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

dark_save("storage.png", storage.plot, width = 10, height = 7, bg = "white")
dark_save("rate.png", rate.plot, width = 10, height = 7, bg = "white")
