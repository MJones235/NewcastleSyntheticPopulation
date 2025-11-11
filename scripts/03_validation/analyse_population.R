# Newcastle Synthetic Population Validation and Analysis
# Compares target controls vs synthetic population distributions

library(ggplot2)
library(dplyr)
library(tidyr)
library(gridExtra)

# Set up paths
project_root <- "/home/michael/NewcastlePopulation"
generation_dir <- file.path(project_root, "data", "outputs", "02_generation")
processed_dir <- file.path(project_root, "data", "processed")
plot_dir <- file.path(project_root, "data", "outputs", "03_validation")

# Create plot directory with timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
plot_dir_timestamped <- file.path(plot_dir, paste0("validation_plots_", timestamp))
if (!dir.exists(plot_dir_timestamped)) {
  dir.create(plot_dir_timestamped, recursive = TRUE)
  cat("Created plot directory:", plot_dir_timestamped, "\n")
}

cat("=== Newcastle Synthetic Population Analysis ===\n")

# Find the most recent synthetic population file
population_files <- list.files(generation_dir, pattern = "synthetic_population_.*\\.csv", full.names = TRUE)
if (length(population_files) == 0) {
  stop("No synthetic population files found in ", generation_dir)
}

# Use the most recent file (assumes timestamp ordering)
latest_population_file <- population_files[length(population_files)]
cat("Using synthetic population file:", basename(latest_population_file), "\n")

# Load consolidated synthetic population
synthetic_pop_full <- read.csv(latest_population_file)
cat("Loaded synthetic population with", nrow(synthetic_pop_full), "individuals\n")
cat("Unique households:", length(unique(synthetic_pop_full$household_id_m)), "\n")
cat("Output areas:", length(unique(synthetic_pop_full$OA)), "\n")

# Create unique household IDs across all OAs
synthetic_pop_full$household_id_unique <- paste0(synthetic_pop_full$OA, "_", synthetic_pop_full$household_id_m)

# Load control data from processed directory
sex_ctrl <- read.csv(file.path(processed_dir, "sex.csv"))
hh_adults_children_ctrl <- read.csv(file.path(processed_dir, "hh_adults_and_children_8m.csv"))
hh_size_ctrl <- read.csv(file.path(processed_dir, "hh_size_9a.csv"))
ns_sec_ctrl <- read.csv(file.path(processed_dir, "ns_sec_10a.csv"))
number_of_cars_ctrl <- read.csv(file.path(processed_dir, "number_of_cars_5a.csv"))
resident_age_ctrl <- read.csv(file.path(processed_dir, "resident_age_6a.csv"))

# Standardize control data column names to match mlfit format - keep only needed columns
sex_ctrl <- sex_ctrl[, c(1, 3, 5)]
names(sex_ctrl) <- c("OA", "sex", "N")

hh_adults_children_ctrl <- hh_adults_children_ctrl[, c(1, 3, 5)]
names(hh_adults_children_ctrl) <- c("OA", "hh_adults_and_children_8m", "N")

hh_size_ctrl <- hh_size_ctrl[, c(1, 3, 5)]
names(hh_size_ctrl) <- c("OA", "hh_size_9a", "N") 

ns_sec_ctrl <- ns_sec_ctrl[, c(1, 3, 5)]
names(ns_sec_ctrl) <- c("OA", "ns_sec_10a", "N")

number_of_cars_ctrl <- number_of_cars_ctrl[, c(1, 3, 5)]
names(number_of_cars_ctrl) <- c("OA", "number_of_cars_5a", "N")

resident_age_ctrl <- resident_age_ctrl[, c(1, 3, 5)]
names(resident_age_ctrl) <- c("OA", "resident_age_6a", "N")

cat("Loaded all control data from processed directory\n")

# Function to create comparison plots
create_comparison_plot <- function(target_data, synthetic_data, var_name, var_col, title, filename) {
  
  # Aggregate target totals
  target_summary <- target_data %>%
    group_by(!!sym(var_col)) %>%
    summarise(target_count = sum(N), .groups = 'drop') %>%
    mutate(source = "Target")
  
  # Aggregate synthetic totals
  synthetic_summary <- synthetic_data %>%
    count(!!sym(var_col), name = "synthetic_count") %>%
    mutate(source = "Synthetic")
  
  # Combine data
  comparison_data <- target_summary %>%
    full_join(synthetic_summary, by = var_col) %>%
    replace_na(list(target_count = 0, synthetic_count = 0)) %>%
    pivot_longer(cols = c(target_count, synthetic_count), 
                 names_to = "type", values_to = "count") %>%
    mutate(type = case_when(
      type == "target_count" ~ "Target",
      type == "synthetic_count" ~ "Synthetic"
    ))
  
  # Create plot
  p <- ggplot(comparison_data, aes(x = factor(!!sym(var_col)), y = count, fill = type)) +
    geom_col(position = "dodge", alpha = 0.8) +
    scale_fill_manual(values = c("Target" = "steelblue", "Synthetic" = "orange")) +
    labs(title = title,
         x = var_name,
         y = "Count",
         fill = "Source") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5))
  
  # Save plot
  ggsave(filename, p, width = 10, height = 6, dpi = 300)
  cat("Saved plot:", filename, "\n")
  
  # Return summary statistics
  comparison_summary <- target_summary %>%
    full_join(synthetic_summary, by = var_col) %>%
    replace_na(list(target_count = 0, synthetic_count = 0)) %>%
    mutate(difference = synthetic_count - target_count,
           pct_difference = ifelse(target_count > 0, 
                                   (difference / target_count) * 100, 
                                   NA))
  
  return(list(plot = p, summary = comparison_summary))
}

cat("\n=== Creating Comparison Plots ===\n")

# 1. Sex distribution
sex_analysis <- create_comparison_plot(
  sex_ctrl, synthetic_pop_full, "Sex", "sex",
  "Sex Distribution: Target vs Synthetic",
  file.path(plot_dir_timestamped, "sex_comparison.png")
)

# 2. Resident Age distribution  
age_analysis <- create_comparison_plot(
  resident_age_ctrl, synthetic_pop_full, "Age Group", "resident_age_6a",
  "Age Distribution: Target vs Synthetic", 
  file.path(plot_dir_timestamped, "age_comparison.png")
)

# 3. NS-SeC distribution
nssec_analysis <- create_comparison_plot(
  ns_sec_ctrl, synthetic_pop_full, "NS-SeC Category", "ns_sec_10a",
  "Socio-Economic Classification: Target vs Synthetic",
  file.path(plot_dir_timestamped, "nssec_comparison.png")
)

# For household-level variables, we need to aggregate by household
household_data <- synthetic_pop_full %>%
  group_by(household_id_unique) %>%  # Use the unique household ID
  slice_head(n = 1) %>%  # Take one row per household
  ungroup()

cat("Household-level analysis based on", nrow(household_data), "unique households\n")

# Function to create household-level comparison plots (counts households, not individuals)
create_household_comparison_plot <- function(target_data, household_data, var_name, var_col, title, filename) {
  
  # Aggregate target totals (already in household counts)
  target_summary <- target_data %>%
    group_by(!!sym(var_col)) %>%
    summarise(target_count = sum(N), .groups = 'drop') %>%
    mutate(source = "Target")
  
  # Aggregate synthetic household totals (count households, not individuals)
  synthetic_summary <- household_data %>%
    count(!!sym(var_col), name = "synthetic_count") %>%
    mutate(source = "Synthetic")
  
  # Combine data
  comparison_data <- target_summary %>%
    full_join(synthetic_summary, by = var_col) %>%
    replace_na(list(target_count = 0, synthetic_count = 0)) %>%
    pivot_longer(cols = c(target_count, synthetic_count), 
                 names_to = "type", values_to = "count") %>%
    mutate(type = case_when(
      type == "target_count" ~ "Target",
      type == "synthetic_count" ~ "Synthetic"
    ))
  
  # Create plot
  p <- ggplot(comparison_data, aes(x = factor(!!sym(var_col)), y = count, fill = type)) +
    geom_col(position = "dodge", alpha = 0.8) +
    scale_fill_manual(values = c("Target" = "steelblue", "Synthetic" = "orange")) +
    labs(title = title,
         x = var_name,
         y = "Number of Households",
         fill = "Source") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5))
  
  # Save plot
  ggsave(filename, p, width = 10, height = 6, dpi = 300)
  cat("Saved plot:", filename, "\n")
  
  # Return summary statistics
  comparison_summary <- target_summary %>%
    full_join(synthetic_summary, by = var_col) %>%
    replace_na(list(target_count = 0, synthetic_count = 0)) %>%
    mutate(difference = synthetic_count - target_count,
           pct_difference = ifelse(target_count > 0, 
                                   (difference / target_count) * 100, 
                                   NA))
  
  return(list(plot = p, summary = comparison_summary))
}

# 4. Household size distribution (household counts)
hh_size_analysis <- create_household_comparison_plot(
  hh_size_ctrl, household_data, "Household Size", "hh_size_9a",
  "Household Size Distribution: Target vs Synthetic (Household Counts)",
  file.path(plot_dir_timestamped, "household_size_comparison.png")
)

# 5. Household adults/children composition (household counts)
hh_comp_analysis <- create_household_comparison_plot(
  hh_adults_children_ctrl, household_data, "Household Composition", "hh_adults_and_children_8m",
  "Household Composition: Target vs Synthetic (Household Counts)",
  file.path(plot_dir_timestamped, "household_composition_comparison.png")
)

# 6. Number of cars distribution (household counts)
cars_analysis <- create_household_comparison_plot(
  number_of_cars_ctrl, household_data, "Number of Cars", "number_of_cars_5a", 
  "Car Availability: Target vs Synthetic (Household Counts)",
  file.path(plot_dir_timestamped, "cars_comparison.png")
)

cat("\n=== Summary Statistics ===\n")

# Print summary statistics for each variable
variables <- list(
  "Sex" = sex_analysis$summary,
  "Age" = age_analysis$summary,
  "NS-SeC" = nssec_analysis$summary,
  "Household Size" = hh_size_analysis$summary,
  "Household Composition" = hh_comp_analysis$summary,
  "Car Availability" = cars_analysis$summary
)

for (var_name in names(variables)) {
  cat("\n", var_name, ":\n")
  summary_data <- variables[[var_name]]
  
  total_target <- sum(summary_data$target_count, na.rm = TRUE)
  total_synthetic <- sum(summary_data$synthetic_count, na.rm = TRUE)
  total_diff <- total_synthetic - total_target
  
  cat("  Total Target:", total_target, "\n")
  cat("  Total Synthetic:", total_synthetic, "\n") 
  cat("  Total Difference:", total_diff, "\n")
  cat("  Overall % Difference:", round((total_diff/total_target)*100, 2), "%\n")
  
  # Show categories with largest differences
  biggest_diffs <- summary_data %>%
    arrange(desc(abs(difference))) %>%
    head(3)
  
  if (nrow(biggest_diffs) > 0) {
    cat("  Largest differences by category:\n")
    for (i in 1:nrow(biggest_diffs)) {
      cat(sprintf("    Category %s: %+d (%.1f%%)\n", 
                  biggest_diffs[i,1], 
                  biggest_diffs$difference[i],
                  biggest_diffs$pct_difference[i]))
    }
  }
}

cat("\n=== Analysis Complete ===\n")
cat("All plots saved to:", plot_dir_timestamped, "\n")