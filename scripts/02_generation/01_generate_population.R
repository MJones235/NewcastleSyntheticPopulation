# Newcastle Synthetic Population Generation using mlfit
# This script applies the mlfit package to real Newcastle census microdata

# Load required libraries
library(mlfit)
library(magrittr)  # for %>% pipe operator

# Load Newcastle microdata as reference sample
ref_sample <- read.table("data/processed/microdata.tab", header = TRUE, sep = "\t")

# Load sex controls and check format
sex_ctrl_raw <- read.csv("data/processed/sex.csv")

# Transform to mlfit format: OA, sex, N (matching microdata column names)
sex_ctrl <- data.frame(
  OA = sex_ctrl_raw$Output.Areas.Code,
  sex = sex_ctrl_raw$Sex..2.categories..Code,
  N = sex_ctrl_raw$Observation
)

# Load household adults and children controls and check format
hh_adults_children_ctrl_raw <- read.csv("data/processed/hh_adults_and_children_8m.csv")

# Transform to mlfit format: OA, hh_adults_and_children_8m, N (matching microdata column names)
hh_adults_children_ctrl <- data.frame(
  OA = hh_adults_children_ctrl_raw$Output.Areas.Code,
  hh_adults_and_children_8m = hh_adults_children_ctrl_raw$Adults.and.children.in.household..8.categories..Code,
  N = hh_adults_children_ctrl_raw$Observation
)

# Load household size controls and check format
hh_size_ctrl_raw <- read.csv("data/processed/hh_size_9a.csv")

# Transform to mlfit format: OA, hh_size_9a, N (matching microdata column names)
hh_size_ctrl <- data.frame(
  OA = hh_size_ctrl_raw$Output.Areas.Code,
  hh_size_9a = hh_size_ctrl_raw$Household.size..9.categories..Code,
  N = hh_size_ctrl_raw$Observation
)

# Load NS-SeC controls and check format
ns_sec_ctrl_raw <- read.csv("data/processed/ns_sec_10a.csv")

# Transform to mlfit format: OA, ns_sec_10a, N (matching microdata column names)
ns_sec_ctrl <- data.frame(
  OA = ns_sec_ctrl_raw$Output.Areas.Code,
  ns_sec_10a = ns_sec_ctrl_raw$National.Statistics.Socio.economic.Classification..NS.SeC...10.categories..Code,
  N = ns_sec_ctrl_raw$Observation
)

# Load number of cars controls and check format
number_of_cars_ctrl_raw <- read.csv("data/processed/number_of_cars_5a.csv")

# Transform to mlfit format: OA, number_of_cars_5a, N (matching microdata column names)
number_of_cars_ctrl <- data.frame(
  OA = number_of_cars_ctrl_raw$Output.Areas.Code,
  number_of_cars_5a = number_of_cars_ctrl_raw$Car.or.van.availability..5.categories..Code,
  N = number_of_cars_ctrl_raw$Observation
)

# Load resident age controls and check format
resident_age_ctrl_raw <- read.csv("data/processed/resident_age_6a.csv")

# Transform to mlfit format: OA, resident_age_6a, N (matching microdata column names)
resident_age_ctrl <- data.frame(
  OA = resident_age_ctrl_raw$Output.Areas.Code,
  resident_age_6a = resident_age_ctrl_raw$Age..6.categories..Code,
  N = resident_age_ctrl_raw$Observation
)

# Create geographic hierarchy
# Get all unique OAs from the controls (they should all be the same)
unique_OAs <- unique(sex_ctrl$OA)
cat("Number of unique Output Areas:", length(unique_OAs), "\n")

# Create geo_hierarchy mapping all OAs to the single region E12000001
geo_hierarchy <- data.frame(
  region = "E12000001",
  OA = unique_OAs
)
cat("Geographic hierarchy created with", nrow(geo_hierarchy), "OAs mapped to region E12000001\n")

# Create fitting problems
fitting_problems <- ml_problem(
  ref_sample = ref_sample,
  field_names = special_field_names(
    groupId = "household_id_m", 
    individualId = "resident_id_m", 
    count = "N",
    zone = "OA", 
    region = "region"
  ),
  group_controls = list(hh_adults_children_ctrl, hh_size_ctrl, number_of_cars_ctrl),
  individual_controls = list(resident_age_ctrl, sex_ctrl, ns_sec_ctrl),
  geo_hierarchy = geo_hierarchy
)

cat("Created fitting problems for", length(fitting_problems), "zones\n")

# Fit using IPU algorithm and replicate using TRS algorithm
fits <- fitting_problems %>%
  lapply(ml_fit, algorithm = "ipu", verbose = TRUE, maxiter=5000) %>%
  lapply(ml_replicate, algorithm = "trs")

cat("Fitting completed successfully!\n")
cat("Number of zones fitted:", length(fits), "\n")

# Save outputs
cat("\nSaving consolidated synthetic population...\n")

# Load configuration (optional - fallback to defaults if config not available)
project_root <- "/home/michael/NewcastlePopulation"
config_file <- file.path(project_root, "config", "pipeline_config.yml")

# Create output directory with new structure
output_dir <- file.path(project_root, "data", "outputs", "02_generation")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  cat("Created output directory:", output_dir, "\n")
}

# Create timestamp for this run
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
cat("Generation timestamp:", timestamp, "\n")

# Consolidate all synthetic populations into a single CSV
cat("Consolidating synthetic populations from", length(fits), "zones...\n")

oa_names <- names(fitting_problems)
consolidated_population <- data.frame()

for (i in seq_along(fits)) {
  oa_name <- oa_names[i]
  
  # Extract the synthetic population for this zone
  synthetic_pop <- fits[[i]]
  
  # Add OA column
  synthetic_pop$OA <- oa_name
  
  # Combine with consolidated dataset
  consolidated_population <- rbind(consolidated_population, synthetic_pop)
  
  cat("  Added", nrow(synthetic_pop), "individuals from", oa_name, "\n")
}

# Reorder columns to put OA first
col_order <- c("OA", setdiff(names(consolidated_population), "OA"))
consolidated_population <- consolidated_population[, col_order]

# Save consolidated population
population_file <- file.path(output_dir, paste0("synthetic_population_", timestamp, ".csv"))
write.csv(consolidated_population, population_file, row.names = FALSE)

cat("\nConsolidated synthetic population saved:")
cat("\n  File:", population_file)
cat("\n  Total individuals:", nrow(consolidated_population))
cat("\n  Unique households:", length(unique(consolidated_population$household_id_m)))
cat("\n  Output areas:", length(unique(consolidated_population$OA)))

# Save metadata about this run
metadata <- list(
  generation_timestamp = timestamp,
  script_version = "newcastle_mlfit.R",
  total_individuals = nrow(consolidated_population),
  unique_households = length(unique(consolidated_population$household_id_m)),
  output_areas = length(unique(consolidated_population$OA)),
  oa_list = unique(consolidated_population$OA),
  mlfit_settings = list(
    fit_algorithm = "ipu",
    replicate_algorithm = "trs",
    max_iterations = 5000
  ),
  input_files = list(
    reference_sample = "data/processed/microdata.tab",
    control_files = c(
      "data/processed/sex.csv",
      "data/processed/hh_adults_and_children_8m.csv", 
      "data/processed/hh_size_9a.csv",
      "data/processed/ns_sec_10a.csv",
      "data/processed/number_of_cars_5a.csv",
      "data/processed/resident_age_6a.csv"
    )
  )
)

# Save metadata as YAML-style text (simple format)
metadata_file <- file.path(output_dir, paste0("generation_metadata_", timestamp, ".yml"))
cat("# Newcastle Synthetic Population Generation Metadata\n", file = metadata_file)
cat("# Generated:", format(Sys.time()), "\n\n", file = metadata_file, append = TRUE)

for (name in names(metadata)) {
  if (is.list(metadata[[name]])) {
    cat(name, ":\n", file = metadata_file, append = TRUE)
    for (subname in names(metadata[[name]])) {
      if (is.vector(metadata[[name]][[subname]]) && length(metadata[[name]][[subname]]) > 1) {
        cat("  ", subname, ":\n", file = metadata_file, append = TRUE)
        for (item in metadata[[name]][[subname]]) {
          cat("    - ", item, "\n", file = metadata_file, append = TRUE)
        }
      } else {
        cat("  ", subname, ":", metadata[[name]][[subname]], "\n", file = metadata_file, append = TRUE)
      }
    }
  } else {
    cat(name, ":", metadata[[name]], "\n", file = metadata_file, append = TRUE)
  }
}

cat("\nMetadata saved to:", metadata_file, "\n")
cat("\n=== GENERATION COMPLETE ===\n")
cat("Next step: Run validation script with:", basename(population_file), "\n")

