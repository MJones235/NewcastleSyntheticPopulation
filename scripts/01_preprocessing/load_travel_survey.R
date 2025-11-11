# Travel Survey Data Selective Loading
# Efficiently loads only essential columns from large travel survey datasets

library(data.table)  # Fast reading and processing for large files
library(dplyr)       # Data manipulation
library(tidyr)       # Data reshaping for missing data analysis

# Set up paths
project_root <- "/home/michael/NewcastlePopulation"
raw_data_dir <- file.path(project_root, "data", "raw")
processed_data_dir <- file.path(project_root, "data", "processed")

# Define file paths
trip_file <- file.path(raw_data_dir, "trip.tab")
day_file <- file.path(raw_data_dir, "day.tab")
household_file <- file.path(raw_data_dir, "household.tab")
individual_file <- file.path(raw_data_dir, "individual.tab")

cat("=== Travel Survey Data Selective Loading ===\n\n")

# Define essential columns for each file
essential_trip_cols <- c(
  # ID fields
  "TripID", "DayID", "IndividualID", "HouseholdID",
  # Essential trip characteristics
  "JourSeq", "MainMode_B03ID", "TripPurpFrom_B01ID", "TripPurpTo_B01ID", 
  "TripStartHours", "TripStartMinutes"
)

essential_day_cols <- c(
  "DayID", "IndividualID", "HouseholdID", "TravDay", "TravelYear", 
  "TravelWeekDay_B01ID", "TravelDayType_B01ID"
)

essential_household_cols <- c(
  "HouseholdID", "TWSMonth_B01ID", "TWSWeekday_B01ID", "HHoldGOR_B02ID", 
  "HHoldStruct_B02ID", "NumCarVan_B02ID", 
  "HHoldNumPeople", "HHoldNumAdults", "HHoldNumChildren"
)

essential_individual_cols <- c(
  "IndividualID", "Age_B04ID", "Sex_B01ID", "NSSec_B03ID", "HRPRelation_B01ID"
)

# Function to load selected columns efficiently
load_selected_columns <- function(file_path, selected_cols, file_name) {
  cat("Loading", file_name, "...\n")
  
  if (!file.exists(file_path)) {
    cat("  ERROR: File not found:", file_path, "\n")
    return(NULL)
  }
  
  # Get file info
  file_info <- file.info(file_path)
  file_size_gb <- round(file_info$size / (1024^3), 3)
  cat("  Original file size:", file_size_gb, "GB\n")
  
  # First, get all column names to verify our selection exists
  sample_row <- fread(file_path, nrows = 1, sep = "\t")
  available_cols <- names(sample_row)
  
  # Check which of our essential columns are actually available
  missing_cols <- setdiff(selected_cols, available_cols)
  available_essential_cols <- intersect(selected_cols, available_cols)
  
  if (length(missing_cols) > 0) {
    cat("  WARNING: Missing columns:", paste(missing_cols, collapse = ", "), "\n")
  }
  
  if (length(available_essential_cols) == 0) {
    cat("  ERROR: No essential columns found!\n")
    return(NULL)
  }
  
  cat("  Loading columns:", paste(available_essential_cols, collapse = ", "), "\n")
  
  # Load only the selected columns - much faster!
  start_time <- Sys.time()
  data <- fread(file_path, select = available_essential_cols, sep = "\t")
  end_time <- Sys.time()
  
  load_time <- round(as.numeric(end_time - start_time), 2)
  cat("  Loaded", nrow(data), "rows in", load_time, "seconds\n")
  cat("  Memory usage: ~", round(object.size(data) / (1024^2), 1), "MB\n")
  
  return(data)
}

# Load all files with selected columns
cat("Starting selective loading...\n\n")

trip_data <- load_selected_columns(trip_file, essential_trip_cols, "trip.tab")
day_data <- load_selected_columns(day_file, essential_day_cols, "day.tab")
household_data <- load_selected_columns(household_file, essential_household_cols, "household.tab")
individual_data <- load_selected_columns(individual_file, essential_individual_cols, "individual.tab")

# Data exploration and validation
cat("\n=== Data Summary ===\n")

if (!is.null(trip_data)) {
  cat("Trip data:", nrow(trip_data), "trips,", ncol(trip_data), "columns\n")
  cat("  Unique individuals:", length(unique(trip_data$IndividualID)), "\n")
  cat("  Unique households:", length(unique(trip_data$HouseholdID)), "\n")
  cat("  Date range:", min(trip_data$DayID, na.rm = T), "to", max(trip_data$DayID, na.rm = T), "\n")
}

if (!is.null(day_data)) {
  cat("Day data:", nrow(day_data), "travel days,", ncol(day_data), "columns\n")
}

if (!is.null(household_data)) {
  cat("Household data:", nrow(household_data), "households,", ncol(household_data), "columns\n")
}

if (!is.null(individual_data)) {
  cat("Individual data:", nrow(individual_data), "individuals,", ncol(individual_data), "columns\n")
}

# Check linkage between files
cat("\n=== ID Linkage Check ===\n")
if (!is.null(trip_data) && !is.null(day_data)) {
  trip_days <- unique(trip_data$DayID)
  available_days <- unique(day_data$DayID)
  missing_days <- setdiff(trip_days, available_days)
  
  cat("Trip days with missing day records:", length(missing_days), "\n")
  if (length(missing_days) > 0 && length(missing_days) <= 10) {
    cat("  Missing day IDs:", paste(head(missing_days, 10), collapse = ", "), "\n")
  }
}

# Save processed data for next steps
cat("\n=== Saving Processed Travel Data ===\n")

if (!dir.exists(processed_data_dir)) {
  dir.create(processed_data_dir, recursive = TRUE)
}

# Create comprehensive joined dataset
cat("\n=== Creating Comprehensive Joined Dataset ===\n")

if (!is.null(trip_data) && !is.null(day_data) && !is.null(household_data) && !is.null(individual_data)) {
  
  cat("Joining all travel survey tables...\n")
  
  # Start with trip data as the base
  travel_complete <- trip_data
  
  # Join with day data
  travel_complete <- travel_complete %>%
    left_join(day_data, by = c("DayID", "IndividualID", "HouseholdID"))
  
  cat("  Added day information:", nrow(travel_complete), "records\n")
  
  # Join with individual data
  travel_complete <- travel_complete %>%
    left_join(individual_data, by = "IndividualID")
  
  cat("  Added individual information:", nrow(travel_complete), "records\n")
  
  # Join with household data
  travel_complete <- travel_complete %>%
    left_join(household_data, by = "HouseholdID")
  
  cat("  Added household information:", nrow(travel_complete), "records\n")
  
  # Summary of joined dataset
  cat("\n=== Joined Dataset Summary ===\n")
  cat("Total records:", nrow(travel_complete), "\n")
  cat("Total columns:", ncol(travel_complete), "\n")
  cat("Unique trips:", length(unique(travel_complete$TripID)), "\n")
  cat("Unique individuals:", length(unique(travel_complete$IndividualID)), "\n")
  cat("Unique households:", length(unique(travel_complete$HouseholdID)), "\n")
  cat("Unique travel days:", length(unique(travel_complete$DayID)), "\n")
  
  # Check for missing data
  cat("\n=== Data Completeness Check ===\n")
  missing_summary <- travel_complete %>%
    summarise_all(~sum(is.na(.))) %>%
    pivot_longer(everything(), names_to = "Column", values_to = "Missing_Count") %>%
    arrange(desc(Missing_Count))
  
  if (any(missing_summary$Missing_Count > 0)) {
    cat("Columns with missing data:\n")
    print(missing_summary %>% filter(Missing_Count > 0))
  } else {
    cat("No missing data found in joined dataset!\n")
  }
  
  # Memory usage
  memory_mb <- round(object.size(travel_complete) / (1024^2), 1)
  cat("\nJoined dataset memory usage:", memory_mb, "MB\n")
  
  # Save comprehensive dataset
  complete_output <- file.path(processed_data_dir, "travel_survey_complete.csv")
  cat("\nSaving comprehensive travel survey dataset...\n")
  
  write.csv(travel_complete, complete_output, row.names = FALSE)
  cat("Saved complete dataset:", complete_output, "\n")
  
  # Column overview
  cat("\n=== Final Dataset Columns ===\n")
  col_overview <- data.frame(
    Column = names(travel_complete),
    Type = sapply(travel_complete, class),
    Sample_Value = sapply(travel_complete, function(x) as.character(x[1]))
  )
  print(col_overview)
  
} else {
  cat("ERROR: Cannot create joined dataset - some files failed to load\n")
}

cat("\n=== Next Steps ===\n")
cat("1. Review joined dataset column structure above\n")
cat("2. Validate data quality and completeness\n")
cat("3. Link with synthetic population demographics\n") 
cat("4. Analyze travel patterns by age, household type, etc.\n")
cat("5. Create travel behavior models for synthetic population\n")