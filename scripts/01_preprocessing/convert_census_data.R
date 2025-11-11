# Census Data Preprocessing Script
# 
# Converts and cleans census constraint files:
# 1. Converts hh_adults_and_children_11a.csv to 8-category format
# 2. Removes invalid categories with 0 observations from other files
# 3. Outputs cleaned files to data/processed folder

library(tools) # for file_path_sans_ext

# Set up paths
script_dir <- getwd()  # Current working directory
project_root <- script_dir  # Assume we're running from project root
raw_data_dir <- file.path(project_root, "data", "raw")
processed_data_dir <- file.path(project_root, "data", "processed")

convert_household_11_to_8_categories <- function(input_file, output_file) {
  cat("Converting", basename(input_file), "to 8-category format...\n")
  
  # Read the input file
  df <- read.csv(input_file)
  cat("  Original rows:", nrow(df), "\n")
  
  # Check for -8 values and their observations
  code_col <- names(df)[3]  # 3rd column
  obs_col <- names(df)[5]   # 5th column (observations)
  
  minus8_rows <- df[df[[code_col]] == -8, ]
  if (nrow(minus8_rows) > 0) {
    cat("  Found", nrow(minus8_rows), "rows with -8 codes\n")
    cat("  -8 observations:", paste(unique(minus8_rows[[obs_col]]), collapse=", "), "\n")
    if (!all(minus8_rows[[obs_col]] == 0)) {
      cat("  WARNING: Some -8 codes have non-zero observations!\n")
    } else {
      cat("  All -8 codes have 0 observations - removing them\n")
    }
  }
  
  # Remove -8 rows
  df <- df[df[[code_col]] != -8, ]
  
  # Define the mapping from 11 categories to 8 categories
  map_category <- function(code) {
    mapping <- c(
      "1" = 1.0,   # One person household
      "2" = 1.0,   # One person household  
      "3" = 2.0,   # No adults, or one adult and one or more children
      "4" = 3.0,   # Two adults: No children
      "5" = 3.0,   # Two adults: No children
      "6" = 3.0,   # Two adults: No children
      "7" = 4.0,   # Two adults: One or two children
      "8" = 5.0,   # Two adults: Three or more children
      "9" = 6.0,   # Three or more adults: One or more children
      "10" = 7.0   # Three or more adults: No children
    )
    return(mapping[as.character(code)])
  }
  
  # Define the new labels
  get_new_label <- function(code) {
    labels <- c(
      "1" = "One Person Household",
      "2" = "No adults, or one adult and one or more children", 
      "3" = "Two adults: No children",
      "4" = "Two adults: One or two children",
      "5" = "Two adults: Three or more children",
      "6" = "Three or more adults: One or more children",
      "7" = "Three or more adults: No children"
    )
    return(labels[as.character(code)])
  }
  
  # Apply the mapping
  df$New_Code <- map_category(df[[code_col]])
  
  # Group by output area and new code, summing observations
  grouped <- aggregate(
    df[[obs_col]], 
    by = list(
      Output_Areas_Code = df[[1]], 
      Output_Areas = df[[2]], 
      New_Code = df$New_Code
    ), 
    FUN = sum
  )
  names(grouped)[4] <- "Observation"
  
  # Add new labels
  grouped$New_Label <- get_new_label(grouped$New_Code)
  
  # Create output dataframe with proper column names
  output_df <- data.frame(
    "Output Areas Code" = grouped$Output_Areas_Code,
    "Output Areas" = grouped$Output_Areas,
    "Adults and children in household (8 categories) Code" = grouped$New_Code,
    "Adults and children in household (8 categories)" = grouped$New_Label,
    "Observation" = grouped$Observation,
    check.names = FALSE
  )
  
  # Sort by output area and category
  output_df <- output_df[order(output_df[["Output Areas Code"]], 
                               output_df[["Adults and children in household (8 categories) Code"]]), ]
  
  # Save the result
  write.csv(output_df, output_file, row.names = FALSE)
  
  cat("  Output rows:", nrow(output_df), "\n")
  cat("  Saved to:", output_file, "\n")
  
  return(output_df)
}

remove_invalid_categories <- function(input_file, output_file, invalid_values, file_description) {
  cat("Processing", file_description, ":", basename(input_file), "\n")
  
  # Read the input file
  df <- read.csv(input_file)
  cat("  Original rows:", nrow(df), "\n")
  
  # Check for invalid values  
  code_col <- names(df)[3]  # 3rd column contains codes
  obs_col <- names(df)[5]   # 5th column contains observations
  
  total_removed <- 0
  for (invalid_val in invalid_values) {
    invalid_rows <- df[df[[code_col]] == invalid_val, ]
    if (nrow(invalid_rows) > 0) {
      cat("  Found", nrow(invalid_rows), "rows with value", invalid_val, "\n")
      cat("    Observations for", invalid_val, ":", paste(unique(invalid_rows[[obs_col]]), collapse=", "), "\n")
      
      if (!all(invalid_rows[[obs_col]] == 0)) {
        cat("    ERROR: Some", invalid_val, "codes have non-zero observations!\n")
        non_zero <- invalid_rows[invalid_rows[[obs_col]] != 0, ]
        cat("    Non-zero rows:", nrow(non_zero), "\n")
        print(non_zero)
        stop(paste("Cannot remove", invalid_val, "values - they have non-zero observations!"))
      } else {
        cat("    All", invalid_val, "codes have 0 observations - safe to remove\n")
        total_removed <- total_removed + nrow(invalid_rows)
      }
    } else {
      cat("  No", invalid_val, "values found\n")
    }
  }
  
  # Remove invalid values
  for (invalid_val in invalid_values) {
    df <- df[df[[code_col]] != invalid_val, ]
  }
  
  cat("  Rows after cleaning:", nrow(df), "(removed", total_removed, ")\n")
  
  # Save the result
  write.csv(df, output_file, row.names = FALSE)
  cat("  Saved to:", output_file, "\n")
  
  return(df)
}

copy_file_if_no_changes_needed <- function(input_file, output_file, file_description) {
  cat("Copying", file_description, ":", basename(input_file), "\n")
  df <- read.csv(input_file)
  write.csv(df, output_file, row.names = FALSE)
  cat("  Copied", nrow(df), "rows to:", output_file, "\n")
  return(df)
}

main <- function() {
  cat("=== Census Data Preprocessing Pipeline ===\n")
  cat("Reading from:", raw_data_dir, "\n")
  cat("Writing to:", processed_data_dir, "\n\n")
  
  # Ensure output directory exists
  if (!dir.exists(processed_data_dir)) {
    dir.create(processed_data_dir, recursive = TRUE)
  }
  
  tryCatch({
    # 1. Convert household adults/children from 11 to 8 categories
    hh_11_input <- file.path(raw_data_dir, "hh_adults_and_children_11a.csv")
    hh_8_output <- file.path(processed_data_dir, "hh_adults_and_children_8m.csv")
    
    if (file.exists(hh_11_input)) {
      convert_household_11_to_8_categories(hh_11_input, hh_8_output)
    } else {
      cat("WARNING:", hh_11_input, "not found!\n")
    }
    cat("\n")
    
    # 2. Process household size (remove value 0)
    hh_size_input <- file.path(raw_data_dir, "hh_size_9a.csv")
    hh_size_output <- file.path(processed_data_dir, "hh_size_9a.csv")
    
    if (file.exists(hh_size_input)) {
      remove_invalid_categories(hh_size_input, hh_size_output, c(0), "household size")
    } else {
      cat("WARNING:", hh_size_input, "not found!\n")
    }
    cat("\n")
    
    # 3. Process number of cars (remove value -8)
    cars_input <- file.path(raw_data_dir, "number_of_cars_5a.csv")
    cars_output <- file.path(processed_data_dir, "number_of_cars_5a.csv")
    
    if (file.exists(cars_input)) {
      remove_invalid_categories(cars_input, cars_output, c(-8), "number of cars")
    } else {
      cat("WARNING:", cars_input, "not found!\n")
    }
    cat("\n")
    
    # 4. Copy files that don't need preprocessing
    files_to_copy <- list(
      c("resident_age_6a.csv", "resident age"),
      c("sex.csv", "sex"),
      c("ns_sec_10a.csv", "NS-SeC")
    )
    
    for (file_info in files_to_copy) {
      filename <- file_info[1]
      description <- file_info[2]
      input_file <- file.path(raw_data_dir, filename)
      output_file <- file.path(processed_data_dir, filename)
      
      if (file.exists(input_file)) {
        copy_file_if_no_changes_needed(input_file, output_file, description)
      } else {
        cat("WARNING:", input_file, "not found!\n")
      }
      cat("\n")
    }
    
    # 5. Summary
    cat("=== Processing Complete ===\n")
    cat("Files created in data/processed/:\n")
    
    csv_files <- list.files(processed_data_dir, pattern = "\\.csv$", full.names = TRUE)
    file_summary <- list()
    
    for (file in csv_files) {
      df <- read.csv(file)
      file_info <- list(
        filename = basename(file),
        rows = nrow(df),
        columns = ncol(df),
        size_mb = round(file.size(file) / (1024^2), 3)
      )
      file_summary[[basename(file)]] <- file_info
      cat(" ", basename(file), ":", nrow(df), "rows\n")
    }
    
    # Save processing record
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    output_record_dir <- file.path(project_root, "data", "outputs", "01_preprocessing")
    if (!dir.exists(output_record_dir)) {
      dir.create(output_record_dir, recursive = TRUE)
    }
    
    # Create processing metadata
    processing_record <- list(
      processing_timestamp = timestamp,
      script_name = "convert_census_data.R", 
      input_directory = raw_data_dir,
      output_directory = processed_data_dir,
      files_processed = file_summary,
      total_files = length(file_summary)
    )
    
    # Save as simple YAML-style text
    record_file <- file.path(output_record_dir, paste0("preprocessing_record_", timestamp, ".yml"))
    cat("# Census Data Preprocessing Record\n", file = record_file)
    cat("# Generated:", format(Sys.time()), "\n\n", file = record_file, append = TRUE)
    
    cat("processing_timestamp:", timestamp, "\n", file = record_file, append = TRUE)
    cat("script_name: convert_census_data.R\n", file = record_file, append = TRUE)
    cat("input_directory:", raw_data_dir, "\n", file = record_file, append = TRUE) 
    cat("output_directory:", processed_data_dir, "\n", file = record_file, append = TRUE)
    cat("total_files:", length(file_summary), "\n", file = record_file, append = TRUE)
    cat("files_processed:\n", file = record_file, append = TRUE)
    
    for (filename in names(file_summary)) {
      info <- file_summary[[filename]]
      cat("  ", filename, ":\n", file = record_file, append = TRUE)
      cat("    rows:", info$rows, "\n", file = record_file, append = TRUE)
      cat("    columns:", info$columns, "\n", file = record_file, append = TRUE)
      cat("    size_mb:", info$size_mb, "\n", file = record_file, append = TRUE)
    }
    
    cat("\nProcessing record saved to:", record_file, "\n")
    cat("All constraint files are now ready for mlfit!\n")
    
  }, error = function(e) {
    cat("ERROR:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# Run the main function
main()