# Microdata Preprocessing Script
#
# Preprocesses the microdata sample for Newcastle (E12000001):
# 1. Filters by region E12000001
# 2. Keeps only required columns for mlfit
# 3. Removes households with individuals having age or sex = -8
# 4. Replaces number_of_cars = -8 with 0
# 5. Validates and corrects household size
# 6. Validates and corrects household adults/children composition

# Set up paths
script_dir <- getwd()
project_root <- script_dir
raw_data_dir <- file.path(project_root, "data", "raw")
processed_data_dir <- file.path(project_root, "data", "processed")

preprocess_microdata <- function() {
  cat("=== Microdata Preprocessing Pipeline ===\n")
  
  # Read the microdata
  input_file <- file.path(raw_data_dir, "microdata.tab")
  output_file <- file.path(processed_data_dir, "microdata.tab")
  
  cat("Reading microdata from:", input_file, "\n")
  df <- read.delim(input_file)
  cat("  Original rows:", nrow(df), "\n")

  # Load dplyr for fast grouped operations (used below)
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("The preprocessing script requires the 'dplyr' package. Please install it with install.packages('dplyr')")
  }
  library(dplyr)
  
  # 1. Filter by region E12000001 (North East)
  cat("\n1. Filtering by region E12000001...\n")
  df <- df[df$region == "E12000001", ]
  cat("  Rows after region filter:", nrow(df), "\n")
  
  # 2. Keep only required columns
  cat("\n2. Selecting required columns...\n")
  required_cols <- c(
    "resident_id_m",
    "household_id_m", 
    "region",
    "hh_adults_and_children_8m",  # household adults/children composition
    "hh_size_9a",                 # household size
    "number_of_cars_5a",          # car availability
    "resident_age_6a",            # individual age
    "sex",                        # individual sex
    "ns_sec_10a"                  # socio-economic classification
  )
  
  # Check which columns exist
  missing_cols <- required_cols[!required_cols %in% names(df)]
  if (length(missing_cols) > 0) {
    cat("  WARNING: Missing columns:", paste(missing_cols, collapse=", "), "\n")
  }
  
  available_cols <- required_cols[required_cols %in% names(df)]
  df <- df[, available_cols]
  cat("  Columns selected:", paste(names(df), collapse=", "), "\n")
  cat("  Rows after column selection:", nrow(df), "\n")
  
  # 2.5. Convert string IDs to sequential integers
  cat("\n2.5. Converting string IDs to sequential integers...\n")
  
  # Sort by household_id_m to ensure consistent ordering
  df <- df[order(df$household_id_m, df$resident_id_m), ]
  cat("  Data sorted by household_id_m and resident_id_m\n")
  
  # Replace household_id_m with sequential integers
  unique_households <- unique(df$household_id_m)
  household_mapping <- data.frame(
    original_hh_id = unique_households,
    new_hh_id = 1:length(unique_households),
    stringsAsFactors = FALSE
  )
  
  # Apply household ID mapping
  df$household_id_m <- household_mapping$new_hh_id[match(df$household_id_m, household_mapping$original_hh_id)]
  cat("  Converted", length(unique_households), "household IDs to integers 1 to", length(unique_households), "\n")
  
  # Replace resident_id_m with sequential integers within each household
  df$resident_id_m <- ave(1:nrow(df), df$household_id_m, FUN = seq_along)
  cat("  Converted resident IDs to sequential integers within each household\n")
  
  cat("  Sample household structure after conversion:\n")
  sample_hh <- df[df$household_id_m %in% 1:3, c("household_id_m", "resident_id_m")]
  print(sample_hh)
  
  # 3. Remove households where individuals have age or sex = -8
  cat("\n3. Removing households with individuals having age or sex = -8...\n")
  
  # Find households with problematic individuals
  problematic_hh <- unique(c(
    df$household_id_m[df$resident_age_6a == -8],
    df$household_id_m[df$sex == -8]
  ))
  
  cat("  Households with age = -8:", length(unique(df$household_id_m[df$resident_age_6a == -8])), "\n")
  cat("  Households with sex = -8:", length(unique(df$household_id_m[df$sex == -8])), "\n")
  cat("  Total households to remove:", length(problematic_hh), "\n")
  
  # Remove these households entirely
  df <- df[!df$household_id_m %in% problematic_hh, ]
  cat("  Rows after removing problematic households:", nrow(df), "\n")
  cat("  Households remaining:", length(unique(df$household_id_m)), "\n")
  
  # 4. Replace number_of_cars = -8 with 0
  cat("\n4. Fixing number_of_cars...\n")
  minus8_cars <- sum(df$number_of_cars_5a == -8, na.rm = TRUE)
  cat("  Individuals with number_of_cars = -8:", minus8_cars, "\n")
  df$number_of_cars_5a[df$number_of_cars_5a == -8] <- 0
  cat("  Replaced -8 with 0 for number_of_cars\n")
  
  # 5. Validate and correct household size
  cat("\n5. Validating household size...\n")
  
  # Calculate actual household sizes
  actual_sizes <- aggregate(
    resident_id_m ~ household_id_m, 
    data = df, 
    FUN = length
  )
  names(actual_sizes)[2] <- "actual_size"
  
  # Get reported household sizes (one per household)
  reported_sizes <- aggregate(
    hh_size_9a ~ household_id_m,
    data = df,
    FUN = function(x) x[1]  # Take first value (should be same for all in household)
  )
  names(reported_sizes)[2] <- "reported_size"
  
  # Merge to compare
  size_check <- merge(actual_sizes, reported_sizes, by = "household_id_m")
  
  # Cap actual size at 8 (8+ category)
  size_check$corrected_size <- pmin(size_check$actual_size, 8)
  
  mismatches <- size_check[size_check$reported_size != size_check$corrected_size, ]
  cat("  Households with size mismatches:", nrow(mismatches), "\n")
  
  if (nrow(mismatches) > 0) {
    cat("  Sample mismatches:\n")
    print(head(mismatches, 10))
    
    # Update the household sizes in the main dataframe
    for (i in 1:nrow(mismatches)) {
      hh_id <- mismatches$household_id_m[i]
      correct_size <- mismatches$corrected_size[i]
      df$hh_size_9a[df$household_id_m == hh_id] <- correct_size
    }
    cat("  Corrected", nrow(mismatches), "household sizes\n")
  }
  
  # 6. Validate and correct household adults/children composition
  cat("\n6. Validating household adults/children composition...\n")
  
  # Calculate actual adults (16+) and children per household using dplyr for speed
  # Age categories: 1 is under 16 (children), 2-6 are 16+ (adults)
  hh_stats <- df %>%
    group_by(household_id_m) %>%
    summarise(
      adults = sum(resident_age_6a >= 2, na.rm = TRUE),
      children = sum(resident_age_6a == 1, na.rm = TRUE),
      total = n(),
      reported_composition = first(hh_adults_and_children_8m),
      .groups = 'drop'
    )

  # Vectorised correct composition mapping
  hh_stats <- hh_stats %>%
    mutate(
      correct_composition = dplyr::case_when(
        adults == 1 & children == 0 ~ 1L,                              # One person
        adults == 0 | (adults == 1 & children > 0) ~ 2L,               # No adults or one adult with children
        adults == 2 & children == 0 ~ 3L,                              # Two adults: no children
        adults == 2 & children %in% c(1,2) ~ 4L,                       # Two adults: 1-2 children
        adults == 2 & children >= 3 ~ 5L,                              # Two adults: 3+ children
        adults >= 3 & children > 0 ~ 6L,                               # 3+ adults with children
        adults >= 3 & children == 0 ~ 7L,                              # 3+ adults no children
        TRUE ~ 1L
      )
    )

  composition_mismatches <- hh_stats %>% filter(reported_composition != correct_composition)
  cat("  Households with composition mismatches:", nrow(composition_mismatches), "\n")

  if (nrow(composition_mismatches) > 0) {
    cat("  Sample composition mismatches:\n")
    print(head(composition_mismatches, 10))

    # Update the compositions in the main dataframe by joining the corrected values
    df <- df %>%
      left_join(hh_stats %>% select(household_id_m, correct_composition), by = "household_id_m") %>%
      mutate(hh_adults_and_children_8m = ifelse(hh_adults_and_children_8m != correct_composition,
                                               correct_composition,
                                               hh_adults_and_children_8m)) %>%
      select(-correct_composition)

    cat("  Corrected", nrow(composition_mismatches), "household compositions\n")
  }
  
  # Remove temporary columns (none to remove now)
  
  # 7. Final summary and save
  cat("\n=== Final Summary ===\n")
  cat("Final rows:", nrow(df), "\n")
  cat("Final households:", length(unique(df$household_id_m)), "\n")
  cat("Age range:", min(df$resident_age_6a), "to", max(df$resident_age_6a), "\n")
  cat("Sex values:", paste(sort(unique(df$sex)), collapse=", "), "\n")
  cat("Household size range:", min(df$hh_size_9a), "to", max(df$hh_size_9a), "\n")
  cat("Car availability range:", min(df$number_of_cars_5a), "to", max(df$number_of_cars_5a), "\n")
  cat("Household composition range:", min(df$hh_adults_and_children_8m), "to", max(df$hh_adults_and_children_8m), "\n")
  
  # Save processed microdata
  write.table(df, output_file, sep = "\t", row.names = FALSE, quote = FALSE)
  cat("\nProcessed microdata saved to:", output_file, "\n")
  
  return(df)
}

# Run the preprocessing
processed_microdata <- preprocess_microdata()