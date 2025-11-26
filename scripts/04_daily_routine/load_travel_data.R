# Load Travel Survey Data
# This script safely loads the large travel_survey_complete.csv dataset

library(data.table)  # Much faster and more memory-efficient than base R

# Load data using fread for better performance with large files
cat("Loading travel survey data...\n")
travel_data <- fread("data/processed/travel_survey_complete.csv", 
                     nrows = -1,  # Read all rows but efficiently
                     showProgress = TRUE)

# Print dataset dimensions
cat("\nDataset dimensions:\n")
cat(sprintf("  Rows: %d\n", nrow(travel_data)))
cat(sprintf("  Columns: %d\n", ncol(travel_data)))

# Print column names
cat("\nColumn names:\n")
print(names(travel_data))

# Print first few rows (using head to limit output)
cat("\nFirst 10 rows:\n")
print(head(travel_data, 10))

# Print summary statistics (safer than printing all data)
cat("\nSummary of first few columns:\n")
print(summary(travel_data[, 1:min(5, ncol(travel_data)), with = FALSE]))
