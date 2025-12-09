# Display Daily Routine for a Household
# This script safely loads travel data and shows all trips for one household

library(data.table)  # Much faster and more memory-efficient than base R

# Load data using fread for better performance with large files
cat("Loading travel survey data...\n")
travel_data <- fread("data/processed/travel_survey_complete.csv", 
                     showProgress = FALSE)  # Disable progress to reduce output

cat(sprintf("Loaded %d trips from %d households\n", 
            nrow(travel_data), 
            uniqueN(travel_data$HouseholdID)))

# Get first 5 households in the dataset
first_households <- unique(travel_data$HouseholdID)[1:5]

cat("\nShowing daily routines for first 5 households (TravDay = 1)\n")
cat(paste0(paste(rep("=", 70), collapse = ""), "\n\n"))

# Loop through each household
for (sample_household in first_households) {
  
  cat(sprintf("HOUSEHOLD: %s\n", sample_household))
  cat(paste0(paste(rep("-", 70), collapse = ""), "\n"))
  
  # Filter data for this household only (much smaller dataset)
  household_trips <- travel_data[HouseholdID == sample_household]
  
  # Filter to only show TravDay = 1
  household_trips <- household_trips[TravDay == 1]
  
  if (nrow(household_trips) == 0) {
    cat("No trips found for TravDay = 1\n\n")
    next
  }
  
  # Order by individual and trip start time
  setorder(household_trips, IndividualID, TripStartHours, TripStartMinutes)
  
  # Get unique individuals in this household
  individuals <- unique(household_trips$IndividualID)
  
  cat(sprintf("Household has %d member(s) with travel data\n", length(individuals)))
  cat(sprintf("Total trips: %d\n", nrow(household_trips)))
  
  # Print trips for each individual
  for (ind in individuals) {
    ind_trips <- household_trips[IndividualID == ind]
    
    cat(sprintf("\n  Individual ID: %s (%d trips)\n", ind, nrow(ind_trips)))
    
    # Select key columns to display
    display_cols <- c("TripStartHours", "TripStartMinutes", 
                      "TripPurpTo_B01ID", "TripMode")
    
    # Only use columns that exist in the dataset
    display_cols <- display_cols[display_cols %in% names(ind_trips)]
    
    if (length(display_cols) > 0) {
      # Print with minimal formatting to avoid crashes
      print(ind_trips[, ..display_cols], row.names = FALSE)
    }
  }
  
  cat("\n")
}

cat(paste0("\n", paste(rep("=", 70), collapse = ""), "\n"))
cat("Daily routine display complete\n")
