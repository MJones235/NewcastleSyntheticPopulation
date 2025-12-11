# Print Household Travel Diaries
# 
# Display the first few household travel diaries in an easy-to-read format

library(data.table)
library(yaml)

cat(rep("=", 80), "\n", sep = "")
cat("HOUSEHOLD TRAVEL DIARIES\n")
cat(rep("=", 80), "\n\n", sep = "")

# ============================================================================
# Load Configuration and Data
# ============================================================================

config <- yaml::read_yaml("config/pipeline_config.yml")

# Load the latest travel diaries
files <- list.files(
  file.path(config$data$outputs, "04_daily_routine"),
  pattern = "travel_diaries_.*\\.csv",
  full.names = TRUE
)
latest_file <- sort(files, decreasing = TRUE)[1]

cat("Loading:", basename(latest_file), "\n\n")
diaries <- fread(latest_file)

# ============================================================================
# Helper Function to Print One Household
# ============================================================================

print_household <- function(dt, hh_oa, hh_id) {
  household <- dt[output_area == hh_oa & household_id == hh_id]
  
  if (nrow(household) == 0) return(invisible())
  
  # Header
  cat(rep("=", 80), "\n", sep = "")
  cat("HOUSEHOLD:", hh_id, "\n")
  cat("Output Area:", hh_oa, "\n")
  cat("Household Size:", uniqueN(household$person_id), "people\n")
  cat("Total Trips:", sum(!is.na(household$trip_id)), "\n")
  cat(rep("=", 80), "\n\n", sep = "")
  
  # Get unique people, sorted by age (oldest first)
  people <- unique(household[, .(
    person_id,
    synthetic_age,
    nts_age_category,
    match_quality
  )])
  setorder(people, -synthetic_age)
  
  # Print each person's diary
  for (i in 1:nrow(people)) {
    person_info <- people[i]
    person_trips <- household[person_id == person_info$person_id & !is.na(trip_id)]
    setorder(person_trips, trip_sequence)
    
    # Person header
    cat(rep("-", 80), "\n", sep = "")
    cat(sprintf("Person %d: Age %d (%s)\n",
                person_info$person_id,
                person_info$synthetic_age,
                person_info$nts_age_category))
    cat(sprintf("Match Quality: %s\n", person_info$match_quality))
    cat(sprintf("Trips: %d\n", nrow(person_trips)))
    cat(rep("-", 80), "\n", sep = "")
    
    if (nrow(person_trips) == 0) {
      cat("  No trips recorded\n")
    } else {
      # Print each trip
      for (j in 1:nrow(person_trips)) {
        trip <- person_trips[j]
        
        # Format time
        time_str <- sprintf("%02d:%02d", 
                           ifelse(is.na(trip$trip_start_hour), 0, trip$trip_start_hour),
                           ifelse(is.na(trip$trip_start_minute), 0, trip$trip_start_minute))
        if (is.na(trip$trip_start_hour)) time_str <- "  ?  "
        
        cat(sprintf("\n  Trip %d @ %s\n", trip$trip_sequence, time_str))
        cat(sprintf("    Mode:    %s\n", trip$transport_mode))
        cat(sprintf("    From:    %s\n", trip$trip_purpose_from))
        cat(sprintf("    To:      %s\n", trip$trip_purpose_to))
      }
    }
    
    cat("\n")
  }
}

# ============================================================================
# Print First 5 Households
# ============================================================================

# Get unique households
households <- unique(diaries[, .(output_area, household_id)])
setorder(households, output_area, household_id)

n_to_print <- min(5, nrow(households))

cat("Showing first", n_to_print, "households:\n\n")

for (i in 1:n_to_print) {
  hh <- households[i]
  print_household(diaries, hh$output_area, hh$household_id)
  if (i < n_to_print) cat("\n\n")
}

cat("\n", rep("=", 80), "\n", sep = "")
cat("Complete!\n")
cat("Total households in dataset:", nrow(households), "\n")
cat(rep("=", 80), "\n", sep = "")
