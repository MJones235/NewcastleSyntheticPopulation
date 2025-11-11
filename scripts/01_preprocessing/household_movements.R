# Complete Household Travel Diaries Analysis
# Show complete household travel patterns for multi-traveler households on same day

library(dplyr)
library(readr)
library(jsonlite)

# Load lookup tables for meaningful descriptions
age_lookup <- read_csv("data/lookup_tables/age_categories.csv", show_col_types = FALSE)
sex_lookup <- read_csv("data/lookup_tables/sex_categories.csv", show_col_types = FALSE) 
transport_lookup <- read_csv("data/lookup_tables/transport_modes.csv", show_col_types = FALSE)
purpose_lookup <- read_csv("data/lookup_tables/trip_purposes.csv", show_col_types = FALSE)
hh_structure_lookup <- read_csv("data/lookup_tables/household_structure.csv", show_col_types = FALSE)
nssec_lookup <- read_csv("data/lookup_tables/socioeconomic_class.csv", show_col_types = FALSE)
geo_lookup <- read_csv("data/lookup_tables/geographic_regions.csv", show_col_types = FALSE)
day_type_lookup <- read_csv("data/lookup_tables/travel_day_types.csv", show_col_types = FALSE)
relationship_lookup <- read_csv("data/lookup_tables/household_relationships.csv", show_col_types = FALSE)
weekday_lookup <- read_csv("data/lookup_tables/day_of_week.csv", show_col_types = FALSE)
car_lookup <- read_csv("data/lookup_tables/car_ownership.csv", show_col_types = FALSE)

# Helper function to lookup descriptions
lookup_description <- function(codes, lookup_table) {
  matches <- match(codes, lookup_table$code)
  result <- lookup_table$description[matches]
  ifelse(is.na(result), paste("Unknown code:", codes), result)
}

# Function to format trip times
format_trip_time <- function(hours, minutes) {
  ifelse(is.na(hours) | is.na(minutes) | hours < 0 | minutes < 0,
         "Unknown time",
         sprintf("%02d:%02d", hours, minutes))
}

cat("=== Complete Household Travel Diaries Analysis ===\n\n")

# Read travel survey data from original tab file (to use TravDay instead of DayID)
cat("Reading original travel survey data...\n")
travel_data <- read_tsv("data/raw/trip.tab", show_col_types = FALSE)
cat("Loaded", format(nrow(travel_data), big.mark = ","), "trips\n")

# Find multi-traveler households using correct date grouping (TravDay + SurveyYear)
cat("Identifying multi-traveler households...\n")
multi_traveler_households <- travel_data %>%
  group_by(HouseholdID, SurveyYear, TravDay) %>%
  summarise(
    num_travelers = n_distinct(IndividualID),
    total_trips = n(),
    .groups = 'drop'
  ) %>%
  filter(num_travelers >= 2) %>%
  arrange(desc(num_travelers), desc(total_trips))

cat("Found", format(nrow(multi_traveler_households), big.mark = ","), 
    "household-days with multiple travelers\n")

# Select representative households for detailed analysis (2-4 travelers)
typical_households <- multi_traveler_households %>%
  filter(num_travelers >= 2, num_travelers <= 4) %>%
  arrange(desc(total_trips)) %>%
  head(5)  # Get top 5 examples

cat("Selected typical households for detailed analysis:\n")
for (i in 1:nrow(typical_households)) {
  hh <- typical_households[i, ]
  cat("- Household", hh$HouseholdID, "on", hh$SurveyYear, "day", hh$TravDay, 
      "(", hh$num_travelers, "travelers,", hh$total_trips, "trips)\n")
}

cat("\n")

# Create output directory if it doesn't exist
if (!dir.exists("data/outputs/household_diaries")) {
  dir.create("data/outputs/household_diaries", recursive = TRUE)
}

# Store all household diaries for JSON output
all_household_diaries <- list()

# Process each typical multi-traveler household
for (i in 1:nrow(typical_households)) {
  household_id <- typical_households$HouseholdID[i]
  survey_year <- typical_households$SurveyYear[i]
  trav_day <- typical_households$TravDay[i]
  
  # Get all trips for this household on this travel day
  household_trips <- travel_data %>%
    filter(HouseholdID == household_id, 
           SurveyYear == survey_year, 
           TravDay == trav_day) %>%
    arrange(IndividualID, JourSeq)
  
  # Get household demographics from first row
  hh_info <- household_trips[1, ]
  
  # Create household information with meaningful descriptions
  household_info <- list(
    household_id = as.character(household_id),
    survey_year = survey_year,
    travel_day = trav_day,
    travel_day_name = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")[trav_day],
    total_travelers = n_distinct(household_trips$IndividualID),
    total_trips = nrow(household_trips)
  )
  
  # Process individual travelers
  travelers <- unique(household_trips$IndividualID)
  household_members <- list()
  
  for (individual_id in travelers) {
    individual_trips <- household_trips %>%
      filter(IndividualID == individual_id) %>%
      arrange(JourSeq)
    
    demo_row <- individual_trips[1, ]
    
    # Create trip list for this individual with meaningful descriptions
    trips_list <- list()
    for (j in 1:nrow(individual_trips)) {
      trip <- individual_trips[j, ]
      
      trips_list[[j]] <- list(
        trip_id = as.character(trip$TripID),
        sequence = trip$JourSeq,
        start_time = format_trip_time(trip$TripStartHours, trip$TripStartMinutes),
        end_time = format_trip_time(trip$TripEndHours, trip$TripEndMinutes),
        transport = list(
          mode_code = trip$MainMode_B03ID,
          mode_description = lookup_description(trip$MainMode_B03ID, transport_lookup),
          category = transport_lookup$category[match(trip$MainMode_B03ID, transport_lookup$code)]
        ),
        journey = list(
          from = list(
            code = trip$TripPurpFrom_B01ID,
            purpose = lookup_description(trip$TripPurpFrom_B01ID, purpose_lookup),
            category = purpose_lookup$category[match(trip$TripPurpFrom_B01ID, purpose_lookup$code)]
          ),
          to = list(
            code = trip$TripPurpTo_B01ID,
            purpose = lookup_description(trip$TripPurpTo_B01ID, purpose_lookup),
            category = purpose_lookup$category[match(trip$TripPurpTo_B01ID, purpose_lookup$code)]
          )
        ),
        travel_time_minutes = ifelse(is.na(trip$TripTotalTime), NA, trip$TripTotalTime)
      )
    }
    
    # Create individual profile
    individual_profile <- list(
      individual_id = as.character(individual_id),
      person_number = demo_row$PersNo,
      total_trips = nrow(individual_trips),
      first_trip_start = format_trip_time(individual_trips$TripStartHours[1], individual_trips$TripStartMinutes[1]),
      last_trip_end = format_trip_time(individual_trips$TripEndHours[nrow(individual_trips)], individual_trips$TripEndMinutes[nrow(individual_trips)]),
      trips = trips_list
    )
    
    household_members[[length(household_members) + 1]] <- individual_profile
  }
  
  # Create complete household diary
  household_diary <- list(
    household_info = household_info,
    household_members = household_members
  )
  
  # Add to collection for JSON export
  all_household_diaries[[paste0("household_", i)]] <- household_diary
  
  # Output summary and JSON
  cat(sprintf("=== HOUSEHOLD %d COMPLETE TRAVEL DIARY ===\n", i))
  cat(sprintf("Household %s | %s %s | %s\n",
             household_id, survey_year, household_info$travel_day_name,
             paste("Day", trav_day)))
  cat(sprintf("Travelers: %d | Total trips: %d\n",
             household_info$total_travelers, household_info$total_trips))
  
  # Show traveler summary with sample trip descriptions
  cat("\nTraveler Summary with Sample Trips:\n")
  for (j in 1:length(household_members)) {
    member <- household_members[[j]]
    cat(sprintf("  Person %s: %d trips (%s to %s)\n",
               member$individual_id, member$total_trips,
               member$first_trip_start, member$last_trip_end))
    
    # Show first few trips with descriptions
    num_trips_to_show <- min(3, length(member$trips))
    for (k in 1:num_trips_to_show) {
      trip <- member$trips[[k]]
      cat(sprintf("    Trip %d: %s-%s via %s (%s → %s)\n",
                 k, trip$start_time, trip$end_time,
                 trip$transport$mode_description,
                 trip$journey$from$purpose,
                 trip$journey$to$purpose))
    }
    if (length(member$trips) > 3) {
      cat(sprintf("    ... and %d more trips\n", length(member$trips) - 3))
    }
    cat("\n")
  }
  
  cat("\n")
}

# Save all household diaries to JSON file
output_file <- "data/outputs/household_diaries/typical_household_travel_diaries.json"
cat("Saving typical household travel diaries to:", output_file, "\n")

# Create comprehensive export structure
export_data <- list(
  metadata = list(
    generated_date = Sys.time(),
    total_households_analyzed = nrow(typical_households),
    data_source = "UK National Travel Survey",
    description = "Complete travel diaries for typical-sized households (2-4 travelers) with multiple travelers on the same day",
    notes = "TravDay represents day of week (1=Monday, 7=Sunday), grouped by HouseholdID + SurveyYear + TravDay. Selected from most active typical households."
  ),
  summary = list(
    total_multi_traveler_household_days = nrow(multi_traveler_households),
    selected_for_detailed_analysis = nrow(typical_households),
    households = sapply(typical_households$HouseholdID, function(x) as.character(x)),
    household_sizes = typical_households$num_travelers,
    total_travelers = sum(sapply(all_household_diaries, function(x) x$household_info$total_travelers)),
    total_trips = sum(sapply(all_household_diaries, function(x) x$household_info$total_trips))
  ),
  household_diaries = all_household_diaries
)

# Write to JSON file
write(toJSON(export_data, pretty = TRUE, auto_unbox = TRUE), output_file)

# Also create a readable summary file
summary_file <- "data/outputs/household_diaries/typical_household_summary.txt"
cat("Creating readable summary file:", summary_file, "\n")

sink(summary_file)
cat("=== TYPICAL MULTI-TRAVELER HOUSEHOLD TRAVEL DIARIES SUMMARY ===\n")
cat("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

cat("OVERVIEW:\n")
cat("- Total multi-traveler household-days found:", format(nrow(multi_traveler_households), big.mark = ","), "\n")
cat("- Typical households selected for analysis:", nrow(typical_households), "\n")
cat("- Focus: 2-4 traveler households (representing ~94% of multi-traveler cases)\n")
cat("- Selection criteria: Most active households by trip count\n\n")

for (i in 1:nrow(typical_households)) {
  diary <- all_household_diaries[[paste0("household_", i)]]
  cat(sprintf("HOUSEHOLD %d:\n", i))
  cat(sprintf("  ID: %s | Year: %s | Day: %s (%s)\n", 
             diary$household_info$household_id,
             diary$household_info$survey_year,
             diary$household_info$travel_day_name,
             diary$household_info$travel_day))
  cat(sprintf("  Travelers: %d | Total trips: %d\n", 
             diary$household_info$total_travelers,
             diary$household_info$total_trips))
  
  cat("  Travel Schedule with Sample Trip Details:\n")
  for (j in 1:length(diary$household_members)) {
    member <- diary$household_members[[j]]
    cat(sprintf("    Person %s: %d trips (%s to %s)\n",
               member$individual_id, 
               member$total_trips,
               member$first_trip_start, 
               member$last_trip_end))
    
    # Add sample trip descriptions
    if (length(member$trips) > 0) {
      first_trip <- member$trips[[1]]
      cat(sprintf("      First trip: %s via %s (%s → %s)\n",
                 first_trip$start_time,
                 first_trip$transport$mode_description,
                 first_trip$journey$from$purpose,
                 first_trip$journey$to$purpose))
      
      if (length(member$trips) > 1) {
        last_trip <- member$trips[[length(member$trips)]]
        cat(sprintf("      Last trip: %s via %s (%s → %s)\n",
                   last_trip$start_time,
                   last_trip$transport$mode_description,
                   last_trip$journey$from$purpose,
                   last_trip$journey$to$purpose))
      }
    }
  }
  cat("\n")
}

sink()

cat("\n=== FILES CREATED ===\n")
cat("1. Complete JSON data:", output_file, "\n")
cat("2. Readable summary:", summary_file, "\n")

cat("\n=== ANALYSIS COMPLETE ===\n")