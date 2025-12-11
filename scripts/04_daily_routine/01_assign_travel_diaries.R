# Assign Travel Diaries to Synthetic Population
# 
# Strategy: For each household in the synthetic population, find an exact match
# in the NTS data based on household structure (adults/children composition).
# Assign the complete daily travel diary from the matched NTS household.

library(data.table)
library(dplyr)
library(yaml)

cat(rep("=", 70), "\n", sep = "")
cat("ASSIGN TRAVEL DIARIES\n")
cat(rep("=", 70), "\n\n", sep = "")

# ============================================================================
# Load Configuration
# ============================================================================

config <- yaml::read_yaml("config/pipeline_config.yml")

# ============================================================================
# Load Data
# ============================================================================

cat("Loading data...\n")

# Load lookup tables
lookup_transport <- fread("data/lookup_tables/transport_modes.csv")
lookup_purposes <- fread("data/lookup_tables/trip_purposes.csv")
lookup_age <- fread("data/lookup_tables/age_categories.csv")
lookup_sex <- fread("data/lookup_tables/sex_categories.csv")
lookup_nssec <- fread("data/lookup_tables/socioeconomic_class.csv")
lookup_hh_structure <- fread("data/lookup_tables/household_structure.csv")

cat("  Loaded lookup tables\n")

# Load synthetic population (latest file)
pop_files <- list.files(
  file.path(config$data$outputs, config$outputs$generation$dir),
  pattern = "synthetic_population_with_locations.*\\.csv",
  full.names = TRUE
)
pop_file <- sort(pop_files, decreasing = TRUE)[1]
cat("  Synthetic population:", basename(pop_file), "\n")

synthetic_pop <- fread(pop_file)
cat("    Loaded", nrow(synthetic_pop), "individuals\n")

# Load travel survey data
travel_survey_file <- file.path(config$data$processed, "travel_survey_complete.csv")
cat("  Travel survey data:", basename(travel_survey_file), "\n")

# Load only essential columns to save memory
travel_survey <- fread(
  travel_survey_file,
  select = c(
    "TripID", "DayID", "IndividualID", "HouseholdID",
    "JourSeq", "MainMode_B03ID", 
    "TripPurpFrom_B01ID", "TripPurpTo_B01ID",
    "TripStartHours", "TripStartMinutes",
    "Age_B04ID", "Sex_B01ID", "NSSec_B03ID", "HRPRelation_B01ID",
    "HHoldStruct_B02ID", "HHoldNumPeople", "HHoldNumAdults", "HHoldNumChildren",
    "TravelDayType_B01ID"
  )
)
cat("    Loaded", nrow(travel_survey), "trip records\n")

# Filter to school term-time weekdays only (day type 3)
travel_survey <- travel_survey[TravelDayType_B01ID == 3]
cat("    Filtered to", nrow(travel_survey), "school term-time weekday trip records\n")

# Filter out records with missing departure times
records_before <- nrow(travel_survey)
travel_survey <- travel_survey[!is.na(TripStartHours) & !is.na(TripStartMinutes)]
records_removed <- records_before - nrow(travel_survey)
cat("    Removed", records_removed, "records with missing departure times\n")

# Filter out records with missing trip purposes
records_before <- nrow(travel_survey)
travel_survey <- travel_survey[!is.na(TripPurpFrom_B01ID) & !is.na(TripPurpTo_B01ID)]
records_removed <- records_before - nrow(travel_survey)
cat("    Removed", records_removed, "records with missing trip purposes\n")

# Filter out records with missing transport mode
records_before <- nrow(travel_survey)
travel_survey <- travel_survey[!is.na(MainMode_B03ID)]
records_removed <- records_before - nrow(travel_survey)
cat("    Removed", records_removed, "records with missing transport mode\n")

cat("    Remaining:", nrow(travel_survey), "trip records\n")

# ============================================================================
# Prepare Household Summaries
# ============================================================================

cat("\nPreparing household summaries...\n")

# Synthetic population households
synthetic_hh <- synthetic_pop[, .(
  hh_structure = first(hh_adults_and_children_8m),
  hh_size = first(hh_size_9a),
  n_people = .N,
  n_adults = sum(assigned_age >= 16),
  n_children = sum(assigned_age < 16),
  n_cars = first(number_of_cars_5a)
), by = .(OA, household_id_m)]

cat("  Synthetic households:", nrow(synthetic_hh), "\n")
cat("  Structure distribution:\n")
print(table(synthetic_hh$hh_structure))

# NTS households (get one record per household)
# Also get age distribution within each household for better matching
nts_hh <- unique(travel_survey[, .(
  HouseholdID,
  hh_structure = HHoldStruct_B02ID,
  n_people = HHoldNumPeople,
  n_adults = HHoldNumAdults,
  n_children = HHoldNumChildren
)])

# Get age distribution for all households to enable better matching
nts_hh_ages <- travel_survey[, .(
  age_list = list(sort(Age_B04ID))
), by = HouseholdID]

nts_hh <- merge(nts_hh, nts_hh_ages, by = "HouseholdID")

cat("\n  NTS households:", nrow(nts_hh), "\n")
cat("  Structure distribution:\n")
print(table(nts_hh$hh_structure))

# ============================================================================
# Match Households by Structure
# ============================================================================

cat("\nMatching households by structure...\n")

# For each synthetic household structure, get available NTS households
# We'll use household structure as the primary matching criterion

set.seed(42)  # For reproducible matching

# Create matching table
matched_households <- data.table(
  OA = character(),
  household_id_m = integer(),
  matched_nts_household = numeric(),
  match_type = character()
)

pb <- txtProgressBar(min = 0, max = nrow(synthetic_hh), style = 3)

for (i in 1:nrow(synthetic_hh)) {
  hh <- synthetic_hh[i]
  
  # Try exact match on structure
  candidates <- nts_hh[
    hh_structure == hh$hh_structure & 
    n_adults == hh$n_adults & 
    n_children == hh$n_children
  ]
  
  if (nrow(candidates) > 0) {
    # For ALL households, try to match age distribution
    # Get ages of people in synthetic household
    hh_people <- synthetic_pop[OA == hh$OA & household_id_m == hh$household_id_m]
    
    # Map synthetic ages to NTS age categories
    synth_age_categories <- sapply(hh_people$assigned_age, function(age) {
      ifelse(age <= 4, 1L,
      ifelse(age <= 10, 2L,
      ifelse(age <= 16, 3L,
      ifelse(age <= 20, 4L,
      ifelse(age <= 29, 5L,
      ifelse(age <= 39, 6L,
      ifelse(age <= 49, 7L,
      ifelse(age <= 59, 8L, 9L))))))))
    })
    synth_age_categories <- sort(synth_age_categories)
    
    # Score each candidate household by age distribution similarity
    age_scores <- sapply(candidates$age_list, function(nts_ages) {
      # Calculate mean absolute difference between sorted age categories
      if (length(nts_ages) != length(synth_age_categories)) {
        return(Inf)  # Different number of people - bad match
      }
      mean(abs(nts_ages - synth_age_categories))
    })
    
    # Find households with best age match
    best_age_score <- min(age_scores)
    
    # If we have exact age matches (score = 0), use those
    # Otherwise, prefer households with close age matches (score < 2)
    if (best_age_score == 0) {
      best_matches <- candidates[age_scores == 0]
      matched_nts <- sample(best_matches$HouseholdID, 1)
    } else if (best_age_score < 2) {
      close_matches <- candidates[age_scores < 2]
      matched_nts <- sample(close_matches$HouseholdID, 1)
    } else {
      # No particularly good age match, use any candidate
      matched_nts <- sample(candidates$HouseholdID, 1)
    }
    
    match_type <- "exact"
  } else {
    # Fallback: match on structure code only
    candidates <- nts_hh[hh_structure == hh$hh_structure]
    
    if (nrow(candidates) > 0) {
      matched_nts <- sample(candidates$HouseholdID, 1)
      match_type <- "structure_only"
    } else {
      # Final fallback: match on adult/child composition
      candidates <- nts_hh[n_adults == hh$n_adults & n_children == hh$n_children]
      
      if (nrow(candidates) > 0) {
        matched_nts <- sample(candidates$HouseholdID, 1)
        match_type <- "composition"
      } else {
        # Use any household with similar size
        candidates <- nts_hh[abs(n_people - hh$n_people) <= 1]
        matched_nts <- sample(candidates$HouseholdID, 1)
        match_type <- "size_similar"
      }
    }
  }
  
  matched_households <- rbind(matched_households, data.table(
    OA = hh$OA,
    household_id_m = hh$household_id_m,
    matched_nts_household = matched_nts,
    match_type = match_type
  ))
  
  setTxtProgressBar(pb, i)
}

close(pb)

cat("\nMatch quality:\n")
print(table(matched_households$match_type))

# ============================================================================
# Assign Individual Travel Diaries
# ============================================================================

cat("\nAssigning travel diaries to individuals...\n")

# For each person in synthetic population, we need to:
# 1. Find their matched NTS household
# 2. Match them to an individual in that household by age/sex/role
# 3. Get all trips for that individual

# Add matched household to synthetic population
synthetic_pop_matched <- merge(
  synthetic_pop,
  matched_households,
  by = c("OA", "household_id_m"),
  all.x = TRUE
)

cat("  Matched", sum(!is.na(synthetic_pop_matched$matched_nts_household)), 
    "individuals to NTS households\n")

# Get unique individuals per NTS household
nts_individuals <- unique(travel_survey[, .(
  HouseholdID, IndividualID, DayID,
  Age_B04ID, Sex_B01ID, NSSec_B03ID, HRPRelation_B01ID
)])

# Decode NTS age categories for matching display
nts_individuals <- merge(
  nts_individuals,
  lookup_age %>% select(code, nts_age_category = description),
  by.x = "Age_B04ID",
  by.y = "code",
  all.x = TRUE
)

# For each synthetic person, find best matching individual in their matched NTS household
cat("  Matching individuals within households...\n")

pb <- txtProgressBar(min = 0, max = nrow(synthetic_pop_matched), style = 3)

matched_individuals <- list()

for (i in 1:nrow(synthetic_pop_matched)) {
  person <- synthetic_pop_matched[i]
  
  # Get individuals from matched NTS household
  candidates <- nts_individuals[HouseholdID == person$matched_nts_household]
  
  if (nrow(candidates) == 0) {
    # No data available for this household
    matched_individuals[[i]] <- data.table(
      OA = person$OA,
      household_id_m = person$household_id_m,
      resident_id_m = person$resident_id_m,
      matched_nts_individual = NA_real_,
      matched_day_id = NA_real_
    )
  } else {
    # Score candidates by similarity
    # Priority 1: Exact age category match (person's age falls in NTS age category)
    # Priority 2: Close age category
    # Priority 3: Sex match
    
    # Map synthetic age to age category code
    synth_age_category <- case_when(
      person$assigned_age <= 4 ~ 1L,
      person$assigned_age <= 10 ~ 2L,
      person$assigned_age <= 16 ~ 3L,
      person$assigned_age <= 20 ~ 4L,
      person$assigned_age <= 29 ~ 5L,
      person$assigned_age <= 39 ~ 6L,
      person$assigned_age <= 49 ~ 7L,
      person$assigned_age <= 59 ~ 8L,
      TRUE ~ 9L
    )
    
    # Calculate match scores
    age_category_match <- ifelse(candidates$Age_B04ID == synth_age_category, 0, 
                                  abs(candidates$Age_B04ID - synth_age_category))
    sex_match <- ifelse(candidates$Sex_B01ID == person$sex, 0, 1)
    
    # Combined score: prioritize age category (weight 10), then sex
    score <- age_category_match * 10 + sex_match
    
    # Pick best match (or random if tied)
    best_score <- min(score)
    best_candidates <- candidates[score == best_score]
    matched <- best_candidates[sample(.N, 1)]
    
    matched_individuals[[i]] <- data.table(
      OA = person$OA,
      household_id_m = person$household_id_m,
      resident_id_m = person$resident_id_m,
      matched_nts_individual = matched$IndividualID,
      matched_day_id = matched$DayID
    )
  }
  
  if (i %% 1000 == 0) setTxtProgressBar(pb, i)
}

close(pb)

matched_individuals_dt <- rbindlist(matched_individuals)

# ============================================================================
# Extract Travel Diaries
# ============================================================================

cat("\nExtracting travel diaries...\n")

# Join matched individuals back to synthetic population
synthetic_pop_final <- merge(
  synthetic_pop_matched,
  matched_individuals_dt,
  by = c("OA", "household_id_m", "resident_id_m"),
  all.x = TRUE
)

# Get all trips for matched individuals and include NTS age category
cat("  Getting trips for matched individuals...\n")

travel_diaries <- merge(
  synthetic_pop_final[, .(
    OA, household_id_m, resident_id_m, assigned_age,
    matched_nts_individual, matched_day_id, match_type
  )],
  travel_survey[, .(
    IndividualID, DayID, TripID, JourSeq,
    MainMode_B03ID, TripPurpFrom_B01ID, TripPurpTo_B01ID,
    TripStartHours, TripStartMinutes, Age_B04ID
  )],
  by.x = c("matched_nts_individual", "matched_day_id"),
  by.y = c("IndividualID", "DayID"),
  all.x = TRUE
)

# Sort by household and trip sequence
setorder(travel_diaries, OA, household_id_m, resident_id_m, JourSeq)

# Filter out any remaining records with missing critical data
records_before <- nrow(travel_diaries)
travel_diaries <- travel_diaries[
  is.na(TripID) | (
    !is.na(TripStartHours) & 
    !is.na(TripStartMinutes) &
    !is.na(TripPurpFrom_B01ID) &
    !is.na(TripPurpTo_B01ID) &
    !is.na(MainMode_B03ID)
  )
]
records_filtered <- records_before - nrow(travel_diaries)
if (records_filtered > 0) {
  cat("  Filtered", records_filtered, "additional trips with missing data\n")
}

# ============================================================================
# Save Results
# ============================================================================

cat("\nDecoding and saving results...\n")

# Decode travel diaries to plain English
travel_diaries_decoded <- travel_diaries %>%
  # Join age category description
  left_join(
    lookup_age %>% select(code, nts_age_category = description),
    by = c("Age_B04ID" = "code")
  ) %>%
  # Join transport mode descriptions
  left_join(
    lookup_transport %>% select(code, transport_mode = description, transport_category = category),
    by = c("MainMode_B03ID" = "code")
  ) %>%
  # Join trip purpose FROM
  left_join(
    lookup_purposes %>% select(code, trip_purpose_from = description, purpose_from_category = category),
    by = c("TripPurpFrom_B01ID" = "code")
  ) %>%
  # Join trip purpose TO
  left_join(
    lookup_purposes %>% select(code, trip_purpose_to = description, purpose_to_category = category),
    by = c("TripPurpTo_B01ID" = "code")
  ) %>%
  # Rename and select columns in logical order
  select(
    # Identifiers
    output_area = OA,
    household_id = household_id_m,
    person_id = resident_id_m,
    synthetic_age = assigned_age,
    nts_age_category,
    nts_age_code = Age_B04ID,
    
    # Matching info
    match_quality = match_type,
    nts_individual_id = matched_nts_individual,
    nts_day_id = matched_day_id,
    
    # Trip details
    trip_id = TripID,
    trip_sequence = JourSeq,
    trip_start_hour = TripStartHours,
    trip_start_minute = TripStartMinutes,
    
    # Transport mode
    transport_mode,
    transport_category,
    transport_mode_code = MainMode_B03ID,
    
    # Trip purposes
    trip_purpose_from,
    purpose_from_category,
    trip_purpose_from_code = TripPurpFrom_B01ID,
    
    trip_purpose_to,
    purpose_to_category,
    trip_purpose_to_code = TripPurpTo_B01ID
  ) %>%
  # Sort by household and trip sequence
  arrange(output_area, household_id, person_id, trip_sequence)

# Convert to data.table for efficient saving
travel_diaries_decoded <- as.data.table(travel_diaries_decoded)

timestamp <- format(Sys.time(), config$naming$timestamp_format)

# Save decoded travel diaries
output_file <- file.path(
  config$data$outputs,
  "04_daily_routine",
  paste0("travel_diaries_", timestamp, ".csv")
)

fwrite(travel_diaries_decoded, output_file)

cat("  Saved decoded travel diaries:", output_file, "\n")

# Save matching summary
match_summary_file <- file.path(
  config$data$outputs,
  "04_daily_routine",
  paste0("matching_summary_", timestamp, ".csv")
)

fwrite(matched_households, match_summary_file)

cat("  Saved matching summary:", match_summary_file, "\n")

# ============================================================================
# Summary Statistics
# ============================================================================

cat("\n", rep("=", 70), "\n", sep = "")
cat("SUMMARY\n")
cat(rep("=", 70), "\n", sep = "")

cat("\nIndividuals:\n")
cat("  Total:", nrow(synthetic_pop_final), "\n")
cat("  With matched NTS individual:", sum(!is.na(synthetic_pop_final$matched_nts_individual)), "\n")

cat("\nTrips:\n")
cat("  Total trip records:", nrow(travel_diaries_decoded), "\n")
cat("  Individuals with trips:", sum(!is.na(travel_diaries_decoded$trip_id)), "\n")
cat("  Individuals with no trips:", sum(is.na(travel_diaries_decoded$trip_id) & !is.na(travel_diaries_decoded$nts_individual_id)), "\n")

# Calculate trips per person
trips_per_person <- travel_diaries_decoded[!is.na(trip_id), .N, by = .(output_area, household_id, person_id)]
if (nrow(trips_per_person) > 0) {
  cat("  Average trips per person (for those with trips):", round(mean(trips_per_person$N), 2), "\n")
}

cat("\nMatch quality:\n")
print(table(matched_households$match_type))

cat("\n", rep("=", 70), "\n", sep = "")
cat("Complete!\n")
