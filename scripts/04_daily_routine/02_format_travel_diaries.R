# Format Travel Diaries for Simulation
# 
# Transform travel diaries into the format needed for simulation:
# - Map trip purposes to location types and actual OSM buildings
# - Simplify transport modes to the 5 valid categories
# - Create origin/destination pairs with coordinates

library(data.table)
library(dplyr)
library(yaml)
library(sf)
library(osmdata)

cat(rep("=", 70), "\n", sep = "")
cat("FORMAT TRAVEL DIARIES FOR SIMULATION\n")
cat(rep("=", 70), "\n\n", sep = "")

# ============================================================================
# Load Configuration
# ============================================================================

config <- yaml::read_yaml("config/pipeline_config.yml")

# ============================================================================
# Load Data
# ============================================================================

cat("Loading data...\n")

# Load latest travel diaries
diary_files <- list.files(
  file.path(config$data$outputs, "04_daily_routine"),
  pattern = "travel_diaries_.*\\.csv",
  full.names = TRUE
)
diary_file <- sort(diary_files, decreasing = TRUE)[1]
cat("  Travel diaries:", basename(diary_file), "\n")

travel_diaries <- fread(diary_file)
cat("    Loaded", nrow(travel_diaries), "trip records\n")

# Load synthetic population with locations
pop_files <- list.files(
  file.path(config$data$outputs, config$outputs$generation$dir),
  pattern = "synthetic_population_with_locations.*\\.csv",
  full.names = TRUE
)
pop_file <- sort(pop_files, decreasing = TRUE)[1]
cat("  Population with locations:", basename(pop_file), "\n")

synthetic_pop <- fread(pop_file)
cat("    Loaded", nrow(synthetic_pop), "individuals\n")

# ============================================================================
# Map Transport Modes
# ============================================================================

cat("\nMapping transport modes...\n")

# Map NTS transport modes to simulation modes (car, bus, bicycle, walk, metro)
transport_mapping <- data.table(
  transport_mode = c(
    "Walk, less than 1 mile",
    "Walk, 1 mile or more",
    "Pedal cycle",
    "Household car - driver",
    "Household car - passenger",
    "Non-household car - driver",
    "Non-household car - passenger",
    "Taxi",
    "Household van / lorry - driver",
    "Household van / lorry - passenger",
    "Non-household van / lorry - driver",
    "Non-household van / lorry - passenger",
    "Motorcycle",
    "Other private transport",
    "London stage bus",
    "Other stage bus",
    "Private (hire) bus",
    "Surface Rail",
    "London Underground",
    "Light rail",
    "Other public transport",
    "Aeroplane",
    "Ferry"
  ),
  sim_mode = c(
    "walk",      # Walk < 1 mile
    "walk",      # Walk >= 1 mile
    "bicycle",   # Pedal cycle
    "car",       # Household car - driver
    "car",       # Household car - passenger
    "car",       # Non-household car - driver
    "car",       # Non-household car - passenger
    "car",       # Taxi (treat as car)
    "car",       # Household van - driver
    "car",       # Household van - passenger
    "car",       # Non-household van - driver
    "car",       # Non-household van - passenger
    "car",       # Motorcycle (treat as car for now)
    "car",       # Other private
    "bus",       # London stage bus
    "bus",       # Other stage bus
    "bus",       # Private bus
    "metro",     # Surface Rail (treat as metro - rapid transit)
    "metro",     # London Underground
    "metro",     # Light rail
    "bus",       # Other public transport
    "car",       # Aeroplane (treat as car to/from airport)
    "bus"        # Ferry (treat as bus)
  )
)

# Join transport mapping
travel_diaries <- merge(
  travel_diaries,
  transport_mapping,
  by = "transport_mode",
  all.x = TRUE
)

# For any unmapped modes, default to walk
travel_diaries[is.na(sim_mode), sim_mode := "walk"]

cat("  Transport mode distribution:\n")
print(travel_diaries[!is.na(trip_id), .N, by = sim_mode][order(-N)])

# ============================================================================
# Map Trip Purposes to Location Types
# ============================================================================

cat("\nMapping trip purposes to location types...\n")

# Create purpose to location type mapping
purpose_to_location <- data.table(
  trip_purpose = c(
    "Home",
    "Work",
    "In course of work",
    "Education",
    "Escort work",
    "Escort education",
    "Escort shopping / personal business",
    "Escort in course of work",
    "Other escort",
    "Food shopping",
    "Non food shopping",
    "Personal business medical",
    "Personal business eat / drink",
    "Personal business other",
    "Eat / drink with friends",
    "Visit friends",
    "Other social",
    "Entertain / public activity",
    "Sport: participate",
    "Holiday: base",
    "Day trip / just walk",
    "Other non-escort",
    "Escort home"
  ),
  location_type = c(
    "home",
    "work",
    "work",
    "education",
    "work",
    "education",
    "commercial",
    "work",  # Escort in course of work
    "home",  # Other escort - assume dropping off at home
    "commercial",
    "commercial",
    "amenity",  # Medical
    "commercial",  # Eat/drink
    "amenity",  # Other personal business
    "commercial",  # Eat/drink with friends
    "home",  # Visit friends - residential
    "amenity",  # Other social
    "amenity",  # Entertainment
    "amenity",  # Sport
    "home",  # Holiday base
    "amenity",  # Day trip
    "amenity",  # Other
    "home"  # Escort home
  )
)

# Join location types for origins (FROM) and destinations (TO)
travel_diaries <- merge(
  travel_diaries,
  purpose_to_location,
  by.x = "trip_purpose_from",
  by.y = "trip_purpose",
  all.x = TRUE
)
setnames(travel_diaries, "location_type", "origin_type")

travel_diaries <- merge(
  travel_diaries,
  purpose_to_location,
  by.x = "trip_purpose_to",
  by.y = "trip_purpose",
  all.x = TRUE
)
setnames(travel_diaries, "location_type", "dest_type")

cat("  Origin type distribution:\n")
print(travel_diaries[!is.na(trip_id), .N, by = origin_type][order(-N)])

cat("\n  Destination type distribution:\n")
print(travel_diaries[!is.na(trip_id), .N, by = dest_type][order(-N)])

# ============================================================================
# Add Assigned Locations from Synthetic Population
# ============================================================================

cat("\nAdding assigned locations from synthetic population...\n")

# Get assigned locations (home, work, school) from synthetic population
person_locations <- synthetic_pop[, .(
  output_area = OA,
  household_id = household_id_m,
  person_id = resident_id_m,
  home_osm_id = home_osm_id,
  home_lat = home_latitude,
  home_lon = home_longitude,
  work_school_osm_id = destination_osm_id,
  work_school_lat = destination_latitude,
  work_school_lon = destination_longitude,
  work_school_type = destination_type
)]

# Join person locations
travel_diaries <- merge(
  travel_diaries,
  person_locations,
  by = c("output_area", "household_id", "person_id"),
  all.x = TRUE
)

cat("  Added home locations for", sum(!is.na(travel_diaries$home_osm_id)), "records\n")
cat("  Added work/school locations for", sum(!is.na(travel_diaries$work_school_osm_id)), "records\n")

# ============================================================================
# Assign OSM Locations for Origins and Destinations
# ============================================================================

cat("\nAssigning OSM locations for trip ends...\n")

# Initialize location columns
travel_diaries[, `:=`(
  origin_osm_id = NA_character_,
  origin_lat = NA_real_,
  origin_lon = NA_real_,
  dest_osm_id = NA_character_,
  dest_lat = NA_real_,
  dest_lon = NA_real_
)]

# 1. Assign home locations
travel_diaries[origin_type == "home", `:=`(
  origin_osm_id = home_osm_id,
  origin_lat = home_lat,
  origin_lon = home_lon
)]

travel_diaries[dest_type == "home", `:=`(
  dest_osm_id = home_osm_id,
  dest_lat = home_lat,
  dest_lon = home_lon
)]

# 2. Assign work/education locations from synthetic population
travel_diaries[origin_type %in% c("work", "education") & !is.na(work_school_osm_id), `:=`(
  origin_osm_id = work_school_osm_id,
  origin_lat = work_school_lat,
  origin_lon = work_school_lon
)]

travel_diaries[dest_type %in% c("work", "education") & !is.na(work_school_osm_id), `:=`(
  dest_osm_id = work_school_osm_id,
  dest_lat = work_school_lat,
  dest_lon = work_school_lon
)]

cat("  Assigned home locations:", 
    sum(!is.na(travel_diaries$origin_osm_id) & travel_diaries$origin_type == "home", na.rm = TRUE) +
    sum(!is.na(travel_diaries$dest_osm_id) & travel_diaries$dest_type == "home", na.rm = TRUE), "\n")
cat("  Assigned work/education locations:",
    sum(!is.na(travel_diaries$origin_osm_id) & travel_diaries$origin_type %in% c("work", "education"), na.rm = TRUE) +
    sum(!is.na(travel_diaries$dest_osm_id) & travel_diaries$dest_type %in% c("work", "education"), na.rm = TRUE), "\n")

# 3. Load OSM buildings for other destinations (commercial, amenity)
cat("\nLoading OSM buildings for other destinations...\n")

# Get bounding box for Newcastle
bbox <- getbb("Newcastle upon Tyne, UK")

# Load shops (commercial)
cat("  Loading commercial buildings (shops)...\n")
shops <- opq(bbox) %>%
  add_osm_feature(key = "shop") %>%
  osmdata_sf()

if (!is.null(shops$osm_points)) {
  commercial_buildings <- shops$osm_points %>%
    st_transform(4326) %>%
    select(osm_id, name, shop) %>%
    mutate(
      osm_id = as.character(osm_id),
      lat = st_coordinates(.)[,2],
      lon = st_coordinates(.)[,1]
    ) %>%
    st_drop_geometry()
  
  cat("    Loaded", nrow(commercial_buildings), "commercial buildings\n")
} else {
  commercial_buildings <- data.table(osm_id = character(), lat = numeric(), lon = numeric())
}

# Load amenities
cat("  Loading amenity buildings...\n")
amenities <- opq(bbox) %>%
  add_osm_feature(key = "amenity") %>%
  osmdata_sf()

if (!is.null(amenities$osm_points)) {
  amenity_buildings <- amenities$osm_points %>%
    st_transform(4326) %>%
    select(osm_id, name, amenity) %>%
    mutate(
      osm_id = as.character(osm_id),
      lat = st_coordinates(.)[,2],
      lon = st_coordinates(.)[,1]
    ) %>%
    st_drop_geometry()
  
  cat("    Loaded", nrow(amenity_buildings), "amenity buildings\n")
} else {
  amenity_buildings <- data.table(osm_id = character(), lat = numeric(), lon = numeric())
}

# Convert to data.tables
commercial_buildings <- as.data.table(commercial_buildings)
amenity_buildings <- as.data.table(amenity_buildings)

# 4. Assign nearest building for remaining origins and destinations
cat("\nAssigning nearest buildings for unassigned locations...\n")

# Function to find nearest building
find_nearest_building <- function(lat, lon, building_dt) {
  if (is.na(lat) || is.na(lon) || nrow(building_dt) == 0) {
    return(list(osm_id = NA_character_, lat = NA_real_, lon = NA_real_))
  }
  
  # Calculate distances (simple Euclidean for speed)
  distances <- sqrt((building_dt$lat - lat)^2 + (building_dt$lon - lon)^2)
  nearest_idx <- which.min(distances)
  
  if (length(nearest_idx) > 0) {
    return(list(
      osm_id = building_dt$osm_id[nearest_idx],
      lat = building_dt$lat[nearest_idx],
      lon = building_dt$lon[nearest_idx]
    ))
  } else {
    return(list(osm_id = NA_character_, lat = NA_real_, lon = NA_real_))
  }
}

# Assign commercial locations
commercial_rows <- which(is.na(travel_diaries$origin_osm_id) & travel_diaries$origin_type == "commercial")
if (length(commercial_rows) > 0 && nrow(commercial_buildings) > 0) {
  cat("  Assigning", length(commercial_rows), "commercial origins...\n")
  for (idx in commercial_rows) {
    nearest <- find_nearest_building(
      travel_diaries$home_lat[idx],
      travel_diaries$home_lon[idx],
      commercial_buildings
    )
    travel_diaries[idx, `:=`(
      origin_osm_id = nearest$osm_id,
      origin_lat = nearest$lat,
      origin_lon = nearest$lon
    )]
  }
}

commercial_rows <- which(is.na(travel_diaries$dest_osm_id) & travel_diaries$dest_type == "commercial")
if (length(commercial_rows) > 0 && nrow(commercial_buildings) > 0) {
  cat("  Assigning", length(commercial_rows), "commercial destinations...\n")
  for (idx in commercial_rows) {
    nearest <- find_nearest_building(
      travel_diaries$home_lat[idx],
      travel_diaries$home_lon[idx],
      commercial_buildings
    )
    travel_diaries[idx, `:=`(
      dest_osm_id = nearest$osm_id,
      dest_lat = nearest$lat,
      dest_lon = nearest$lon
    )]
  }
}

# Assign amenity locations
amenity_rows <- which(is.na(travel_diaries$origin_osm_id) & travel_diaries$origin_type == "amenity")
if (length(amenity_rows) > 0 && nrow(amenity_buildings) > 0) {
  cat("  Assigning", length(amenity_rows), "amenity origins...\n")
  for (idx in amenity_rows) {
    nearest <- find_nearest_building(
      travel_diaries$home_lat[idx],
      travel_diaries$home_lon[idx],
      amenity_buildings
    )
    travel_diaries[idx, `:=`(
      origin_osm_id = nearest$osm_id,
      origin_lat = nearest$lat,
      origin_lon = nearest$lon
    )]
  }
}

amenity_rows <- which(is.na(travel_diaries$dest_osm_id) & travel_diaries$dest_type == "amenity")
if (length(amenity_rows) > 0 && nrow(amenity_buildings) > 0) {
  cat("  Assigning", length(amenity_rows), "amenity destinations...\n")
  for (idx in amenity_rows) {
    nearest <- find_nearest_building(
      travel_diaries$home_lat[idx],
      travel_diaries$home_lon[idx],
      amenity_buildings
    )
    travel_diaries[idx, `:=`(
      dest_osm_id = nearest$osm_id,
      dest_lat = nearest$lat,
      dest_lon = nearest$lon
    )]
  }
}

# For any remaining unassigned locations, use home as fallback
travel_diaries[is.na(origin_osm_id), `:=`(
  origin_osm_id = home_osm_id,
  origin_lat = home_lat,
  origin_lon = home_lon
)]

travel_diaries[is.na(dest_osm_id), `:=`(
  dest_osm_id = home_osm_id,
  dest_lat = home_lat,
  dest_lon = home_lon
)]

cat("\nLocation assignment complete\n")

# ============================================================================
# Format Output
# ============================================================================

cat("\nFormatting output...\n")

# Select and rename columns to match required format
formatted_diaries <- travel_diaries[!is.na(trip_id), .(
  output_area,
  household_id,
  person_id,
  trip_seq = trip_sequence,
  origin_type,
  origin_osm_id,
  origin_lat,
  origin_lon,
  dest_type,
  dest_osm_id,
  dest_lat,
  dest_lon,
  depart_time_hours = trip_start_hour,
  depart_time_minutes = trip_start_minute,
  transport_mode = sim_mode
)]

# Filter out records with missing departure times (data quality check)
records_before_filter <- nrow(formatted_diaries)
formatted_diaries <- formatted_diaries[!is.na(depart_time_hours) & !is.na(depart_time_minutes)]
records_filtered <- records_before_filter - nrow(formatted_diaries)

if (records_filtered > 0) {
  cat("  Filtered out", records_filtered, "records with missing departure times\n")
}

# Sort by household, person, and trip sequence
setorder(formatted_diaries, output_area, household_id, person_id, trip_seq)

cat("  Formatted", nrow(formatted_diaries), "trip records\n")

# ============================================================================
# Save Results
# ============================================================================

timestamp <- format(Sys.time(), config$naming$timestamp_format)

output_file <- file.path(
  config$data$outputs,
  "04_daily_routine",
  paste0("formatted_travel_diaries_", timestamp, ".csv")
)

fwrite(formatted_diaries, output_file)

cat("\n  Saved formatted diaries:", output_file, "\n")

# ============================================================================
# Summary Statistics
# ============================================================================

cat("\n", rep("=", 70), "\n", sep = "")
cat("SUMMARY\n")
cat(rep("=", 70), "\n", sep = "")

cat("\nTrips:\n")
cat("  Total:", nrow(formatted_diaries), "\n")
cat("  Individuals with trips:", uniqueN(formatted_diaries[, .(output_area, household_id, person_id)]), "\n")

cat("\nTransport modes:\n")
print(formatted_diaries[, .N, by = transport_mode][order(-N)])

cat("\nOrigin types:\n")
print(formatted_diaries[, .N, by = origin_type][order(-N)])

cat("\nDestination types:\n")
print(formatted_diaries[, .N, by = dest_type][order(-N)])

cat("\nHome location coverage:\n")
cat("  Trips starting from home:", sum(formatted_diaries$origin_type == "home"), "\n")
cat("  Trips ending at home:", sum(formatted_diaries$dest_type == "home"), "\n")

cat("\nLocation completeness:\n")
cat("  Origins with valid locations:", sum(!is.na(formatted_diaries$origin_osm_id)), "/", nrow(formatted_diaries), "\n")
cat("  Destinations with valid locations:", sum(!is.na(formatted_diaries$dest_osm_id)), "/", nrow(formatted_diaries), "\n")

cat("\n", rep("=", 70), "\n", sep = "")
cat("Complete!\n")
cat(rep("=", 70), "\n", sep = "")
