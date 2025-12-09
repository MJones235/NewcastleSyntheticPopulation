# ============================================================================
# Add Home Locations to Synthetic Population
# ============================================================================
# Simple script to assign home locations using OpenStreetMap buildings
# 
# Required:
#   - Synthetic population with ages (from step 03)
#   - OA boundary file: data/processed/output_area_boundaries.gpkg
#
# Output:
#   - synthetic_population_with_locations_{timestamp}.csv
# ============================================================================

library(tidyverse)
library(osmdata)
library(sf)
library(yaml)

config <- yaml::read_yaml("config/pipeline_config.yml")

# ============================================================================
# Load Data
# ============================================================================

# Get most recent population file with ages
population_files <- list.files(
  path = file.path(config$data$outputs, config$outputs$generation$dir),
  pattern = "synthetic_population_with_ages_.*\\.csv",
  full.names = TRUE
)

if (length(population_files) == 0) {
  stop("No population file with ages found. Run 03_add_ages.R first.")
}

input_file <- sort(population_files, decreasing = TRUE)[1]
cat("Loading population from:", input_file, "\n")

population <- read_csv(input_file, show_col_types = FALSE)

n_households <- population %>%
  distinct(OA, household_id_m) %>%
  nrow()

cat("Population:", nrow(population), "individuals in",
    n_households, "households across",
    length(unique(population$OA)), "Output Areas\n")

# Load OA boundaries
boundary_file <- file.path(config$data$processed, "output_area_boundaries.gpkg")

if (!file.exists(boundary_file)) {
  stop("OA boundary file not found: ", boundary_file)
}

cat("\nLoading OA boundaries from:", boundary_file, "\n")
oa_boundaries <- st_read(boundary_file, quiet = TRUE)
cat("Loaded", nrow(oa_boundaries), "boundaries\n")

# Identify OA code column
oa_code_col <- names(oa_boundaries)[grep("^OA", names(oa_boundaries), ignore.case=TRUE)][1]
if (is.na(oa_code_col)) {
  oa_code_col <- names(oa_boundaries)[1]
}
cat("Using OA code column:", oa_code_col, "\n")

# ============================================================================
# Query Buildings for Entire Region
# ============================================================================

cat("\nQuerying buildings for entire region...\n")

oa_codes <- unique(population$OA)
all_oas_wgs84 <- st_transform(
  oa_boundaries %>% filter(.data[[oa_code_col]] %in% oa_codes), 
  crs = 4326
)
region_bbox <- st_bbox(all_oas_wgs84)
region_bbox_vec <- as.vector(region_bbox)
names(region_bbox_vec) <- c("xmin", "ymin", "xmax", "ymax")

# Query with retry logic
regional_buildings <- NULL
max_retries <- 3

for (attempt in 1:max_retries) {
  if (attempt > 1) {
    backoff <- 3 * attempt
    cat("Waiting", backoff, "s before retry", attempt, "/", max_retries, "...\n")
    Sys.sleep(backoff)
  }
  
  regional_buildings <- tryCatch({
    query <- opq(bbox = region_bbox_vec) %>%
      add_osm_feature(key = "building") %>%
      osmdata_sf()
    
    polys <- query$osm_polygons
    
    if (!is.null(polys) && nrow(polys) > 0) {
      # Filter to residential building types
      if ("building" %in% names(polys)) {
        residential_types <- c(
          "yes", "residential", "house", "detached", "semidetached_house", 
          "terrace", "apartments", "bungalow", "cabin", "dwelling_house", "dormitory"
        )
        
        polys <- polys %>%
          filter(building %in% residential_types)
      }
      
      # Transform to OA CRS
      polys <- st_transform(polys, st_crs(oa_boundaries))
      
      cat("Found", nrow(polys), "residential buildings in region\n")
      polys
    } else {
      NULL
    }
  }, error = function(e) {
    cat("Attempt", attempt, "failed:", e$message, "\n")
    NULL
  })
  
  if (!is.null(regional_buildings)) {
    break
  }
}

if (is.null(regional_buildings)) {
  stop("Failed to retrieve buildings after ", max_retries, " attempts")
}

# ============================================================================
# Prepare Buildings with Weights
# ============================================================================

cat("\nPreparing buildings with area weights...\n")

# Calculate building areas (as a proxy for capacity)
regional_buildings$area <- as.numeric(st_area(regional_buildings))

# Filter out buildings with zero or very small area
regional_buildings <- regional_buildings %>%
  filter(area > 10)  # At least 10 sq meters

cat("Using", nrow(regional_buildings), "buildings with area > 10 sq meters\n")

# ============================================================================
# Assign Households to Buildings
# ============================================================================

cat("\nAssigning households to buildings...\n")

home_locations <- list()
pb <- txtProgressBar(min = 0, max = length(oa_codes), style = 3)

for (i in seq_along(oa_codes)) {
  oa_code <- oa_codes[i]
  
  # Get households in this OA
  oa_households <- population %>%
    filter(OA == oa_code) %>%
    distinct(household_id_m) %>%
    pull(household_id_m)
  
  # Get OA boundary
  oa_boundary <- oa_boundaries %>% filter(.data[[oa_code_col]] == oa_code)
  
  if (nrow(oa_boundary) == 0) {
    warning("No boundary found for OA: ", oa_code)
    setTxtProgressBar(pb, i)
    next
  }
  
  # Find buildings within this OA using spatial intersection with buffer
  # Use a 50m buffer to catch buildings near OA edges
  oa_boundary_buffered <- st_buffer(oa_boundary, dist = 50)
  buildings_in_oa_idx <- st_intersects(regional_buildings, oa_boundary_buffered, sparse = FALSE)[,1]
  buildings_in_oa <- regional_buildings[buildings_in_oa_idx, ]
  
  if (nrow(buildings_in_oa) == 0) {
    # No buildings in OA - find nearby buildings within 500m
    oa_centroid <- st_centroid(oa_boundary)
    distances <- st_distance(regional_buildings, oa_centroid)
    nearby_idx <- which(as.numeric(distances) < 500)
    
    if (length(nearby_idx) > 0) {
      buildings_in_oa <- regional_buildings[nearby_idx, ]
    } else {
      warning("No buildings found for OA: ", oa_code)
      setTxtProgressBar(pb, i)
      next
    }
  }
  
  # Sample buildings weighted by area (larger buildings can accommodate more households)
  n_households <- length(oa_households)
  weights <- buildings_in_oa$area
  
  # Sample buildings with replacement, weighted by area
  set.seed(42 + i)  # Reproducible but different per OA
  sampled_indices <- sample(
    nrow(buildings_in_oa), 
    size = n_households, 
    replace = TRUE, 
    prob = weights
  )
  
  sampled_buildings <- buildings_in_oa[sampled_indices, ]
  
  # For each unique building, count how many households and generate that many random points
  building_counts <- table(sampled_buildings$osm_id)
  unique_building_ids <- as.numeric(names(building_counts))
  
  all_points_list <- list()
  all_building_ids <- numeric()
  all_building_types <- character()
  
  for (building_id in unique_building_ids) {
    n_points <- building_counts[as.character(building_id)]
    building_row <- buildings_in_oa[buildings_in_oa$osm_id == building_id, ][1, ]
    
    # Generate multiple random points at once for this building
    points <- tryCatch({
      st_sample(building_row$geometry, size = n_points, type = "random")
    }, error = function(e) {
      # Fallback: generate points with jitter around centroid
      centroid <- st_centroid(building_row$geometry)
      bbox <- st_bbox(building_row$geometry)
      width <- bbox["xmax"] - bbox["xmin"]
      height <- bbox["ymax"] - bbox["ymin"]
      
      # Generate random offsets
      offsets_x <- runif(n_points, -width/4, width/4)
      offsets_y <- runif(n_points, -height/4, height/4)
      
      centroid_coords <- st_coordinates(centroid)
      jittered_points <- lapply(1:n_points, function(j) {
        st_point(c(centroid_coords[1] + offsets_x[j], 
                   centroid_coords[2] + offsets_y[j]))
      })
      st_sfc(jittered_points, crs = st_crs(building_row))
    })
    
    all_points_list[[length(all_points_list) + 1]] <- points
    all_building_ids <- c(all_building_ids, rep(building_id, n_points))
    all_building_types <- c(all_building_types, rep(building_row$building, n_points))
  }
  
  # Combine all points
  all_points <- do.call(c, all_points_list)
  
  # Transform to WGS84
  points_wgs84 <- st_transform(all_points, crs = 4326)
  coords <- st_coordinates(points_wgs84)
  
  home_locations[[oa_code]] <- data.frame(
    OA = oa_code,
    household_id_m = oa_households,
    home_osm_id = all_building_ids,
    home_building_type = all_building_types,
    home_longitude = coords[, "X"],
    home_latitude = coords[, "Y"],
    stringsAsFactors = FALSE
  )
  
  setTxtProgressBar(pb, i)
}

close(pb)

# Combine all locations
home_locations <- bind_rows(home_locations)

cat("\n  Assigned", nrow(home_locations), "households to buildings\n")

# Count unique locations (by coordinates)
unique_locations <- home_locations %>%
  distinct(home_longitude, home_latitude) %>%
  nrow()

cat("  Using", unique_locations, "unique locations from",
    length(unique(home_locations$home_osm_id)), "buildings\n")

# ============================================================================
# Query Schools, Colleges, Universities, and Workplaces
# ============================================================================

cat("\nQuerying destinations...\n")

# Create extended bounding box covering all OAs (in WGS84)
bbox_extended <- st_bbox(all_oas_wgs84)
bbox_extended["xmin"] <- bbox_extended["xmin"] - 0.02
bbox_extended["xmax"] <- bbox_extended["xmax"] + 0.02
bbox_extended["ymin"] <- bbox_extended["ymin"] - 0.02
bbox_extended["ymax"] <- bbox_extended["ymax"] + 0.02

bbox_vec <- as.vector(bbox_extended)
names(bbox_vec) <- c("xmin", "ymin", "xmax", "ymax")

# Query schools
Sys.sleep(1)
schools <- tryCatch({
  query <- opq(bbox = bbox_vec) %>%
    add_osm_feature(key = "amenity", value = "school") %>%
    osmdata_sf()
  
  points <- query$osm_points
  polygons <- query$osm_polygons
  
  if (!is.null(polygons) && nrow(polygons) > 0) {
    polygons <- st_centroid(polygons)
  }
  
  if (!is.null(points) && !is.null(polygons)) {
    rbind(points[, c("osm_id", "name", "geometry")],
          polygons[, c("osm_id", "name", "geometry")])
  } else if (!is.null(points)) {
    points[, c("osm_id", "name", "geometry")]
  } else if (!is.null(polygons)) {
    polygons[, c("osm_id", "name", "geometry")]
  } else {
    NULL
  }
}, error = function(e) {
  cat("  School query failed:", e$message, "\n")
  NULL
})

if (!is.null(schools)) {
  schools <- schools %>% filter(!is.na(name))
  
  # Categorize schools by name
  schools$school_type <- case_when(
    grepl("nursery|preschool|pre-school", schools$name, ignore.case = TRUE) ~ "nursery",
    grepl("primary|infant|junior|preparatory|prep school", schools$name, ignore.case = TRUE) ~ "primary",
    grepl("secondary|high school|academy|college|grammar|comprehensive", schools$name, ignore.case = TRUE) ~ "secondary",
    TRUE ~ "school"
  )
  
  cat("  Found", nrow(schools), "schools\n")
  cat("    -", sum(schools$school_type == "primary"), "primary\n")
  cat("    -", sum(schools$school_type == "secondary"), "secondary\n")
  cat("    -", sum(schools$school_type == "school"), "unclassified\n")
} else {
  cat("  No schools found\n")
}

# Query sixth form colleges
Sys.sleep(1)
sixth_forms <- tryCatch({
  query <- opq(bbox = bbox_vec) %>%
    add_osm_feature(key = "amenity", value = "college") %>%
    osmdata_sf()
  
  points <- query$osm_points
  polygons <- query$osm_polygons
  
  if (!is.null(polygons) && nrow(polygons) > 0) {
    polygons <- st_centroid(polygons)
  }
  
  if (!is.null(points) && !is.null(polygons)) {
    rbind(points[, c("osm_id", "name", "geometry")],
          polygons[, c("osm_id", "name", "geometry")])
  } else if (!is.null(points)) {
    points[, c("osm_id", "name", "geometry")]
  } else if (!is.null(polygons)) {
    polygons[, c("osm_id", "name", "geometry")]
  } else {
    NULL
  }
}, error = function(e) {
  cat("  Sixth form query failed:", e$message, "\n")
  NULL
})

if (!is.null(sixth_forms)) {
  cat("  Found", nrow(sixth_forms), "sixth form colleges\n")
}

# Query universities
Sys.sleep(1)
universities <- tryCatch({
  query <- opq(bbox = bbox_vec) %>%
    add_osm_feature(key = "building", value = "university") %>%
    osmdata_sf()
  
  points <- query$osm_points
  polygons <- query$osm_polygons
  
  if (!is.null(points) && nrow(points) > 0) {
    points <- points %>% filter(!is.na(name)) %>% select(osm_id, name, geometry)
  }
  
  if (!is.null(polygons) && nrow(polygons) > 0) {
    polygons <- polygons %>% filter(!is.na(name)) %>% st_centroid() %>% select(osm_id, name, geometry)
  }
  
  if (!is.null(points) && !is.null(polygons)) {
    rbind(points, polygons)
  } else if (!is.null(points)) {
    points
  } else if (!is.null(polygons)) {
    polygons
  } else {
    NULL
  }
}, error = function(e) {
  cat("  University query failed:", e$message, "\n")
  NULL
})

if (!is.null(universities)) {
  cat("  Found", nrow(universities), "universities\n")
}

# Query workplaces (non-residential buildings)
Sys.sleep(1)
workplaces <- tryCatch({
  query <- opq(bbox = bbox_vec) %>%
    add_osm_feature(key = "building") %>%
    osmdata_sf()
  
  buildings <- query$osm_polygons
  
  if (!is.null(buildings) && nrow(buildings) > 0 && "building" %in% names(buildings)) {
    # Exclude residential buildings
    excluded_types <- c("residential", "house", "detached", "semidetached_house", "terrace", 
                       "apartments", "bungalow", "cabin", "dwelling_house", "dormitory")
    buildings <- buildings %>% filter(!building %in% excluded_types)
    
    # Also exclude buildings used as homes
    home_building_ids <- unique(home_locations$home_osm_id)
    buildings <- buildings %>% filter(!osm_id %in% home_building_ids)
    
    cat("  Found", nrow(buildings), "workplace buildings\n")
    buildings
  } else {
    NULL
  }
}, error = function(e) {
  cat("  Workplace query failed:", e$message, "\n")
  NULL
})

# ============================================================================
# Assign Destinations
# ============================================================================

cat("\nAssigning destinations...\n")

all_destinations <- list()

# Transform destinations to OA CRS
if (!is.null(schools)) schools <- st_transform(schools, st_crs(oa_boundaries))
if (!is.null(sixth_forms)) sixth_forms <- st_transform(sixth_forms, st_crs(oa_boundaries))
if (!is.null(universities)) universities <- st_transform(universities, st_crs(oa_boundaries))
if (!is.null(workplaces)) workplaces <- st_transform(workplaces, st_crs(oa_boundaries))

# Assign schools to children aged 4-15
if (!is.null(schools) && nrow(schools) > 0) {
  children <- population %>%
    filter(assigned_age >= 4, assigned_age < 16) %>%
    left_join(home_locations, by = c("OA", "household_id_m"), relationship = "many-to-one") %>%
    filter(!is.na(home_longitude))
  
  if (nrow(children) > 0) {
    cat("  Processing", nrow(children), "children...\n")
    
    children_sf <- st_as_sf(children, coords = c("home_longitude", "home_latitude"), crs = 4326)
    children_sf <- st_transform(children_sf, crs = st_crs(oa_boundaries))
    
    # Separate primary (4-10) and secondary (11-15) aged children
    primary_children_idx <- which(children$assigned_age < 11)
    secondary_children_idx <- which(children$assigned_age >= 11)
    
    school_assignments <- list()
    
    # Assign primary-aged children
    if (length(primary_children_idx) > 0) {
      cat("    -", length(primary_children_idx), "primary-aged children...\n")
      primary_sf <- children_sf[primary_children_idx, ]
      
      # Use primary schools if available, otherwise all schools
      primary_schools <- schools %>% filter(school_type == "primary")
      target_schools <- if(nrow(primary_schools) > 0) primary_schools else schools
      
      nearest_idx <- st_nearest_feature(primary_sf, target_schools)
      nearest_schools <- target_schools[nearest_idx, ]
      
      school_coords <- st_transform(nearest_schools$geometry, crs = 4326)
      coords <- st_coordinates(school_coords)
      
      school_assignments[[1]] <- data.frame(
        OA = children$OA[primary_children_idx],
        household_id_m = children$household_id_m[primary_children_idx],
        resident_id_m = children$resident_id_m[primary_children_idx],
        destination_osm_id = nearest_schools$osm_id,
        destination_name = nearest_schools$name,
        destination_longitude = coords[, "X"],
        destination_latitude = coords[, "Y"],
        destination_type = "school",
        stringsAsFactors = FALSE
      )
    }
    
    # Assign secondary-aged children
    if (length(secondary_children_idx) > 0) {
      cat("    -", length(secondary_children_idx), "secondary-aged children...\n")
      secondary_sf <- children_sf[secondary_children_idx, ]
      
      # Use secondary schools if available, otherwise all schools
      secondary_schools <- schools %>% filter(school_type == "secondary")
      target_schools <- if(nrow(secondary_schools) > 0) secondary_schools else schools
      
      nearest_idx <- st_nearest_feature(secondary_sf, target_schools)
      nearest_schools <- target_schools[nearest_idx, ]
      
      school_coords <- st_transform(nearest_schools$geometry, crs = 4326)
      coords <- st_coordinates(school_coords)
      
      school_assignments[[2]] <- data.frame(
        OA = children$OA[secondary_children_idx],
        household_id_m = children$household_id_m[secondary_children_idx],
        resident_id_m = children$resident_id_m[secondary_children_idx],
        destination_osm_id = nearest_schools$osm_id,
        destination_name = nearest_schools$name,
        destination_longitude = coords[, "X"],
        destination_latitude = coords[, "Y"],
        destination_type = "school",
        stringsAsFactors = FALSE
      )
    }
    
    school_assignments <- bind_rows(school_assignments)
    
    all_destinations[[length(all_destinations) + 1]] <- school_assignments
    
    cat("  Assigned", nrow(school_assignments), "children to schools\n")
  }
}

# Assign sixth forms to 16-18 year olds in education
# Combine sixth form colleges with secondary schools (which typically have sixth forms)
if (!is.null(sixth_forms) && nrow(sixth_forms) > 0) {
  all_sixth_forms <- sixth_forms
  
  # Add secondary schools as they typically have sixth forms
  if (!is.null(schools) && nrow(schools) > 0) {
    secondary_schools <- schools %>% filter(school_type == "secondary")
    if (nrow(secondary_schools) > 0) {
      all_sixth_forms <- bind_rows(
        all_sixth_forms %>% select(osm_id, name, geometry),
        secondary_schools %>% select(osm_id, name, geometry)
      )
    }
  }
  
  cat("  Found", nrow(all_sixth_forms), "sixth form locations\n")
  
  # 16-18 year olds in education, but for 18-year-olds only include children/grandchildren
  sixth_formers <- population %>%
    filter(ns_sec_10a == 9) %>%
    filter(
      (assigned_age >= 16 & assigned_age < 18) |
      (assigned_age == 18 & role %in% c("Child", "Grandchild"))
    ) %>%
    left_join(home_locations, by = c("OA", "household_id_m"), relationship = "many-to-one") %>%
    filter(!is.na(home_longitude))
  
  if (nrow(sixth_formers) > 0) {
    sixth_formers_sf <- st_as_sf(sixth_formers, coords = c("home_longitude", "home_latitude"), crs = 4326)
    sixth_formers_sf <- st_transform(sixth_formers_sf, crs = st_crs(oa_boundaries))
    
    nearest_idx <- st_nearest_feature(sixth_formers_sf, all_sixth_forms)
    nearest_colleges <- all_sixth_forms[nearest_idx, ]
    
    college_coords <- st_transform(nearest_colleges$geometry, crs = 4326)
    coords <- st_coordinates(college_coords)
    
    all_destinations[[length(all_destinations) + 1]] <- data.frame(
      OA = sixth_formers$OA,
      household_id_m = sixth_formers$household_id_m,
      resident_id_m = sixth_formers$resident_id_m,
      destination_osm_id = nearest_colleges$osm_id,
      destination_name = nearest_colleges$name,
      destination_longitude = coords[, "X"],
      destination_latitude = coords[, "Y"],
      destination_type = "sixth_form",
      stringsAsFactors = FALSE
    )
    
    cat("  Assigned", nrow(sixth_formers), "sixth formers\n")
  }
} else {
  cat("  No sixth form locations found\n")
}

# Assign universities to 18+ in education
if (!is.null(universities) && nrow(universities) > 0) {
  students <- population %>%
    filter(ns_sec_10a == 9, assigned_age > 18) %>%
    left_join(home_locations, by = c("OA", "household_id_m"), relationship = "many-to-one") %>%
    filter(!is.na(home_longitude))
  
  if (nrow(students) > 0) {
    uni_coords <- st_transform(universities$geometry, crs = 4326)
    coords <- st_coordinates(uni_coords)
    
    uni_data <- data.frame(
      osm_id = universities$osm_id,
      name = universities$name,
      longitude = coords[, "X"],
      latitude = coords[, "Y"],
      stringsAsFactors = FALSE
    )
    
    set.seed(42)
    assigned_unis <- uni_data[sample(nrow(uni_data), nrow(students), replace = TRUE), ]
    
    all_destinations[[length(all_destinations) + 1]] <- data.frame(
      OA = students$OA,
      household_id_m = students$household_id_m,
      resident_id_m = students$resident_id_m,
      destination_osm_id = assigned_unis$osm_id,
      destination_name = assigned_unis$name,
      destination_longitude = assigned_unis$longitude,
      destination_latitude = assigned_unis$latitude,
      destination_type = "university",
      stringsAsFactors = FALSE
    )
    
    cat("  Assigned", nrow(students), "university students\n")
  }
}

# Assign workplaces to employed people
if (!is.null(workplaces) && nrow(workplaces) > 0) {
  # Get already-assigned people
  already_assigned <- bind_rows(all_destinations)
  assigned_keys <- if (nrow(already_assigned) > 0) {
    paste(already_assigned$OA, already_assigned$household_id_m, already_assigned$resident_id_m)
  } else {
    character(0)
  }
  
  workers <- population %>%
    filter(assigned_age >= 16, assigned_age <= 65, ns_sec_10a != 8, ns_sec_10a != 9) %>%
    left_join(home_locations, by = c("OA", "household_id_m"), relationship = "many-to-one") %>%
    filter(!is.na(home_longitude))
  
  if (length(assigned_keys) > 0) {
    worker_keys <- paste(workers$OA, workers$household_id_m, workers$resident_id_m)
    workers <- workers[!worker_keys %in% assigned_keys, ]
  }
  
  if (nrow(workers) > 0) {
    workplace_centroids <- st_centroid(workplaces$geometry)
    workplace_coords_wgs84 <- st_transform(workplace_centroids, crs = 4326)
    workplace_coords <- st_coordinates(workplace_coords_wgs84)
    
    workplace_data <- data.frame(
      osm_id = workplaces$osm_id,
      longitude = workplace_coords[, "X"],
      latitude = workplace_coords[, "Y"],
      stringsAsFactors = FALSE
    )
    
    set.seed(42)
    assigned_workplaces <- workplace_data[sample(nrow(workplace_data), nrow(workers), replace = TRUE), ]
    
    all_destinations[[length(all_destinations) + 1]] <- data.frame(
      OA = workers$OA,
      household_id_m = workers$household_id_m,
      resident_id_m = workers$resident_id_m,
      destination_osm_id = assigned_workplaces$osm_id,
      destination_name = NA_character_,
      destination_longitude = assigned_workplaces$longitude,
      destination_latitude = assigned_workplaces$latitude,
      destination_type = "workplace",
      stringsAsFactors = FALSE
    )
    
    cat("  Assigned", nrow(workers), "workers\n")
  }
}

# Combine all destinations
all_destination_assignments <- bind_rows(all_destinations)

# ============================================================================
# Combine and Save
# ============================================================================

cat("\nCombining home and destination locations...\n")

# Join homes with destinations
final_locations <- population %>%
  left_join(home_locations, by = c("OA", "household_id_m"), relationship = "many-to-one") %>%
  left_join(all_destination_assignments, by = c("OA", "household_id_m", "resident_id_m"), relationship = "many-to-one")

# Generate timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Save final population with all locations
output_file <- sprintf("data/outputs/02_generation/synthetic_population_with_locations_%s.csv", timestamp)
write_csv(final_locations, output_file)

cat("\nComplete! Saved to:", output_file, "\n")

# Summary statistics
cat("\nSummary:\n")
cat("  Total individuals:", nrow(final_locations), "\n")
cat("  With home locations:", sum(!is.na(final_locations$home_longitude)), "\n")
cat("  With destinations:", sum(!is.na(final_locations$destination_longitude)), "\n")
if (nrow(all_destination_assignments) > 0) {
  destination_summary <- all_destination_assignments %>%
    group_by(destination_type) %>%
    summarise(count = n(), .groups = "drop")
  for (i in 1:nrow(destination_summary)) {
    cat("    -", destination_summary$destination_type[i], ":", destination_summary$count[i], "\n")
  }
}


