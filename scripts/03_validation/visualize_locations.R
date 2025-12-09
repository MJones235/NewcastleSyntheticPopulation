# ============================================================================
# Visualize Population Locations
# ============================================================================
# Creates interactive maps showing home, school, and work/study locations
# for the synthetic population
# ============================================================================

library(tidyverse)
library(leaflet)
library(htmlwidgets)
library(yaml)

config <- yaml::read_yaml("config/pipeline_config.yml")

# Load most recent population file with locations
population_files <- list.files(
  path = file.path(config$data$outputs, config$outputs$generation$dir),
  pattern = "synthetic_population_with_locations_.*\\.csv",
  full.names = TRUE
)

if (length(population_files) == 0) {
  stop("No population file with locations found. Run 04_add_locations.R first.")
}

input_file <- sort(population_files, decreasing = TRUE)[1]
cat("Loading:", input_file, "\n")

population <- read_csv(input_file, show_col_types = FALSE)

# ============================================================================
# Summary Statistics
# ============================================================================

total_individuals <- nrow(population)
total_households <- nrow(distinct(population, OA, household_id_m))
individuals_with_home <- sum(!is.na(population$home_longitude))
households_with_home <- population %>% 
  filter(!is.na(home_longitude)) %>% 
  distinct(OA, household_id_m) %>% 
  nrow()

cat("\nTotal individuals:", total_individuals, "\n")
cat("Total households:", total_households, "\n")
cat("Individuals with home location:", individuals_with_home, 
    "(", round(100 * individuals_with_home / total_individuals, 1), "%)\n")
cat("Households with home location:", households_with_home, 
    "(", round(100 * households_with_home / total_households, 1), "%)\n")

children_with_school <- sum(!is.na(population$destination_osm_id) & 
                            population$destination_type == "school")
cat("Children with school location:", children_with_school, "\n")

people_with_destination <- sum(!is.na(population$destination_osm_id))
cat("People with destination (school/work/study):", people_with_destination, "\n")

if (households_with_home == 0) {
  stop("No households have locations. Cannot create visualizations.")
}

# ============================================================================
# Create Combined Overview Map
# ============================================================================

cat("\nCreating combined map with all locations...\n")

# Sample data for clearer visualization
set.seed(42)

# Sample households - show ALL of them, not just 200
household_locations <- population %>%
  filter(!is.na(home_longitude)) %>%
  distinct(OA, household_id_m, .keep_all = TRUE)

n_households <- nrow(household_locations)
cat("Showing", n_households, "household locations\n")

# Use all households for visualization
sample_households <- household_locations

# Get school locations
sample_schools <- if (children_with_school > 0) {
  population %>% 
    filter(!is.na(destination_osm_id), destination_type == "school") %>%
    group_by(destination_osm_id, destination_name, destination_longitude, destination_latitude) %>%
    summarise(
      num_children = n(),
      avg_age = round(mean(assigned_age, na.rm = TRUE), 1),
      .groups = "drop"
    )
} else NULL

# Get work/study locations (non-school destinations)
sample_work <- if (people_with_destination > children_with_school) {
  population %>%
    filter(!is.na(destination_osm_id), destination_type != "school") %>%
    mutate(destination_category = case_when(
      destination_type == "sixth_form" ~ "Sixth Form",
      destination_type == "university" ~ "University",
      destination_type == "workplace" ~ "Workplace",
      TRUE ~ "Other"
    )) %>%
    group_by(destination_osm_id, destination_name, destination_longitude, destination_latitude, destination_category) %>%
    summarise(
      num_people = n(),
      avg_age = round(mean(assigned_age, na.rm = TRUE), 1),
      .groups = "drop"
    )
} else NULL

combined_map <- leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  addCircleMarkers(
    data = sample_households,
    lng = ~home_longitude,
    lat = ~home_latitude,
    radius = 1,
    color = "blue",
    fillOpacity = 0.4,
    stroke = FALSE,
    group = "Homes",
    popup = ~paste0("<b>Home</b><br>OA: ", OA)
  )

if (!is.null(sample_schools)) {
  combined_map <- combined_map %>%
    addCircleMarkers(
      data = sample_schools,
      lng = ~destination_longitude,
      lat = ~destination_latitude,
      radius = ~sqrt(num_children) * 2,
      color = "red",
      fillOpacity = 0.7,
      stroke = TRUE,
      weight = 2,
      group = "Schools",
      popup = ~paste0("<b>School</b><br>", 
                     ifelse(!is.na(destination_name), paste0(destination_name, "<br>"), ""),
                     "Students: ", num_children, "<br>",
                     "Avg age: ", avg_age)
    )
}

if (!is.null(sample_work)) {
  work_colors <- colorFactor(
    palette = c("purple", "orange", "green"),
    domain = c("Sixth Form", "University", "Workplace")
  )
  
  combined_map <- combined_map %>%
    addCircleMarkers(
      data = sample_work,
      lng = ~destination_longitude,
      lat = ~destination_latitude,
      radius = ~sqrt(num_people) * 2,
      color = ~work_colors(destination_category),
      fillOpacity = 0.7,
      stroke = TRUE,
      weight = 2,
      group = "Work/Study",
      popup = ~paste0("<b>", destination_category, "</b><br>",
                     ifelse(!is.na(destination_name), paste0(destination_name, "<br>"), ""),
                     "People: ", num_people, "<br>",
                     "Avg age: ", avg_age)
    ) %>%
    addLegend(
      position = "bottomright",
      pal = work_colors,
      values = sample_work$destination_category,
      title = "Work/Study Type",
      opacity = 0.7
    )
}

combined_map <- combined_map %>%
  addLayersControl(
    overlayGroups = c("Homes", "Schools", "Work/Study"),
    options = layersControlOptions(collapsed = FALSE)
  )

output_dir <- file.path(config$data$outputs, config$outputs$generation$dir)
combined_map_file <- file.path(output_dir, "map_all_locations.html")
saveWidget(combined_map, combined_map_file, selfcontained = FALSE)

cat("Combined map saved:", combined_map_file, "\n")

# ============================================================================
# Summary
# ============================================================================

cat("\n", rep("=", 70), "\n", sep = "")
cat("VISUALIZATION COMPLETE\n")
cat(rep("=", 70), "\n", sep = "")
cat("\nGenerated map:", combined_map_file, "\n")
cat("Open this HTML file in a web browser to explore the locations.\n")
cat(rep("=", 70), "\n", sep = "")
