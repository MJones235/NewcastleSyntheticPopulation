#!/usr/bin/env Rscript
# Add role allocations to synthetic population based on household composition

library(dplyr)
library(readr)

# Function to assign roles within a household
assign_roles <- function(household_df) {
  # Add is_child flag (age category 1 = "Aged 15 years and under")
  household_df <- household_df %>%
    mutate(is_child = ifelse(resident_age_6a == 1, 1, 0))
  
  # Sort by adult/child status (adults first), then by age (oldest first)
  household_df <- household_df %>%
    arrange(is_child, desc(resident_age_6a))
  
  # Get household composition
  hh_comp_code <- household_df$hh_adults_and_children_8m[1]
  
  # Split into adults and children
  adults <- household_df %>% filter(is_child == 0)
  children <- household_df %>% filter(is_child == 1)
  
  n_adults <- nrow(adults)
  n_children <- nrow(children)
  
  # Initialize role column
  household_df$role <- ""
  
  # Rule 1: One Person Household (code 1)
  if (hh_comp_code == 1) {
    household_df$role[1] <- "Head"
  }
  
  # Rule 2: No adults, or one adult and one or more children (code 2)
  else if (hh_comp_code == 2) {
    household_df$role[household_df$is_child == 0] <- "Head"
    household_df$role[household_df$is_child == 1] <- "Child"
  }
  
  # Rule 3: Two adults: No children (code 3)
  # Two adults living together should almost never be Parent-Child - prefer Partner/Housemate
  else if (hh_comp_code == 3) {
    household_df$role[1] <- "Head"
    
    head_age <- adults$resident_age_6a[1]
    other_age <- adults$resident_age_6a[2]
    age_diff <- head_age - other_age
    
    # Strong preference for Partner over Child roles
    # Only assign Child/Grandchild in extreme age gaps (4+ bands = 40+ years)
    if (age_diff >= 4) {
      household_df$role[2] <- "Grandchild"
    } else if (adults$sex[1] != adults$sex[2]) {
      # Different sex - assume Partner
      household_df$role[2] <- "Partner"
    } else {
      # Same sex - Housemate
      household_df$role[2] <- "Housemate"
    }
  }
  
  # Rule 4: Two adults with one or more children (code 4)
  # Adult children only allowed if aged 16-24 (band 2), otherwise Housemate
  else if (hh_comp_code == 4) {
    household_df$role[1] <- "Head"
    household_df$role[2] <- "Partner"
    
    head_age <- adults$resident_age_6a[1]
    adult2_age <- adults$resident_age_6a[2]
    age_diff <- head_age - adult2_age
    
    # Check the age difference between the two adults
    if (age_diff >= 4) {
      household_df$role[2] <- "Grandchild"
    } else if (age_diff >= 2 && age_diff < 4) {
      # Only assign Child if aged 16-24 (band 2), otherwise Housemate
      if (adult2_age == 2) {
        household_df$role[2] <- "Child"
      } else {
        household_df$role[2] <- "Housemate"
      }
    }
    
    # Assign children
    for (i in 1:n_children) {
      household_df$role[n_adults + i] <- "Child"
    }
  }
  
  # Rule 5: Three or more adults with children (code 6)
  # Adult children only allowed if aged 16-24 (band 2), otherwise Housemate
  else if (hh_comp_code == 6) {
    household_df$role[1] <- "Head"
    household_df$role[household_df$is_child == 1] <- "Child"
    
    head_age <- adults$resident_age_6a[1]
    
    # Process remaining adults - apply age gap constraint
    for (i in 2:n_adults) {
      adult_age <- adults$resident_age_6a[i]
      age_diff <- head_age - adult_age
      
      # Assign based on age gap
      if (age_diff >= 4) {
        # 4+ bands = Grandchild
        household_df$role[i] <- "Grandchild"
      } else if (age_diff >= 2 && age_diff < 4) {
        # 2-3 bands = Child only if aged 16-24 (band 2), otherwise Housemate
        if (adult_age == 2) {
          household_df$role[i] <- "Child"
        } else {
          household_df$role[i] <- "Housemate"
        }
      }
    }
    
    # Assign Partner to first adult within 1 age band of head (if any)
    partner_assigned <- FALSE
    for (i in 2:n_adults) {
      if (household_df$role[i] == "") {
        adult_age <- adults$resident_age_6a[i]
        age_diff <- head_age - adult_age
        
        if (age_diff <= 1 && !partner_assigned) {
          household_df$role[i] <- "Partner"
          partner_assigned <- TRUE
        } else {
          household_df$role[i] <- "Housemate"
        }
      }
    }
  }
  
  # Rule 6: Three or more adults with no children (code 7)
  # Adult children only allowed if aged 16-24 (band 2), otherwise Housemate
  else if (hh_comp_code == 7) {
    household_df$role[1] <- "Head"
    
    head_age <- adults$resident_age_6a[1]
    all_ages <- adults$resident_age_6a
    max_gap <- head_age - min(all_ages)
    
    if (max_gap <= 1) {
      # All within 1 age band - all others are housemates
      for (i in 2:n_adults) {
        household_df$role[i] <- "Housemate"
      }
    } else {
      # Gap of 2+ bands
      # First pass: identify candidates and assign Partner
      partner_assigned <- FALSE
      
      for (i in 2:n_adults) {
        adult_age <- adults$resident_age_6a[i]
        age_diff <- head_age - adult_age
        
        # Assign Partner first (within 1 age band)
        if (age_diff <= 1 && !partner_assigned) {
          household_df$role[i] <- "Partner"
          partner_assigned <- TRUE
        }
      }
      
      # Second pass: assign adult children/grandchildren or Housemates
      for (i in 2:n_adults) {
        if (household_df$role[i] != "") next  # Skip if already assigned
        
        adult_age <- adults$resident_age_6a[i]
        age_diff <- head_age - adult_age
        
        if (age_diff >= 4) {
          # Grandchild (4+ bands gap)
          household_df$role[i] <- "Grandchild"
        } else if (age_diff >= 2 && age_diff < 4) {
          # Child only if aged 16-24 (band 2), otherwise Housemate
          if (adult_age == 2) {
            household_df$role[i] <- "Child"
          } else {
            household_df$role[i] <- "Housemate"
          }
        } else {
          # Everyone else is Housemate
          household_df$role[i] <- "Housemate"
        }
      }
    }
  }
  
  # Post-processing: Handle households with Grandchildren
  if (any(household_df$role == "Grandchild")) {
    # Find the head's age band
    head_idx <- which(household_df$role == "Head")[1]
    if (length(head_idx) > 0) {
      head_age <- household_df$resident_age_6a[head_idx]
      
      # Find all grandchildren
      grandchild_indices <- which(household_df$role == "Grandchild")
      
      for (gc_idx in grandchild_indices) {
        gc_age <- household_df$resident_age_6a[gc_idx]
        
        # Look for adults who sit between head and grandchild (the parent generation)
        for (i in seq_len(nrow(household_df))) {
          if (i == head_idx || i == gc_idx) next
          if (household_df$role[i] %in% c("Partner", "Grandchild")) next  # Don't change Partner or other Grandchildren
          
          person_age <- household_df$resident_age_6a[i]
          age_gap_from_head <- head_age - person_age
          age_gap_to_gc <- person_age - gc_age
          
          # If this person is between head and grandchild (2-3 bands from head, 2+ bands from grandchild)
          if (age_gap_from_head >= 2 && age_gap_from_head < 4 && age_gap_to_gc >= 2) {
            household_df$role[i] <- "Child"
          }
        }
        
        # If grandchild is actually a child (under 16) and no parent is present, keep as Grandchild
        # This handles the case where children under 16 should be grandchildren
        if (household_df$is_child[gc_idx] == 1) {
          # Check if there's a parent generation present
          has_parent <- any(household_df$role == "Child" & household_df$resident_age_6a > 1)
          if (!has_parent) {
            # Keep as Grandchild - this is correct
          }
        }
      }
    }
  }
  
  # Final pass: ensure no blank roles remain
  for (i in seq_len(nrow(household_df))) {
    if (household_df$role[i] == "") {
      household_df$role[i] <- "Housemate"
    }
  }
  
  return(household_df)
}

# Main execution
cat("Loading synthetic population...\n")

# Find the most recent synthetic population file
gen_dir <- "data/outputs/02_generation"
pop_files <- list.files(gen_dir, pattern = "^synthetic_population_.*\\.csv$", full.names = TRUE)

if (length(pop_files) == 0) {
  stop("No synthetic population file found in ", gen_dir)
}

# Use the most recent file
pop_file <- pop_files[length(pop_files)]
cat("Using file:", basename(pop_file), "\n")

pop_df <- read_csv(pop_file, show_col_types = FALSE)

cat("Loaded", nrow(pop_df), "individuals\n")
cat("Processing households...\n\n")

# Process each household and assign roles
# Group by both OA and household_id_m to get unique households
pop_with_roles <- pop_df %>%
  group_by(OA, household_id_m) %>%
  group_modify(~ assign_roles(.x)) %>%
  ungroup() %>%
  select(-is_child)  # Remove temporary is_child column

# Generate output filename with timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_file <- file.path(gen_dir, paste0("synthetic_population_with_roles_", timestamp, ".csv"))

# Save to new file
write_csv(pop_with_roles, output_file)

cat("Saved population with roles to:", basename(output_file), "\n\n")

# Display role distribution
cat("Role distribution:\n")
print(table(pop_with_roles$role))

cat("\nRole distribution by household composition:\n")
role_by_hh <- pop_with_roles %>%
  group_by(hh_adults_and_children_8m, role) %>%
  summarise(count = n(), .groups = "drop") %>%
  arrange(hh_adults_and_children_8m, role)
print(role_by_hh, n = 50)

# Show sample households
cat("\nSample households (first 5):\n")
sample_households <- pop_with_roles %>%
  arrange(household_id_m) %>%
  group_by(household_id_m) %>%
  slice(1:10) %>%
  ungroup() %>%
  select(household_id_m, resident_age_6a, sex, hh_adults_and_children_8m, role) %>%
  head(30)
print(sample_households, n = 30)

cat("\nDone!\n")
