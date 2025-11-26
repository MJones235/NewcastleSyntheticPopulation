#!/usr/bin/env Rscript
# Assign single-year ages to synthetic population
# Respects household relationships and aligns with MSOA single-year age-sex distribution

library(data.table)
library(readr)

cat("==============================================\n")
cat("SINGLE-YEAR AGE ASSIGNMENT\n")
cat("==============================================\n\n")

# --------------------------
# 0) LOAD DATA
# --------------------------
cat("Loading data...\n")

# Find most recent population with roles
gen_dir <- "data/outputs/02_generation"
pop_files <- list.files(gen_dir, pattern = "^synthetic_population_with_roles_.*\\.csv$", full.names = TRUE)
if (length(pop_files) == 0) stop("No synthetic population with roles file found")
pop_file <- pop_files[length(pop_files)]
cat("  Population file:", basename(pop_file), "\n")

pop <- fread(pop_file)

# Load MSOA single-year age-sex distribution
msoa_age <- fread("data/processed/msoa_age_by_sex.csv")
cat("  MSOA age-sex data loaded\n")

# Load relationship priors
partner_gap <- fread("data/processed/age_difference.csv")
mother_aab <- fread("data/processed/mother_age_at_birth.csv")
father_aab <- fread("data/processed/father_age_at_birth.csv")
cat("  Relationship priors loaded\n\n")

# --------------------------
# 1) PREPARE PRIORS
# --------------------------
cat("Preparing prior distributions...\n")

# Normalize probability distributions
normalize <- function(dt, pcol) {
  dt[[pcol]][is.na(dt[[pcol]])] <- 0
  s <- sum(dt[[pcol]])
  if (s == 0) dt[[pcol]] <- 0 else dt[[pcol]] <- dt[[pcol]] / s
  dt
}

# Partner age gap: age_difference column = Partner age - Head age
setnames(partner_gap, c("age_difference", "percentage"), c("gap", "prob"))
partner_gap[, prob := prob / 100]  # Convert percentage to probability
partner_gap <- normalize(partner_gap, "prob")
setkey(partner_gap, gap)

# Mother age at birth: spread bins uniformly across single years
mother_aab_dist <- data.table(
  age_min = c(12, 20, 25, 30, 35, 40, 45),
  age_max = c(19, 24, 29, 34, 39, 44, 55),
  prob_total = c(2.436965907251605, 12.05848057529242, 26.01450783363558, 
                 34.21711541193064, 20.115250071845487, 4.739159589473233, 
                 0.41852061057103124)
)

# Expand to single years with uniform distribution within each bin
mother_aab <- mother_aab_dist[, {
  years <- seq(age_min, age_max)
  prob_per_year <- prob_total / length(years)
  .(age_at_birth = years, prob = prob_per_year)
}, by = 1:nrow(mother_aab_dist)]
mother_aab <- mother_aab[, .(age_at_birth, prob)]
mother_aab[, prob := prob / sum(prob)]  # Normalize to sum to 1
setkey(mother_aab, age_at_birth)

# Father age at birth: spread bins uniformly across single years
father_aab_dist <- data.table(
  age_min = c(12, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65),
  age_max = c(19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 75),
  prob_total = c(1.0378290873530756, 6.915502474288761, 19.040299033733355,
                 32.29394098877755, 24.017362576664738, 10.542259184570305,
                 3.4419449698725595, 1.1707059495832184, 0.3770923763420129,
                 0.11446516628714255, 0.0371707823754909)
)

# Expand to single years with uniform distribution within each bin
father_aab <- father_aab_dist[, {
  years <- seq(age_min, age_max)
  prob_per_year <- prob_total / length(years)
  .(age_at_birth = years, prob = prob_per_year)
}, by = 1:nrow(father_aab_dist)]
father_aab <- father_aab[, .(age_at_birth, prob)]
father_aab[, prob := prob / sum(prob)]  # Normalize to sum to 1
setkey(father_aab, age_at_birth)

# Lookup functions
p_partner_gap <- function(g) {
  val <- partner_gap[list(g)]$prob
  ifelse(is.na(val) | length(val) == 0, 1e-9, val)
}

p_age_at_birth <- function(parent_sex, aab) {
  # Handle NA or out of bounds values
  if (is.na(aab) || aab < 12 || aab > 75) return(1e-12)
  if (parent_sex == 1) {  # Female
    val <- mother_aab[list(aab)]$prob
  } else {  # Male (sex == 2)
    val <- father_aab[list(aab)]$prob
  }
  ifelse(is.na(val) | length(val) == 0, 1e-9, val)
}

cat("  Prior distributions prepared\n\n")

# --------------------------
# 2) MAP BROAD BANDS TO RANGES
# --------------------------
band_range <- function(code) {
  switch(
    as.character(code),
    "1" = c(0L, 15L),
    "2" = c(16L, 24L),
    "3" = c(25L, 34L),
    "4" = c(35L, 49L),
    "5" = c(50L, 64L),
    "6" = c(65L, 100L)
  )
}

# --------------------------
# 3) PREPARE MSOA QUOTAS
# --------------------------
cat("Preparing MSOA single-year age quotas...\n")

# Extract the MSOA code (assuming all OAs in same MSOA for now)
msoa_code <- unique(msoa_age$`Middle layer Super Output Areas Code`)[1]
cat("  Using MSOA:", msoa_code, "\n")

# Prepare quota table: MSOA, sex, age, original count, remaining quota
msoa_quota <- msoa_age[, .(
  MSOA = `Middle layer Super Output Areas Code`,
  age = as.integer(`Age (101 categories) Code`),
  sex = as.integer(`Sex (2 categories) Code`),  # 1=Female, 2=Male
  n = as.integer(Observation)
)]
msoa_quota <- msoa_quota[!is.na(MSOA) & !is.na(age) & !is.na(sex) & age >= 0 & age <= 100]
msoa_quota[, remaining := n]
setkey(msoa_quota, MSOA, sex, age)

cat("  Quota table prepared:", nrow(msoa_quota), "age-sex combinations\n")
cat("  Total individuals in MSOA:", sum(msoa_quota$n), "\n")
cat("  Total individuals in population:", nrow(pop), "\n\n")

# --------------------------
# 4) HELPER FUNCTIONS
# --------------------------

# Pick an age within [a_min, a_max] for given sex, balancing quota needs with randomness
pick_age_from_quota <- function(sex_val, a_min, a_max, quota_dt) {
  rng <- a_min:a_max
  # Get all ages within band
  band <- quota_dt[.(msoa_code, sex_val)][age %in% rng]
  
  if (nrow(band) > 0L) {
    # Score based on remaining quota with randomness
    # Use log to avoid extreme preference for high quotas
    band[, score := -log(pmax(remaining, 0.5)) + runif(.N, 0, 2)]
    setorder(band, score, age)
    chosen <- band$age[1L]
    quota_dt[.(msoa_code, sex_val, chosen), remaining := remaining - 1L]
    return(chosen)
  }
  
  # Fallback: just pick midpoint of band
  return(as.integer((a_min + a_max) / 2))
}

# Stable ordering for deterministic assignment
stable_order <- function(dt) {
  setorder(dt, OA, household_id_m, -resident_age_6a, role, resident_id_m)
}

# --------------------------
# 5) ASSIGN SINGLE-YEAR AGES
# --------------------------
cat("Assigning single-year ages...\n\n")

pop[, assigned_age := NA_integer_]
pop[, sex := as.integer(sex)]
stable_order(pop)

# 5.1) HEADS
cat("  Processing Heads...\n")
heads <- pop[role == "Head"]
cat("    Total heads:", nrow(heads), "\n")

for (i in seq_len(nrow(heads))) {
  r <- heads[i]
  br <- band_range(r$resident_age_6a)
  a <- pick_age_from_quota(r$sex, br[1], br[2], msoa_quota)
  pop[OA == r$OA & household_id_m == r$household_id_m & resident_id_m == r$resident_id_m, 
      assigned_age := a]
}

# 5.2) PARTNERS
cat("  Processing Partners...\n")
# Get head age and sex for gap calculation
head_lookup <- pop[role == "Head", 
                   .(head_age = assigned_age[1L], head_sex = sex[1L]), 
                   by = .(OA, household_id_m)]

partners <- merge(pop[role == "Partner"], head_lookup, 
                 by = c("OA", "household_id_m"), all.x = TRUE)
cat("    Total partners:", nrow(partners), "\n")

if (nrow(partners) > 0) {
  for (i in seq_len(nrow(partners))) {
    r <- partners[i]
    br <- band_range(r$resident_age_6a)
    rng <- br[1]:br[2]
    
    # Get candidate ages within band
    slice <- msoa_quota[.(msoa_code, r$sex)][age %in% rng]
    if (nrow(slice) == 0) slice <- msoa_quota[.(msoa_code, r$sex)]
    
    # Calculate partner gap penalty (always Male age - Female age)
    # Determine which is male and which is female
    if (r$sex == 2) {
      # Partner is male, head is female
      slice[, gap := age - r$head_age]
    } else {
      # Partner is female, head is male
      slice[, gap := r$head_age - age]
    }
    slice[, pgap := vapply(gap, p_partner_gap, numeric(1))]
    
    # Score: balance prior with quota and randomness
    # Reduced weight on prior (0.5x), added randomness to spread distribution
    slice[, score := 0.5 * -log(pgap) - log(pmax(remaining, 0.5)) + runif(.N, 0, 3)]
    setorder(slice, score, age)
    
    chosen <- slice$age[1L]
    msoa_quota[.(msoa_code, r$sex, chosen), remaining := remaining - 1L]
    pop[OA == r$OA & household_id_m == r$household_id_m & resident_id_m == r$resident_id_m,
        assigned_age := chosen]
  }
}

# 5.3) CHILDREN
cat("  Processing Children...\n")

# Identify parent(s): get both mother and father ages when available
hh <- pop[role %in% c("Head", "Partner"), .SD, by = .(OA, household_id_m)]
parent_info <- hh[, {
  female_parent <- .SD[sex == 1]
  male_parent <- .SD[sex == 2]
  
  mother_age <- if (nrow(female_parent) > 0) female_parent$assigned_age[1] else NA_integer_
  father_age <- if (nrow(male_parent) > 0) male_parent$assigned_age[1] else NA_integer_
  
  # If only one parent, use that one's age and sex
  if (is.na(mother_age) && !is.na(father_age)) {
    .(parent_sex = 2L, parent_age = father_age, mother_age = NA_integer_, father_age = father_age)
  } else if (!is.na(mother_age) && is.na(father_age)) {
    .(parent_sex = 1L, parent_age = mother_age, mother_age = mother_age, father_age = NA_integer_)
  } else {
    # Both parents: prefer mother for primary scoring
    .(parent_sex = 1L, parent_age = mother_age, mother_age = mother_age, father_age = father_age)
  }
}, by = .(OA, household_id_m)]

children <- merge(pop[role == "Child"], parent_info, 
                 by = c("OA", "household_id_m"), all.x = TRUE)
cat("    Total children:", nrow(children), "\n")

if (nrow(children) > 0) {
  # Sort children: older bands first for spacing
  setorder(children, OA, household_id_m, -resident_age_6a, resident_id_m)
  
  # Track all sibling ages per household for spacing
  sibling_ages <- new.env(parent = emptyenv())
  
  for (i in seq_len(nrow(children))) {
    r <- children[i]
    br <- band_range(r$resident_age_6a)
    rng <- br[1]:br[2]
    
    slice <- msoa_quota[.(msoa_code, r$sex)][age %in% rng]
    if (nrow(slice) == 0) slice <- msoa_quota[.(msoa_code, r$sex)]
    
    # Age at birth penalty using BOTH parents if available
    slice[, child_age := age]

    # Calculate probability for both mother and father (if present)
    # Apply hard minimum age cutoff: parent must be at least 16 at birth
    if (!is.na(r$mother_age)) {
      slice[, mother_aab := r$mother_age - child_age]
      slice[, p_mother := vapply(mother_aab, function(x) {
        if (x < 18) return(0)  # Hard cutoff
        p_age_at_birth(1L, x)
      }, numeric(1))]
    } else {
      slice[, p_mother := 1.0]  # No penalty if no mother
    }

    if (!is.na(r$father_age)) {
      slice[, father_aab := r$father_age - child_age]
      slice[, p_father := vapply(father_aab, function(x) {
        if (x < 18) return(0)  # Hard cutoff
        p_age_at_birth(2L, x)
      }, numeric(1))]
    } else {
      slice[, p_father := 1.0]  # No penalty if no father
    }

    # Combined probability: both parents must have realistic ages
    # Use product of probabilities (multiply), weighted toward mother
    slice[, pp := (p_mother^0.7) * (p_father^0.3)]
    
    # Sibling spacing: prefer 1-4 year gaps to any existing sibling
    key_id <- paste0(r$OA, ":", r$household_id_m)
    existing_ages <- if (exists(key_id, envir = sibling_ages)) get(key_id, envir = sibling_ages) else integer(0)
    
    if (length(existing_ages) > 0) {
      # Calculate minimum absolute gap to any existing sibling
      slice[, min_gap := sapply(age, function(a) min(abs(existing_ages - a)))]
      
      # Very strong penalties to enforce realistic sibling spacing
      slice[, sib_pen := fcase(
        min_gap == 0, 500,                      # Twins: extremely strong penalty
        min_gap == 1, 1,                        # 1 year gap: small penalty (uncommon but possible)
        min_gap %in% 2:3, 0,                    # 2-3 year gaps: ideal spacing
        min_gap == 4, 2,                        # 4 years: still good
        min_gap %in% 5:6, 50,                   # 5-6 years: strong penalty
        min_gap %in% 7:10, 100,                 # 7-10 years: very strong penalty
        min_gap > 10, 200                       # 10+ years: massive penalty
      )]
    } else {
      slice[, sib_pen := 0]
    }
    
    # Total score: use -log(probability) for age-at-birth, plus quota and sibling penalties
    # Higher weight on age-at-birth prior (3.0x) to strongly enforce realistic parent ages
    slice[, score := 3.0 * -log(pp) - log(pmax(remaining, 0.5)) + sib_pen + runif(.N, 0, 1)]
    setorder(slice, score, age)
    
    chosen <- slice$age[1L]
    msoa_quota[.(msoa_code, r$sex, chosen), remaining := remaining - 1L]
    pop[OA == r$OA & household_id_m == r$household_id_m & resident_id_m == r$resident_id_m,
        assigned_age := chosen]
    
    # Add this child's age to the household's sibling list
    existing_ages <- if (exists(key_id, envir = sibling_ages)) get(key_id, envir = sibling_ages) else integer(0)
    assign(key_id, c(existing_ages, chosen), envir = sibling_ages)
  }
}

# 5.4) GRANDCHILDREN
cat("  Processing Grandchildren...\n")

# Use head as grandparent proxy
grandchildren <- merge(pop[role == "Grandchild"], head_lookup, 
                       by = c("OA", "household_id_m"), all.x = TRUE)
cat("    Total grandchildren:", nrow(grandchildren), "\n")

if (nrow(grandchildren) > 0) {
  # Grandparent age at grandchild birth typically 45-70 years
  # We'll use a simple scoring based on realistic age gaps
  
  for (i in seq_len(nrow(grandchildren))) {
    r <- grandchildren[i]
    br <- band_range(r$resident_age_6a)
    rng <- br[1]:br[2]
    
    slice <- msoa_quota[.(msoa_code, r$sex)][age %in% rng]
    if (nrow(slice) == 0) slice <- msoa_quota[.(msoa_code, r$sex)]
    
    # Grandparent age at birth = current grandparent age - grandchild age
    # Realistic range: 45-75 years at grandchild birth
    slice[, gpab := r$head_age - age]  # Grandparent age at birth
    
    # Penalty for unrealistic grandparent ages at birth
    # Ideal range: 50-65, acceptable: 45-75, outside: very high penalty
    slice[, gp_penalty := fcase(
      gpab < 40, 100,                    # Too young to be grandparent
      gpab >= 40 & gpab < 45, 20,        # Young grandparent
      gpab >= 45 & gpab < 50, 5,         # Acceptable
      gpab >= 50 & gpab <= 65, 0,        # Ideal range
      gpab > 65 & gpab <= 75, 5,         # Older grandparent
      gpab > 75 & gpab <= 85, 20,        # Very old grandparent
      gpab > 85, 100                     # Unrealistic
    )]
    
    # Score: quota pressure + grandparent penalty + randomness
    slice[, score := -log(pmax(remaining, 0.5)) + gp_penalty + runif(.N, 0, 2)]
    setorder(slice, score, age)
    
    chosen <- slice$age[1L]
    msoa_quota[.(msoa_code, r$sex, chosen), remaining := remaining - 1L]
    pop[OA == r$OA & household_id_m == r$household_id_m & resident_id_m == r$resident_id_m,
        assigned_age := chosen]
  }
}

# 5.5) HOUSEMATES
cat("  Processing Housemates...\n")
housemates <- pop[role == "Housemate"]
cat("    Total housemates:", nrow(housemates), "\n")

if (nrow(housemates) > 0) {
  housemates <- merge(housemates, head_lookup, 
                     by = c("OA", "household_id_m"), all.x = TRUE)
  setorder(housemates, OA, household_id_m, resident_id_m)
  
  for (i in seq_len(nrow(housemates))) {
    r <- housemates[i]
    br <- band_range(r$resident_age_6a)
    rng <- br[1]:br[2]
    
    slice <- msoa_quota[.(msoa_code, r$sex)][age %in% rng]
    if (nrow(slice) == 0) slice <- msoa_quota[.(msoa_code, r$sex)]
    
    # Prefer ages close to head's age (housemates are typically similar ages)
    # Penalty increases with age difference (squared for stronger effect)
    slice[, age_diff := abs(age - r$head_age)]
    slice[, score := (age_diff^2) / 50 - log(pmax(remaining, 0.5))]
    setorder(slice, score, age)
    
    chosen <- slice$age[1L]
    msoa_quota[.(msoa_code, r$sex, chosen), remaining := remaining - 1L]
    pop[OA == r$OA & household_id_m == r$household_id_m & resident_id_m == r$resident_id_m,
        assigned_age := chosen]
  }
}

# Check for any unassigned
if (pop[is.na(assigned_age), .N] > 0L) {
  warning("Some ages remain unassigned!")
}

cat("\n")

# --------------------------
# 6) SAVE RESULTS
# --------------------------
cat("Saving results...\n")

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_file <- file.path(gen_dir, paste0("synthetic_population_with_ages_", timestamp, ".csv"))

fwrite(pop, output_file)

cat("  Saved to:", basename(output_file), "\n\n")