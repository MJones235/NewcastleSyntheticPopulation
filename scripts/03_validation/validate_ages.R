#!/usr/bin/env Rscript
# Validation script for synthetic population with ages
# Compares synthetic distributions to target data

library(data.table)
library(ggplot2)
library(gridExtra)

cat("==============================================\n")
cat("SYNTHETIC POPULATION VALIDATION\n")
cat("==============================================\n\n")

# --------------------------
# 1) LOAD DATA
# --------------------------
cat("Loading data...\n")

# Find most recent population with ages
gen_dir <- "data/outputs/02_generation"
pop_files <- list.files(gen_dir, pattern = "^synthetic_population_with_ages_.*\\.csv$", full.names = TRUE)
if (length(pop_files) == 0) stop("No synthetic population with ages file found")
pop_file <- pop_files[length(pop_files)]
cat("  Population file:", basename(pop_file), "\n")

pop <- fread(pop_file)

# Load target distributions
msoa_age <- fread("data/processed/msoa_age_by_sex.csv")
partner_gap <- fread("data/processed/age_difference.csv")
mother_aab <- fread("data/processed/mother_age_at_birth.csv")
father_aab <- fread("data/processed/father_age_at_birth.csv")

cat("  Target distributions loaded\n\n")

# MSOA code used for household-level targets
msoa_code <- unique(msoa_age$`Middle layer Super Output Areas Code`)[1]

# Create output directory for plots
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
output_dir <- file.path("data/outputs/03_validation", paste0("validation_plots_", timestamp))
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
cat("  Output directory:", output_dir, "\n\n")

# --------------------------
# 2) MSOA AGE-SEX DISTRIBUTION
# --------------------------
cat("Validating MSOA age-sex distribution...\n")

# Prepare target data
msoa_target <- msoa_age[, .(
  age = as.integer(`Age (101 categories) Code`),
  sex = as.integer(`Sex (2 categories) Code`),
  target = as.integer(Observation)
)]

# Prepare synthetic data
msoa_synthetic <- pop[, .N, by = .(age = assigned_age, sex)]
setnames(msoa_synthetic, "N", "synthetic")

# Merge
msoa_comparison <- merge(msoa_target, msoa_synthetic, by = c("age", "sex"), all.x = TRUE)
msoa_comparison[is.na(synthetic), synthetic := 0]
msoa_comparison[, diff := synthetic - target]
msoa_comparison[, sex_label := fifelse(sex == 1, "Female", "Male")]

# Statistics
cat("  Total target:", sum(msoa_comparison$target), "\n")
cat("  Total synthetic:", sum(msoa_comparison$synthetic), "\n")
cat("  Difference:", sum(msoa_comparison$diff), "\n")
cat("  RMSE:", round(sqrt(mean(msoa_comparison$diff^2)), 2), "\n")
cat("  Max overcount:", max(msoa_comparison$diff), "\n")
cat("  Max undercount:", min(msoa_comparison$diff), "\n\n")

# Plot 1: Age-Sex Pyramid Comparison
p1 <- ggplot(msoa_comparison, aes(x = age)) +
  geom_line(aes(y = target, color = "Target"), size = 0.8) +
  geom_line(aes(y = synthetic, color = "Synthetic"), size = 0.8, alpha = 0.7) +
  facet_wrap(~ sex_label, ncol = 1) +
  scale_color_manual(values = c("Target" = "blue", "Synthetic" = "red")) +
  labs(title = "MSOA Age-Sex Distribution: Target vs Synthetic",
       x = "Age", y = "Count", color = "") +
  theme_bw() +
  theme(legend.position = "top")

ggsave(file.path(output_dir, "01_age_sex_distribution.png"), p1, 
       width = 10, height = 6, dpi = 300)

# Plot 2: Difference (Residuals)
p2 <- ggplot(msoa_comparison, aes(x = age, y = diff)) +
  geom_col(aes(fill = diff > 0), width = 1) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  facet_wrap(~ sex_label, ncol = 1) +
  scale_fill_manual(values = c("TRUE" = "darkred", "FALSE" = "darkblue"),
                    labels = c("Undercount", "Overcount")) +
  labs(title = "Age-Sex Distribution: Residuals (Synthetic - Target)",
       x = "Age", y = "Difference", fill = "") +
  theme_bw() +
  theme(legend.position = "top")

ggsave(file.path(output_dir, "02_age_sex_residuals.png"), p2, 
       width = 10, height = 6, dpi = 300)

# --------------------------
# 3) PARTNER AGE GAPS
# --------------------------
cat("Validating partner age gaps...\n")

# Calculate synthetic partner gaps (Male age - Female age)
partners <- pop[role %in% c("Head", "Partner")]
partner_gaps_synth <- partners[, {
  if (.N == 2) {
    male_age <- .SD[sex == 2, assigned_age]
    female_age <- .SD[sex == 1, assigned_age]
    if (length(male_age) == 1 && length(female_age) == 1) {
      .(gap = male_age - female_age)
    }
  }
}, by = .(OA, household_id_m)]

# Prepare target data
partner_target <- partner_gap[, .(
  gap = age_difference,
  target_pct = percentage
)]

# Prepare synthetic data
partner_synth_summary <- partner_gaps_synth[, .N, by = gap]
partner_synth_summary[, synthetic_pct := N / sum(N) * 100]

# Merge
partner_comparison <- merge(partner_target, partner_synth_summary[, .(gap, synthetic_pct)], 
                            by = "gap", all = TRUE)
partner_comparison[is.na(target_pct), target_pct := 0]
partner_comparison[is.na(synthetic_pct), synthetic_pct := 0]

# Statistics
cat("  Synthetic partner pairs:", nrow(partner_gaps_synth), "\n")
cat("  Mean gap (synthetic):", round(mean(partner_gaps_synth$gap), 2), "years\n")
cat("  SD gap (synthetic):", round(sd(partner_gaps_synth$gap), 2), "years\n")
cat("  Median gap (synthetic):", median(partner_gaps_synth$gap), "years\n\n")

# Plot 3: Partner Age Gap Distribution
p3 <- ggplot(partner_comparison[gap >= -15 & gap <= 20], aes(x = gap)) +
  geom_line(aes(y = target_pct, color = "Target"), size = 1) +
  geom_line(aes(y = synthetic_pct, color = "Synthetic"), size = 1, alpha = 0.7) +
  geom_vline(xintercept = 0, linetype = "dashed", alpha = 0.5) +
  scale_color_manual(values = c("Target" = "blue", "Synthetic" = "red")) +
  labs(title = "Partner Age Gap Distribution (Partner Age - Head Age)",
       subtitle = "Negative = Female older, Positive = Male older",
       x = "Age Gap (years)", y = "Percentage", color = "") +
  theme_bw() +
  theme(legend.position = "top")

ggsave(file.path(output_dir, "03_partner_age_gaps.png"), p3, 
       width = 10, height = 6, dpi = 300)

# --------------------------
# 4) MOTHER AGE AT BIRTH
# --------------------------
cat("Validating mother age at birth...\n")

# Calculate synthetic mother ages at birth
# Find households with female parent and children
hh_parents <- pop[role %in% c("Head", "Partner"), .SD, by = .(OA, household_id_m)]
mother_data <- hh_parents[, {
  mothers <- .SD[sex == 1]  # Female parents
  if (nrow(mothers) > 0) {
    mother_age <- mothers[order(-assigned_age)][1, assigned_age]
    .(mother_age = mother_age)
  }
}, by = .(OA, household_id_m)]

children_with_mothers <- merge(
  pop[role == "Child", .(OA, household_id_m, child_age = assigned_age)],
  mother_data,
  by = c("OA", "household_id_m")
)
children_with_mothers[, age_at_birth := mother_age - child_age]

# Prepare target data (convert bins to midpoints for comparison)
mother_target <- data.table(
  age_at_birth = c(18, 22, 27, 32, 37, 42, 47),
  age_range = c("Under 20", "20-24", "25-29", "30-34", "35-39", "40-44", "45+"),
  target_pct = mother_aab$percentage
)

# Prepare synthetic data (binned)
mother_synth_binned <- children_with_mothers[age_at_birth >= 12 & age_at_birth <= 55, 
  .(age_range = fcase(
    age_at_birth < 20, "Under 20",
    age_at_birth < 25, "20-24",
    age_at_birth < 30, "25-29",
    age_at_birth < 35, "30-34",
    age_at_birth < 40, "35-39",
    age_at_birth < 45, "40-44",
    age_at_birth >= 45, "45+"
  ))]

mother_synth_summary <- mother_synth_binned[, .N, by = age_range]
mother_synth_summary[, synthetic_pct := N / sum(N) * 100]

# Merge
mother_comparison <- merge(mother_target, mother_synth_summary[, .(age_range, synthetic_pct)], 
                           by = "age_range", all.x = TRUE)
mother_comparison[is.na(synthetic_pct), synthetic_pct := 0]

# Statistics
cat("  Children with mother data:", nrow(children_with_mothers), "\n")
cat("  Mean mother age at birth (synthetic):", 
    round(mean(children_with_mothers$age_at_birth, na.rm = TRUE), 1), "years\n")
cat("  Median:", 
    round(median(children_with_mothers$age_at_birth, na.rm = TRUE), 1), "years\n\n")

# Plot 4: Mother Age at Birth
mother_comparison[, age_range := factor(age_range, 
  levels = c("Under 20", "20-24", "25-29", "30-34", "35-39", "40-44", "45+"))]

p4 <- ggplot(mother_comparison, aes(x = age_range)) +
  geom_col(aes(y = target_pct, fill = "Target"), alpha = 0.6, position = "dodge") +
  geom_col(aes(y = synthetic_pct, fill = "Synthetic"), alpha = 0.6, position = "dodge") +
  scale_fill_manual(values = c("Target" = "blue", "Synthetic" = "red")) +
  labs(title = "Mother Age at Birth Distribution",
       x = "Mother's Age at Birth", y = "Percentage", fill = "") +
  theme_bw() +
  theme(legend.position = "top",
        axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(output_dir, "04_mother_age_at_birth.png"), p4, 
       width = 10, height = 6, dpi = 300)

# --------------------------
# 5) FATHER AGE AT BIRTH
# --------------------------
cat("Validating father age at birth...\n")

# Calculate synthetic father ages at birth
father_data <- hh_parents[, {
  fathers <- .SD[sex == 2]  # Male parents
  if (nrow(fathers) > 0) {
    father_age <- fathers[order(-assigned_age)][1, assigned_age]
    .(father_age = father_age)
  }
}, by = .(OA, household_id_m)]

children_with_fathers <- merge(
  pop[role == "Child", .(OA, household_id_m, child_age = assigned_age)],
  father_data,
  by = c("OA", "household_id_m")
)
children_with_fathers[, age_at_birth := father_age - child_age]

# Prepare target data
father_target <- data.table(
  age_at_birth = c(18, 22, 27, 32, 37, 42, 47, 52, 57, 62, 67),
  age_range = c("Under 20", "20-24", "25-29", "30-34", "35-39", "40-44", 
                "45-49", "50-54", "55-59", "60-64", "65+"),
  target_pct = father_aab$percentage
)

# Prepare synthetic data (binned)
father_synth_binned <- children_with_fathers[age_at_birth >= 12 & age_at_birth <= 75, 
  .(age_range = fcase(
    age_at_birth < 20, "Under 20",
    age_at_birth < 25, "20-24",
    age_at_birth < 30, "25-29",
    age_at_birth < 35, "30-34",
    age_at_birth < 40, "35-39",
    age_at_birth < 45, "40-44",
    age_at_birth < 50, "45-49",
    age_at_birth < 55, "50-54",
    age_at_birth < 60, "55-59",
    age_at_birth < 65, "60-64",
    age_at_birth >= 65, "65+"
  ))]

father_synth_summary <- father_synth_binned[, .N, by = age_range]
father_synth_summary[, synthetic_pct := N / sum(N) * 100]

# Merge
father_comparison <- merge(father_target, father_synth_summary[, .(age_range, synthetic_pct)], 
                           by = "age_range", all.x = TRUE)
father_comparison[is.na(synthetic_pct), synthetic_pct := 0]

# Statistics
cat("  Children with father data:", nrow(children_with_fathers), "\n")
cat("  Mean father age at birth (synthetic):", 
    round(mean(children_with_fathers$age_at_birth, na.rm = TRUE), 1), "years\n")
cat("  Median:", 
    round(median(children_with_fathers$age_at_birth, na.rm = TRUE), 1), "years\n\n")

# Plot 5: Father Age at Birth
father_comparison[, age_range := factor(age_range, 
  levels = c("Under 20", "20-24", "25-29", "30-34", "35-39", "40-44", 
             "45-49", "50-54", "55-59", "60-64", "65+"))]

p5 <- ggplot(father_comparison, aes(x = age_range)) +
  geom_col(aes(y = target_pct, fill = "Target"), alpha = 0.6, position = "dodge") +
  geom_col(aes(y = synthetic_pct, fill = "Synthetic"), alpha = 0.6, position = "dodge") +
  scale_fill_manual(values = c("Target" = "blue", "Synthetic" = "red")) +
  labs(title = "Father Age at Birth Distribution",
       x = "Father's Age at Birth", y = "Percentage", fill = "") +
  theme_bw() +
  theme(legend.position = "top",
        axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(output_dir, "05_father_age_at_birth.png"), p5, 
       width = 10, height = 6, dpi = 300)

# --------------------------
# 6) SIBLING AGE GAPS
# --------------------------
cat("Analyzing sibling age gaps...\n")

children <- pop[role == "Child"]
sibling_gaps <- children[, {
  ages <- sort(assigned_age, decreasing = TRUE)
  if (length(ages) > 1) {
    gaps <- diff(ages)
    .(gap = abs(gaps))
  }
}, by = .(OA, household_id_m)]

if (nrow(sibling_gaps) > 0) {
  cat("  Total sibling pairs:", nrow(sibling_gaps), "\n")
  cat("  Twins (0 gap):", sum(sibling_gaps$gap == 0), "pairs\n")
  cat("  1-4 year gaps:", sum(sibling_gaps$gap %in% 1:4), "pairs (", 
      round(sum(sibling_gaps$gap %in% 1:4) / nrow(sibling_gaps) * 100, 1), "%)\n")
  cat("  5+ year gaps:", sum(sibling_gaps$gap >= 5), "pairs (", 
      round(sum(sibling_gaps$gap >= 5) / nrow(sibling_gaps) * 100, 1), "%)\n")
  
  # Plot 6: Sibling Age Gaps
  sibling_summary <- sibling_gaps[, .N, by = gap][order(gap)]
  sibling_summary[, pct := N / sum(N) * 100]
  
  p6 <- ggplot(sibling_summary[gap <= 15], aes(x = gap, y = pct)) +
    geom_col(fill = "steelblue") +
    geom_vline(xintercept = c(1, 4), linetype = "dashed", color = "red", alpha = 0.5) +
    annotate("text", x = 2.5, y = max(sibling_summary$pct) * 0.9, 
             label = "Ideal range\n(1-4 years)", color = "red", size = 3) +
    labs(title = "Sibling Age Gap Distribution",
         subtitle = "Gap between consecutive children in same household",
         x = "Age Gap (years)", y = "Percentage of Sibling Pairs") +
    theme_bw()
  
  ggsave(file.path(output_dir, "06_sibling_age_gaps.png"), p6, 
         width = 10, height = 6, dpi = 300)
}

cat("\n")

# --------------------------
# 7) HOUSEHOLD COMPOSITION VALIDATION (CROSS-TABULATION)
# --------------------------
cat("Validating household composition cross-tabulation against MSOA targets...\n")

# Load MSOA household composition targets (cross-tabulation)
msoa_hh <- fread("data/processed/msoa_hh_composition.csv")
msoa_hh_target <- msoa_hh[`Middle layer Super Output Areas Code` == msoa_code &
                          `Household composition (6 categories) Code` != -8 &
                          `Adults and children in household (11 categories) Code` != -8,
                          .(comp_code = `Household composition (6 categories) Code`,
                            comp = `Household composition (6 categories)`,
                            adults_children_code = `Adults and children in household (11 categories) Code`,
                            adults_children = `Adults and children in household (11 categories)`,
                            target = as.integer(Observation))]

# Classify each household in the synthetic population
hh <- pop[, .(
  n_persons = .N,
  n_adults = sum(assigned_age >= 16, na.rm = TRUE),
  n_children = sum(assigned_age < 16, na.rm = TRUE),
  n_66plus = sum(assigned_age >= 66, na.rm = TRUE),
  n_65minus = sum(assigned_age <= 65, na.rm = TRUE),
  has_head = any(role == "Head"),
  has_partner = any(role == "Partner"),
  n_child_role = sum(role == "Child")
), by = .(OA, household_id_m)]

# Assign composition category (6 categories)
hh[, comp_code := fcase(
  n_persons == 1, 1L,  # One-person household
  n_persons > 1 & n_66plus == n_persons, 2L,  # All aged 66+
  has_head & has_partner, 3L,  # Couple family
  has_head & !has_partner & n_child_role > 0, 4L,  # Lone parent
  default = 5L  # Other
)]

# Assign adults/children category (11 categories)
hh[, adults_children_code := fcase(
  n_persons == 1 & n_66plus == 1, 1L,  # One adult 66+
  n_persons == 1 & n_66plus == 0, 2L,  # One person 65 or under
  n_adults == 0 | (n_adults == 1 & n_children > 0), 3L,  # No adults or one adult + children
  n_adults == 2 & n_children == 0 & n_65minus >= 1 & n_66plus >= 1, 4L,  # Two adults mixed age, no children
  n_adults == 2 & n_children == 0 & n_66plus == 2, 5L,  # Two adults both 66+, no children
  n_adults == 2 & n_children == 0 & n_66plus == 0, 6L,  # Two adults both 65-, no children
  n_adults == 2 & n_children %in% 1:2, 7L,  # Two adults, 1-2 children
  n_adults == 2 & n_children >= 3, 8L,  # Two adults, 3+ children
  n_adults >= 3 & n_children >= 1, 9L,  # Three+ adults, 1+ children
  n_adults >= 3 & n_children == 0, 10L,  # Three+ adults, no children
  default = -8L
)]

# Aggregate synthetic counts
synthetic_hh_counts <- hh[, .N, by = .(comp_code, adults_children_code)]
setnames(synthetic_hh_counts, "N", "synthetic")

# Merge with targets
comp_compare <- merge(
  msoa_hh_target[, .(comp_code, adults_children_code, comp, adults_children, target)],
  synthetic_hh_counts,
  by = c("comp_code", "adults_children_code"),
  all.x = TRUE
)
comp_compare[is.na(synthetic), synthetic := 0]
comp_compare[, diff := synthetic - target]

# Filter to non-zero rows for display
comp_compare_display <- comp_compare[target > 0 | synthetic > 0]
setorder(comp_compare_display, comp_code, adults_children_code)

cat("  Total target households:", sum(comp_compare$target), "\n")
cat("  Total synthetic households:", sum(comp_compare$synthetic), "\n")
cat("  Difference (synthetic - target):", sum(comp_compare$diff), "\n\n")

cat("  Top mismatches (by absolute difference):\n")
top_diffs <- comp_compare_display[order(-abs(diff))][1:10]
print(top_diffs[, .(comp, adults_children, target, synthetic, diff)])
cat("\n")

# Save full cross-tab matrix to CSV
comp_matrix_file <- file.path(output_dir, "household_composition_crosstab.csv")
fwrite(comp_compare_display[, .(comp_code, comp, adults_children_code, adults_children, target, synthetic, diff)],
       comp_matrix_file)
cat("  Full cross-tab matrix saved to household_composition_crosstab.csv\n\n")

# Plot composition comparison (aggregate by composition category)
comp_agg <- comp_compare[, .(target = sum(target), synthetic = sum(synthetic)), by = comp]
comp_agg[, target_pct := round(target / sum(target) * 100, 1)]
comp_agg[, synthetic_pct := round(synthetic / sum(synthetic) * 100, 1)]

cmp_melt <- melt(comp_agg[, .(comp, target_pct, synthetic_pct)], id.vars = "comp")
p7 <- ggplot(cmp_melt, aes(x = reorder(comp, -value), y = value, fill = variable)) +
  geom_col(position = "dodge") +
  labs(title = "Household Composition: Target vs Synthetic",
       x = "Household composition", y = "Percentage", fill = "") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(output_dir, "07_household_composition.png"), p7, width = 10, height = 6, dpi = 300)

cat("  Household composition comparison saved to 07_household_composition.png\n\n")

# --------------------------
# 8) SUMMARY REPORT
# --------------------------
cat("==============================================\n")
cat("VALIDATION SUMMARY\n")
cat("==============================================\n\n")

cat("All validation plots saved to:\n")
cat("  ", output_dir, "\n\n")

cat("Files created:\n")
cat("  01_age_sex_distribution.png - Age-sex pyramid comparison\n")
cat("  02_age_sex_residuals.png - Synthetic vs target differences\n")
cat("  03_partner_age_gaps.png - Partner age gap distribution\n")
cat("  04_mother_age_at_birth.png - Mother's age at childbirth\n")
cat("  05_father_age_at_birth.png - Father's age at childbirth\n")
cat("  06_sibling_age_gaps.png - Age gaps between siblings\n")
cat("  07_household_composition.png - Household composition comparison\n\n")

cat("Done!\n")
