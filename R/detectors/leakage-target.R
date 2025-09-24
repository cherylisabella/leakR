#' @title Detect target leakage
#'
#' @description Identifies suspicious relationships between features and target variable that
#' may indicate information leakage from future or unavailable data.
#'
#' @param audit_data Prepared audit data structure
#' @param config Configuration list with correlation and significance thresholds
#'
#' @return detector_result object with target leakage findings
#' @keywords internal
detect_target_leakage <- function(audit_data, config) {

  issues <- data.frame()
  evidence <- list()

  # Early return if no target provided
  if (is.null(audit_data$target)) {
    return(create_detector_result(issues, list(note = "No target variable specified")))
  }

  data <- audit_data$data
  target <- data[[audit_data$target]]
  feature_names <- audit_data$feature_names

  if (length(feature_names) == 0) {
    return(create_detector_result(issues, list(note = "No features available for analysis")))
  }

  # Store target information for evidence
  evidence$target_info <- analyse_target_distribution(target)

  # 1. Correlation-based leakage detection
  correlation_result <- detect_correlation_leakage(data, target, feature_names, config)
  if (nrow(correlation_result$issues) > 0) {
    issues <- rbind(issues, correlation_result$issues)
  }
  evidence$correlations <- correlation_result$evidence

  # 2. Perfect separation detection (near-deterministic relationships)
  separation_result <- detect_perfect_separation(data, target, feature_names, config)
  if (nrow(separation_result$issues) > 0) {
    issues <- rbind(issues, separation_result$issues)
  }
  evidence$perfect_separation <- separation_result$evidence

  # 3. Aggregation-based leakage (group statistics computed on target)
  aggregation_result <- detect_aggregation_leakage(data, target, feature_names, config)
  if (nrow(aggregation_result$issues) > 0) {
    issues <- rbind(issues, aggregation_result$issues)
  }
  evidence$aggregation_patterns <- aggregation_result$evidence

  # 4. Feature importance leakage (shadow model approach)
  if (length(audit_data$numeric_features) >= 2) {
    importance_result <- detect_feature_importance_leakage(data, target, audit_data$numeric_features, config)
    if (nrow(importance_result$issues) > 0) {
      issues <- rbind(issues, importance_result$issues)
    }
    evidence$feature_importance <- importance_result$evidence
  }

  # 5. Temporal consistency checks (if applicable)
  if ("date" %in% tolower(names(data)) || "time" %in% tolower(names(data))) {
    temporal_result <- detect_temporal_target_leakage(data, target, config)
    if (nrow(temporal_result$issues) > 0) {
      issues <- rbind(issues, temporal_result$issues)
    }
    evidence$temporal_analysis <- temporal_result$evidence
  }

  return(create_detector_result(issues, evidence))
}

#' Analyse target variable distribution
#'
#' @param target Target variable vector
#' @return List with target distribution statistics
#' @keywords internal
analyse_target_distribution <- function(target) {

  target_clean <- target[!is.na(target)]

  if (is.numeric(target_clean)) {
    list(
      type = "numeric",
      n_values = length(target_clean),
      n_unique = length(unique(target_clean)),
      mean = mean(target_clean),
      median = median(target_clean),
      sd = sd(target_clean),
      range = range(target_clean),
      missing_rate = sum(is.na(target)) / length(target)
    )
  } else {
    target_table <- table(target_clean)
    list(
      type = "categorical",
      n_values = length(target_clean),
      n_levels = length(target_table),
      levels = names(target_table),
      level_counts = as.numeric(target_table),
      level_proportions = as.numeric(target_table) / sum(target_table),
      missing_rate = sum(is.na(target)) / length(target),
      is_balanced = max(target_table) / min(target_table) < 3
    )
  }
}

#' Detect correlation-based target leakage
#'
#' @param data Dataset
#' @param target Target variable
#' @param feature_names Feature column names
#' @param config Configuration parameters
#' @return List with correlation-based leakage results
#' @keywords internal
detect_correlation_leakage <- function(data, target, feature_names, config) {

  issues <- data.frame()
  correlations <- list()

  for (feature_name in feature_names) {
    feature <- data[[feature_name]]

    # Skip if feature has no variation
    if (length(unique(feature[!is.na(feature)])) <= 1) {
      next
    }

    # Calculate correlation
    corr_result <- calculate_target_correlation(feature, target, method = "auto")
    correlations[[feature_name]] <- corr_result

    # Flag high correlations
    if (!is.na(corr_result$correlation) && !is.na(corr_result$p_value)) {
      abs_corr <- abs(corr_result$correlation)

      if (abs_corr >= config$correlation_threshold && corr_result$p_value < 0.05) {
        severity <- determine_correlation_severity(abs_corr, corr_result$p_value)

        issue <- data.frame(
          issue_type = "high_correlation",
          severity = severity,
          description = sprintf("Feature '%s' shows suspiciously high correlation with target (r=%.3f, p<%.3f)",
                                feature_name, corr_result$correlation, corr_result$p_value),
          suggested_fix = sprintf("Investigate whether '%s' contains future information or is derived from the target",
                                  feature_name)
        )

        issues <- rbind(issues, issue)
      }
    }
  }

  # Rank correlations for evidence
  valid_correlations <- correlations[!sapply(correlations, function(x) is.na(x$correlation))]
  if (length(valid_correlations) > 0) {
    corr_values <- sapply(valid_correlations, function(x) abs(x$correlation))
    correlation_ranking <- names(sort(corr_values, decreasing = TRUE))
  } else {
    correlation_ranking <- character(0)
  }

  evidence <- list(
    correlations = correlations,
    correlation_ranking = correlation_ranking,
    n_features_tested = length(feature_names),
    n_high_correlations = nrow(issues)
  )

  return(list(issues = issues, evidence = evidence))
}

#' Determine correlation severity
#' @keywords internal
determine_correlation_severity <- function(abs_correlation, p_value) {
  if (abs_correlation >= 0.95 && p_value < 0.001) {
    "critical"
  } else if (abs_correlation >= 0.9 && p_value < 0.01) {
    "high"
  } else if (abs_correlation >= 0.8) {
    "medium"
  } else {
    "low"
  }
}

#' Detect perfect or near-perfect separation
#'
#' @param data Dataset
#' @param target Target variable
#' @param feature_names Feature names to test
#' @param config Configuration parameters
#' @return List with perfect separation results
#' @keywords internal
detect_perfect_separation <- function(data, target, feature_names, config) {

  issues <- data.frame()
  separation_tests <- list()

  # Only meaningful for classification targets
  if (!is.factor(target) && !is.character(target)) {
    return(list(issues = issues, evidence = list(note = "Perfect separation analysis requires categorical target")))
  }

  target_factor <- as.factor(target)
  target_levels <- levels(target_factor)

  if (length(target_levels) < 2) {
    return(list(issues = issues, evidence = list(note = "Target has insufficient levels for separation analysis")))
  }

  for (feature_name in feature_names) {
    feature <- data[[feature_name]]

    # Test for perfect separation
    separation_result <- test_perfect_separation(feature, target_factor, feature_name)
    separation_tests[[feature_name]] <- separation_result

    if (separation_result$is_perfect_separator) {
      severity <- if (separation_result$separation_strength > 0.99) "critical" else "high"

      issue <- data.frame(
        issue_type = "perfect_separation",
        severity = severity,
        description = sprintf("Feature '%s' perfectly or nearly perfectly separates target classes (strength=%.3f)",
                              feature_name, separation_result$separation_strength),
        suggested_fix = sprintf("Verify that '%s' is not derived from or leaked from the target variable",
                                feature_name)
      )

      issues <- rbind(issues, issue)
    }
  }

  evidence <- list(
    separation_tests = separation_tests,
    target_levels = target_levels,
    n_perfect_separators = nrow(issues)
  )

  return(list(issues = issues, evidence = evidence))
}

#' Test for perfect separation between feature and target
#' @keywords internal
test_perfect_separation <- function(feature, target_factor, feature_name) {

  # Create contingency table
  contingency <- table(feature, target_factor, useNA = "ifany")

  if (any(dim(contingency) < 2)) {
    return(list(
      is_perfect_separator = FALSE,
      separation_strength = 0,
      method = "insufficient_variation"
    ))
  }

  # Calculate separation strength using various methods

  # Method 1: Cramér's V
  chi_sq <- suppressWarnings(chisq.test(contingency))
  cramers_v <- if (!is.na(chi_sq$statistic)) {
    sqrt(chi_sq$statistic / (sum(contingency) * (min(dim(contingency)) - 1)))
  } else {
    0
  }

  # Method 2: Check for rows/columns with zeros (perfect separation indicator)
  row_zeros <- apply(contingency, 1, function(x) sum(x == 0))
  col_zeros <- apply(contingency, 2, function(x) sum(x == 0))

  perfect_separation_indicator <- (max(row_zeros) >= (ncol(contingency) - 1)) ||
    (max(col_zeros) >= (nrow(contingency) - 1))

  # Method 3: Mutual information-based measure
  mutual_info <- calculate_mutual_information(contingency)

  separation_strength <- max(cramers_v, mutual_info, na.rm = TRUE)

  list(
    is_perfect_separator = perfect_separation_indicator || separation_strength > 0.95,
    separation_strength = separation_strength,
    cramers_v = cramers_v,
    mutual_information = mutual_info,
    contingency_table = contingency,
    method = "combined"
  )
}

#' Calculate mutual information from contingency table
#' @keywords internal
calculate_mutual_information <- function(contingency) {

  # Normalise to get joint probabilities
  joint_prob <- contingency / sum(contingency)

  # Marginal probabilities
  row_prob <- rowSums(joint_prob)
  col_prob <- colSums(joint_prob)

  # Calculate mutual information
  mutual_info <- 0
  for (i in seq_len(nrow(joint_prob))) {
    for (j in seq_len(ncol(joint_prob))) {
      if (joint_prob[i, j] > 0 && row_prob[i] > 0 && col_prob[j] > 0) {
        mutual_info <- mutual_info + joint_prob[i, j] * log2(joint_prob[i, j] / (row_prob[i] * col_prob[j]))
      }
    }
  }

  # Normalise by entropy of target (column variable)
  target_entropy <- -sum(col_prob * log2(col_prob + 1e-10))

  if (target_entropy > 0) {
    mutual_info / target_entropy
  } else {
    0
  }
}

#' Detect aggregation-based leakage
#'
#' @param data Dataset
#' @param target Target variable
#' @param feature_names Feature names
#' @param config Configuration parameters
#' @return List with aggregation leakage results
#' @keywords internal
detect_aggregation_leakage <- function(data, target, feature_names, config) {

  issues <- data.frame()
  aggregation_patterns <- list()

  # Look for features that might be group aggregations
  for (feature_name in feature_names) {
    feature <- data[[feature_name]]

    # Skip non-numeric features for aggregation analysis
    if (!is.numeric(feature)) {
      next
    }

    # Test for aggregation patterns
    agg_result <- test_aggregation_pattern(feature, target, feature_name, data)
    aggregation_patterns[[feature_name]] <- agg_result

    if (agg_result$likely_aggregation && agg_result$leakage_score > 0.7) {
      severity <- if (agg_result$leakage_score > 0.9) "high" else "medium"

      issue <- data.frame(
        issue_type = "aggregation_leakage",
        severity = severity,
        description = sprintf("Feature '%s' shows patterns consistent with target-based aggregation (score=%.3f)",
                              feature_name, agg_result$leakage_score),
        suggested_fix = sprintf("Verify that '%s' is not computed using group statistics that include the target",
                                feature_name)
      )

      issues <- rbind(issues, issue)
    }
  }

  evidence <- list(
    aggregation_tests = aggregation_patterns,
    n_features_tested = sum(sapply(data[feature_names], is.numeric)),
    n_likely_aggregations = nrow(issues)
  )

  return(list(issues = issues, evidence = evidence))
}

#' Test for aggregation-based leakage patterns
#' @keywords internal
test_aggregation_pattern <- function(feature, target, feature_name, data) {

  # Look for repeated values that correlate with target
  feature_counts <- table(feature)
  repeated_values <- names(feature_counts)[feature_counts > 1]

  if (length(repeated_values) == 0) {
    return(list(likely_aggregation = FALSE, leakage_score = 0, method = "no_repetition"))
  }

  # For each repeated value, check if it's associated with specific target values
  leakage_scores <- numeric(length(repeated_values))

  for (i in seq_along(repeated_values)) {
    value <- as.numeric(repeated_values[i])
    value_mask <- feature == value

    if (sum(value_mask) < 2) {
      next
    }

    # Calculate how "pure" this group is with respect to target
    target_subset <- target[value_mask]

    if (is.numeric(target)) {
      # For numeric targets, check coefficient of variation
      cv <- sd(target_subset, na.rm = TRUE) / mean(target_subset, na.rm = TRUE)
      leakage_scores[i] <- 1 - min(cv, 1)  # Lower variation = higher leakage score
    } else {
      # For categorical targets, check purity
      target_table <- table(target_subset)
      purity <- max(target_table) / sum(target_table)
      leakage_scores[i] <- purity
    }
  }

  overall_score <- mean(leakage_scores, na.rm = TRUE)

  list(
    likely_aggregation = overall_score > 0.5,
    leakage_score = overall_score,
    n_repeated_values = length(repeated_values),
    max_group_purity = max(leakage_scores, na.rm = TRUE),
    method = "group_purity"
  )
}

#' Detect feature importance-based leakage using shadow models
#'
#' @param data Dataset
#' @param target Target variable
#' @param numeric_features Numeric feature names
#' @param config Configuration parameters
#' @return List with feature importance results
#' @keywords internal
detect_feature_importance_leakage <- function(data, target, numeric_features, config) {

  issues <- data.frame()

  tryCatch({
    # Prepare data for modelling
    feature_data <- data[, numeric_features, drop = FALSE]

    # Remove features with no variation or too many missing values
    valid_features <- sapply(feature_data, function(x) {
      length(unique(x[!is.na(x)])) > 1 && sum(is.na(x)) / length(x) < 0.5
    })

    if (sum(valid_features) < 2) {
      evidence <- list(note = "Insufficient valid numeric features for importance analysis")
      return(list(issues = issues, evidence = evidence))
    }

    X <- as.matrix(feature_data[, valid_features, drop = FALSE])
    y <- target

    # Remove rows with missing values
    complete_cases <- complete.cases(X, y)
    X <- X[complete_cases, , drop = FALSE]
    y <- y[complete_cases]

    if (nrow(X) < 10) {
      evidence <- list(note = "Insufficient complete cases for importance analysis")
      return(list(issues = issues, evidence = evidence))
    }

    # Build shadow model and calculate feature importance
    importance_result <- calculate_feature_importance(X, y)

    # Flag features with suspiciously high importance
    if (!is.null(importance_result$importance)) {
      high_importance_threshold <- quantile(importance_result$importance, 0.9)
      high_importance_features <- names(importance_result$importance)[
        importance_result$importance > high_importance_threshold &
          importance_result$importance > 0.3
      ]

      for (feature_name in high_importance_features) {
        importance_value <- importance_result$importance[feature_name]

        severity <- if (importance_value > 0.7) "high" else "medium"

        issue <- data.frame(
          issue_type = "high_feature_importance",
          severity = severity,
          description = sprintf("Feature '%s' has unusually high predictive importance (%.3f)",
                                feature_name, importance_value),
          suggested_fix = sprintf("Investigate whether '%s' contains leaked information about the target",
                                  feature_name)
        )

        issues <- rbind(issues, issue)
      }
    }

    evidence <- list(
      model_performance = importance_result$performance,
      feature_importance = importance_result$importance,
      n_features_modelled = ncol(X),
      model_type = importance_result$model_type
    )

  }, error = function(e) {
    evidence <- list(error = as.character(e), note = "Feature importance analysis failed")
  })

  return(list(issues = issues, evidence = evidence))
}

#' Calculate feature importance using a simple model
#' @keywords internal
calculate_feature_importance <- function(X, y) {

  # Determine model type based on target
  if (is.numeric(y)) {
    # Regression
    tryCatch({
      model <- lm(y ~ ., data = data.frame(y = y, X))

      # Use absolute t-statistics as importance measure
      coef_summary <- summary(model)$coefficients
      importance <- abs(coef_summary[-1, "t value"])  # Exclude intercept
      names(importance) <- colnames(X)

      # Normalise to [0,1]
      importance <- importance / max(importance, na.rm = TRUE)

      list(
        importance = importance,
        performance = summary(model)$r.squared,
        model_type = "linear_regression"
      )
    }, error = function(e) {
      list(importance = NULL, performance = NA, model_type = "failed")
    })
  } else {
    # Classification
    tryCatch({
      y_factor <- as.factor(y)
      model_data <- data.frame(y = y_factor, X)
      model <- glm(y ~ ., data = model_data, family = binomial())

      # Use absolute z-statistics as importance measure
      coef_summary <- summary(model)$coefficients
      importance <- abs(coef_summary[-1, "z value"])  # Exclude intercept
      names(importance) <- colnames(X)

      # Normalise to [0,1]
      importance <- importance / max(importance, na.rm = TRUE)

      # Calculate pseudo R-squared
      null_deviance <- model$null.deviance
      residual_deviance <- model$deviance
      pseudo_r2 <- 1 - (residual_deviance / null_deviance)

      list(
        importance = importance,
        performance = pseudo_r2,
        model_type = "logistic_regression"
      )
    }, error = function(e) {
      list(importance = NULL, performance = NA, model_type = "failed")
    })
  }
}

#' Detect temporal target leakage
#'
#' @param data Dataset
#' @param target Target variable
#' @param config Configuration parameters
#' @return List with temporal leakage results
#' @keywords internal
detect_temporal_target_leakage <- function(data, target, config) {

  issues <- data.frame()

  # Find potential time columns
  time_cols <- find_time_columns(data)

  if (length(time_cols) == 0) {
    evidence <- list(note = "No temporal columns identified")
    return(list(issues = issues, evidence = evidence))
  }

  # Analyse temporal patterns in relation to target
  temporal_analysis <- list()

  for (time_col in time_cols) {
    time_values <- data[[time_col]]

    # Check if target varies systematically with time
    temporal_result <- analyse_temporal_target_relationship(time_values, target, time_col)
    temporal_analysis[[time_col]] <- temporal_result

    if (temporal_result$has_temporal_leakage) {
      severity <- if (temporal_result$leakage_strength > 0.8) "medium" else "low"

      issue <- data.frame(
        issue_type = "temporal_target_pattern",
        severity = severity,
        description = sprintf("Target shows systematic temporal patterns with '%s' (strength=%.3f)",
                              time_col, temporal_result$leakage_strength),
        suggested_fix = sprintf("Verify temporal data splitting and check if '%s' introduces lookahead bias",
                                time_col)
      )

      issues <- rbind(issues, issue)
    }
  }

  evidence <- list(
    temporal_columns = time_cols,
    temporal_analysis = temporal_analysis
  )

  return(list(issues = issues, evidence = evidence))
}

#' Find potential time columns in dataset
#' @keywords internal
find_time_columns <- function(data) {
  time_indicators <- c("date", "time", "timestamp", "created", "updated", "year", "month", "day")

  potential_time_cols <- character(0)

  for (col_name in names(data)) {
    col_lower <- tolower(col_name)

    # Check if column name contains time indicators
    if (any(sapply(time_indicators, function(x) grepl(x, col_lower)))) {
      potential_time_cols <- c(potential_time_cols, col_name)
    }

    # Check if column is Date, POSIXt, or looks like a time
    col_data <- data[[col_name]]
    if (inherits(col_data, c("Date", "POSIXt")) ||
        (is.character(col_data) && any(grepl("\\d{4}-\\d{2}-\\d{2}", col_data[1:min(10, length(col_data))])))) {
      potential_time_cols <- c(potential_time_cols, col_name)
    }
  }

  unique(potential_time_cols)
}

#' Analyse temporal relationship with target
#' @keywords internal
analyse_temporal_target_relationship <- function(time_values, target, time_col_name) {

  tryCatch({
    # Convert to proper time format if possible
    if (is.character(time_values)) {
      time_values <- as.Date(time_values)
    }

    # Remove missing values
    complete_cases <- complete.cases(time_values, target)
    time_clean <- time_values[complete_cases]
    target_clean <- target[complete_cases]

    if (length(time_clean) < 10) {
      return(list(has_temporal_leakage = FALSE, leakage_strength = 0, method = "insufficient_data"))
    }

    # Calculate correlation between time (as numeric) and target
    time_numeric <- as.numeric(time_clean)

    if (is.numeric(target_clean)) {
      corr_result <- cor.test(time_numeric, target_clean, method = "spearman")
      leakage_strength <- abs(corr_result$estimate)
    } else {
      # For categorical targets, use trend test
      target_numeric <- as.numeric(as.factor(target_clean))
      corr_result <- cor.test(time_numeric, target_numeric, method = "spearman")
      leakage_strength <- abs(corr_result$estimate)
    }

    list(
      has_temporal_leakage = !is.na(leakage_strength) && leakage_strength > 0.3 && corr_result$p.value < 0.05,
      leakage_strength = ifelse(is.na(leakage_strength), 0, leakage_strength),
      p_value = corr_result$p.value,
      method = "spearman_correlation"
    )
  }, error = function(e) {
    list(has_temporal_leakage = FALSE, leakage_strength = 0, method = "error", error = as.character(e))
  })
}
