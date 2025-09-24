#' @title Audit dataset for data leakage
#'
#' @description Main function to detect common data leakage patterns in ML workflows.
#' Uses a registry-based detector system for extensibility.
#'
#' @param data A data.frame, tibble, or data.table to audit
#' @param target Character name of target/response column (optional)
#' @param split Vector indicating train/test split or column name containing split labels
#' @param id Character name of ID column for duplicate detection (optional)
#' @param detectors Character vector of detector names to run. NULL runs all available
#' @param config List of configuration options (thresholds, sampling, plotting, etc.)
#'
#' @return leakr_report object containing detected issues and diagnostics
#' @export
#'
#' @examples
#' # Basic audit
#' report <- leakr_audit(iris, target = "Species")
#'
#' @keywords internal
#' # With configuration
#' config <- list(correlation_threshold = 0.9, plot_results = TRUE)
#' report <- leakr_audit(iris, target = "Species", config = config)
leakr_audit <- function(data, target = NULL, split = NULL, id = NULL,
                        detectors = NULL, config = list()) {

  # Input validation with robust preprocessing
  data <- validate_and_preprocess_data(data, target, split, id)

  # Enhanced default configuration
  default_config <- list(
    sample_size = 50000,
    correlation_threshold = 0.8,
    contamination_threshold = 0.1,
    numeric_severity = TRUE,
    plot_results = FALSE,
    parallel = FALSE,
    seed = 123
  )
  config <- modifyList(default_config, config)

  # Set seed for reproducibility
  set.seed(config$seed)

  # Get available detectors from registry
  available_detectors <- list_registered_detectors()

  if (is.null(detectors)) {
    detectors <- available_detectors
  }

  # Validate detector names
  unknown_detectors <- setdiff(detectors, available_detectors)
  if (length(unknown_detectors) > 0) {
    stop("Unknown detectors: ", paste(unknown_detectors, collapse = ", "))
  }

  # Prepare data structure with robust preprocessing
  audit_data <- prepare_audit_data(data, target, split, id, config)

  # Run detectors (with optional parallel execution)
  results <- run_detectors(detectors, audit_data, config)

  # Compile comprehensive report
  report <- compile_report(results, audit_data, config)

  # Optional plotting
  if (config$plot_results && nrow(report$summary) > 0) {
    report$plots <- generate_diagnostic_plots(report)
  }

  return(report)
}

#' Registry-based detector system
#' @keywords internal
.detector_registry <- new.env(parent = emptyenv())

#' Register a new detector
#'
#' Add a detector function to the registry for use in audits.
#'
#' @param name Character name of detector
#' @param func Function that takes (audit_data, config) and returns detector_result
#' @param description Short description of what the detector checks
#' @keywords internal
#' @export
register_detector <- function(name, func, description = "") {
  if (!is.function(func)) {
    stop("func must be a function")
  }

  if (!is.character(name) || length(name) != 1) {
    stop("name must be a single character string")
  }

  .detector_registry[[name]] <- list(
    func = func,
    description = description,
    registered_at = Sys.time()
  )

  invisible(TRUE)
}

#' List registered detectors
#' @keywords internal
#' @return Character vector of available detector names
#' @export
list_registered_detectors <- function() {
  names(.detector_registry)
}

#' Get detector information
#'
#' @param name Detector name (optional - if NULL, returns all)
#' @return List with detector information
#' @keywords internal
#' @export
get_detector_info <- function(name = NULL) {
  if (is.null(name)) {
    return(lapply(.detector_registry, function(x) list(description = x$description)))
  }

  if (!name %in% names(.detector_registry)) {
    stop("Detector '", name, "' not found")
  }

  .detector_registry[[name]][c("description", "registered_at")]
}

#' Robust data validation and preprocessing
#' @keywords internal
validate_and_preprocess_data <- function(data, target, split, id) {

  # Type validation
  if (!inherits(data, c("data.frame", "tbl_df", "data.table"))) {
    stop("data must be a data.frame, tibble, or data.table")
  }

  # Handle data.table properly
  if (inherits(data, "data.table")) {
    data <- as.data.frame(data)
  }

  # Column validation
  if (!is.null(target) && !target %in% names(data)) {
    stop("target column '", target, "' not found in data")
  }

  if (!is.null(id) && !id %in% names(data)) {
    stop("id column '", id, "' not found in data")
  }

  # Check for empty data
  if (nrow(data) == 0 || ncol(data) == 0) {
    stop("data cannot be empty")
  }

  # Handle problematic column names
  names(data) <- make.names(names(data), unique = TRUE)

  return(data)
}

#' Enhanced data preparation with robust preprocessing
#' @keywords internal
prepare_audit_data <- function(data, target, split, id, config) {

  original_n_rows <- nrow(data)

  # Intelligent sampling for large datasets
  if (nrow(data) > config$sample_size) {
    # Stratified sampling if target is provided
    if (!is.null(target) && target %in% names(data)) {
      sample_idx <- stratified_sample(data[[target]], config$sample_size)
    } else {
      sample_idx <- sample(nrow(data), config$sample_size)
    }

    data <- data[sample_idx, , drop = FALSE]

    # Adjust split vector if provided
    if (!is.null(split) && is.vector(split) && length(split) == original_n_rows) {
      split <- split[sample_idx]
    }
  }

  # Handle split specification
  if (is.character(split) && length(split) == 1 && split %in% names(data)) {
    split <- data[[split]]
  }

  # Identify feature types for better processing
  feature_names <- setdiff(names(data), c(target, id))
  numeric_features <- feature_names[sapply(data[feature_names], is.numeric)]
  categorical_features <- feature_names[sapply(data[feature_names], function(x) is.factor(x) || is.character(x))]

  # Create enhanced audit data structure
  audit_data <- list(
    data = data,
    target = target,
    split = split,
    id = id,
    n_rows = nrow(data),
    n_cols = ncol(data),
    original_n_rows = original_n_rows,
    feature_names = feature_names,
    numeric_features = numeric_features,
    categorical_features = categorical_features,
    was_sampled = original_n_rows > config$sample_size
  )

  class(audit_data) <- "audit_data"
  return(audit_data)
}

#' Stratified sampling helper
#' @keywords internal
stratified_sample <- function(target_vec, n_sample) {
  if (length(unique(target_vec)) <= 1) {
    return(sample(length(target_vec), n_sample))
  }

  # Calculate proportional sample sizes
  target_table <- table(target_vec)
  target_props <- target_table / sum(target_table)
  target_samples <- round(target_props * n_sample)

  # Ensure we don't exceed available samples
  target_samples <- pmin(target_samples, target_table)

  # Sample from each stratum
  sampled_indices <- c()
  for (level in names(target_samples)) {
    if (target_samples[level] > 0) {
      level_indices <- which(target_vec == level)
      sampled_indices <- c(sampled_indices,
                           sample(level_indices, target_samples[level]))
    }
  }

  return(sampled_indices)
}

#' Run detectors with optional parallel execution
#' @keywords internal
run_detectors <- function(detectors, audit_data, config) {

  run_single_detector <- function(detector_name) {
    detector_info <- .detector_registry[[detector_name]]
    if (is.null(detector_info)) {
      stop("Detector '", detector_name, "' not found in registry")
    }

    result <- detector_info$func(audit_data, config)
    result$detector_name <- detector_name
    return(result)
  }

  if (config$parallel && requireNamespace("parallel", quietly = TRUE)) {
    # Parallel execution
    cl <- parallel::makeCluster(parallel::detectCores() - 1)
    on.exit(parallel::stopCluster(cl))

    results <- parallel::parLapply(cl, detectors, run_single_detector)
  } else {
    # Sequential execution
    results <- lapply(detectors, run_single_detector)
  }

  names(results) <- detectors
  return(results)
}

#' Enhanced report compilation with numeric severity scores
#' @keywords internal
compile_report <- function(results, audit_data, config) {

  # Collect all issues with enhanced processing
  all_issues <- data.frame()
  all_evidence <- list()

  severity_scores <- c("critical" = 4, "high" = 3, "medium" = 2, "low" = 1)

  for (detector_name in names(results)) {
    result <- results[[detector_name]]

    if (nrow(result$issues) > 0) {
      result$issues$detector <- detector_name

      # Add numeric severity scores
      if (config$numeric_severity) {
        result$issues$severity_score <- severity_scores[result$issues$severity]
      }

      all_issues <- rbind(all_issues, result$issues)
    }

    all_evidence[[detector_name]] <- result$evidence
  }

  # Enhanced sorting
  if (nrow(all_issues) > 0) {
    if (config$numeric_severity) {
      all_issues <- all_issues[order(-all_issues$severity_score, all_issues$detector), ]
    } else {
      severity_order <- c("critical", "high", "medium", "low")
      all_issues$severity <- factor(all_issues$severity, levels = severity_order)
      all_issues <- all_issues[order(all_issues$severity, all_issues$detector), ]
    }
    rownames(all_issues) <- NULL
  }

  # Enhanced metadata
  meta <- list(
    n_detectors = length(results),
    n_issues = nrow(all_issues),
    data_shape = c(audit_data$n_rows, audit_data$n_cols),
    original_data_shape = c(audit_data$original_n_rows, audit_data$n_cols),
    was_sampled = audit_data$was_sampled,
    detectors_run = names(results),
    timestamp = Sys.time(),
    config_used = config
  )

  # Create report object
  report <- list(
    summary = all_issues,
    evidence = all_evidence,
    meta = meta
  )

  class(report) <- "leakr_report"
  return(report)
}

#' Enhanced summarise with better formatting
#'
#' @param report leakr_report object from leakr_audit()
#' @param top_n Maximum number of issues to display
#' @param show_config Whether to show configuration details
#' @keywords internal
#' @return data.frame with summary of top issues
#' @export
leakr_summarise <- function(report, top_n = 10, show_config = FALSE) {

  if (!inherits(report, "leakr_report")) {
    stop("report must be a leakr_report object")
  }

  cat("Leakage Audit Report\n")
  cat("===================\n")
  cat("Data shape:", paste(report$meta$data_shape, collapse = " x "), "\n")

  if (report$meta$was_sampled) {
    cat("(Sampled from:", paste(report$meta$original_data_shape, collapse = " x "), ")\n")
  }

  cat("Detectors run:", paste(report$meta$detectors_run, collapse = ", "), "\n")
  cat("Timestamp:", format(report$meta$timestamp), "\n\n")

  if (nrow(report$summary) == 0) {
    cat("✓ No leakage issues detected.\n")
    return(invisible(data.frame()))
  }

  # Summary statistics
  cat("Issues Summary:\n")
  severity_counts <- table(report$summary$severity)
  for (level in c("critical", "high", "medium", "low")) {
    if (level %in% names(severity_counts)) {
      cat(sprintf("  %s: %d\n", stringr::str_to_title(level), severity_counts[level]))
    }
  }
  cat("\n")

  # Top issues
  summary_df <- head(report$summary, top_n)
  cols_to_show <- c("detector", "severity", "issue_type", "description")
  if ("severity_score" %in% names(summary_df)) {
    cols_to_show <- c("severity_score", cols_to_show)
  }

  cat("Top Issues:\n")
  print(summary_df[, cols_to_show])

  if (show_config) {
    cat("\nConfiguration Used:\n")
    cat(paste(capture.output(str(report$meta$config_used)), collapse = "\n"))
  }

  return(invisible(summary_df))
}

#' Print method for leakr_report
#' @param x leakr_report object
#' @param ... Additional arguments passed to leakr_summarise
#' @keywords internal
#' @export
print.leakr_report <- function(x, ...) {
  leakr_summarise(x, ...)
}

#' Initialise built-in detectors
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Initialise built-in detectors when package loads
  register_detector("train_test_contamination", detect_train_test_contamination,
                    "Detects overlap and distributional differences between train/test sets")
  register_detector("target_leakage", detect_target_leakage,
                    "Identifies suspicious correlations between features and target")
  register_detector("duplication_detection", detect_duplication_leakage,
                    "Finds exact and near-duplicate rows in datasets")
}
