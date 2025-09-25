# Suppress R CMD check notes for global variables
utils::globalVariables(c("summary_df", "cols_to_show", "show_config", "report"))

#' @title Audit dataset for data leakage
#'
#' @description
#' This function audits a dataset for potential data leakage, running a series of predefined detectors
#' and generating a comprehensive report with detailed findings.
#'
#' @param data The dataset to be audited (data frame or tibble).
#' @param target The target variable (optional). If NULL, no target variable is assumed.
#' @param split The split variable used for training/test split (optional). If NULL, no split is assumed.
#' @param id The unique identifier for each row (optional). If NULL, no id is used.
#' @param detectors A vector of detector names to run (optional). If NULL, all available detectors will be used.
#' @param config A list of configuration parameters for the audit. Defaults to an empty list.
#'
#' @return A `leakr_report` object containing the audit results, including summary, evidence, and metadata.
#'
#' @examples
#' \dontrun{
#' # Example of running an audit on a dataset
#' report <- leakr_audit(my_data, target = "target_column", split = "split_column")
#' }
#'
#' @export
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
  # Ensure config has all required defaults
  if (is.null(config$sample_size)) config$sample_size <- 50000
  if (is.null(config$correlation_threshold)) config$correlation_threshold <- 0.8
  if (is.null(config$contamination_threshold)) config$contamination_threshold <- 0.1

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

#' @title Registry-based Detector System
#'
#' @description This section of the package manages a registry for various data leakage detectors. Detectors are stored in the `.detector_registry` environment and are accessible by name. The system allows for easy registration of detectors, providing their descriptions and registration times. Detectors can be queried by name or listed.
#'
#' @keywords internal
#' @name detector_registry

# This environment will store the detectors, allowing for easy access by name
# Create the detector registry environment
.detector_registry <- new.env(parent = emptyenv())

# Register detectors by adding them to the `.detector_registry` environment
assign("file_format",
       list(description = "Auto-registered detector file_format",
            registered_at = Sys.time()),
       envir = .detector_registry)

assign("train_test_contamination",
       list(description = "Checks for overlap between train/test sets",
            registered_at = Sys.time()),
       envir = .detector_registry)

assign("target_leakage",
       list(description = "Detects suspicious feature-target relationships",
            registered_at = Sys.time()),
       envir = .detector_registry)

assign("duplication_detection",
       list(description = "Detects duplicate rows",
            registered_at = Sys.time()),
       envir = .detector_registry)

#' @title List Registered Detectors
#' @description
#' Returns the names of all detectors currently registered in the system. This is useful for checking which detectors are available.
#'
#' @return A character vector containing the names of all registered detectors.
#'
#' @examples
#' # List all registered detectors
#' list_registered_detectors()
#'
#' @export
list_registered_detectors <- function() {
  # Returns a list of names of all registered detectors
  names(.detector_registry)
}


#' @title Robust data validation and preprocessing
#'
#' @description
#' This function performs data validation and preprocessing for audit purposes. It checks the validity of the input data,
#' ensures that the target and ID columns exist, and handles empty or problematic columns.
#'
#' @param data A data frame, tibble, or data table to be validated and preprocessed.
#' @param target The name of the target column, which should be present in the `data`. If `NULL`, no target validation is performed.
#' @param split A vector specifying the split column, which will be checked in the `data`. If `NULL`, no split validation is performed.
#' @param id The name of the ID column, which should be present in the `data`. If `NULL`, no ID validation is performed.
#'
#' @return The validated and preprocessed data.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' # Example data
#' data <- data.frame(target = rnorm(100), id = 1:100)
#' target <- "target"
#' id <- "id"
#' validated_data <- validate_and_preprocess_data(data, target, NULL, id)
#' }
#'
#' @export
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

#' @title Enhanced data preparation with robust preprocessing
#'
#' @description
#' This function performs robust data preprocessing and prepares the data for leakage detection.
#' It handles intelligent sampling, adjusts for the presence of a target variable, and structures
#' the data for further audit and analysis.
#'
#' @param data A data frame containing the dataset to be audited.
#' @param target The name of the target variable (optional). Used for stratified sampling if provided.
#' @param split A vector or a column name specifying the data split (e.g., training/test split).
#' @param id The unique identifier column for the dataset (optional).
#' @param config A list of configuration settings, including sample size and other audit parameters.
#'
#' @return A list of class `audit_data` containing preprocessed data along with metadata, such as:
#'   - `data`: The processed data.
#'   - `target`: The target variable name.
#'   - `split`: The split vector or column name.
#'   - `n_rows`: The number of rows in the data.
#'   - `n_cols`: The number of columns in the data.
#'   - `was_sampled`: A logical indicating whether sampling was performed.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' audit_data <- prepare_audit_data(data, target = "target_column", 
#'                                  split = "train_test_split", 
#'                                  id = "id_column", 
#'                                  config = list(sample_size = 50000))
#' }
#'
#' @export
prepare_audit_data <- function(data, target, split, id, config) {
  original_n_rows <- nrow(data)

  # Intelligent sampling for large datasets
  if (is.null(config$sample_size) || length(config$sample_size) == 0) config$sample_size <- 50000
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

#' @title Stratified sampling helper
#'
#' @description
#' This function performs stratified sampling based on the provided target vector.
#' The sampling is done proportionally to the distribution of values in the target vector.
#'
#' @param target_vec A vector representing the target variable used for stratification.
#'   The function will sample from each class (level) proportionally.
#' @param n_sample The total number of samples to draw.
#'
#' @return A vector of indices representing the sampled observations.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' target <- sample(c("class1", "class2", "class3"), size = 100, replace = TRUE)
#' n_sample <- 30
#' sampled_indices <- stratified_sample(target, n_sample)
#' }
#'
#' @export
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

#' @title Enhanced report compilation with numeric severity scores
#'
#' @description This function compiles a report with enhanced sorting, severity scoring,
#' and detailed metadata, including configuration information.
#'
#' @param results A list containing detection results.
#' @param audit_data The audit data used for the report.
#' @param config Configuration settings, including whether to use numeric severity scores.
#' @param show_config Logical, whether to display the configuration used for report generation. Defaults to FALSE.
#' @param top_n Numeric, the number of top results to display in the report. Defaults to 10.
#' @param report A string indicating the type of report to generate. Defaults to "default".
#'
#' @return A `leakr_report` object containing the summary, evidence, and metadata for the report.
#'
#' @examples
#' \dontrun{
#' compile_report(results, audit_data, config, show_config = TRUE, top_n = 5, report = "detailed")
#' }
#'
#' @export
compile_report <- function(results, audit_data, config, show_config = FALSE, top_n = 10, report = "default") {
  # Function code remains the same
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

#' @title Enhanced summarise with better formatting
#'
#' @description
#' This function provides a formatted summary of the leakage audit report. It displays
#' a summary of the leakage issues, including the severity and top issues detected.
#' Optionally, it can also display configuration details used for the audit.
#'
#' @param report A `leakr_report` object from `leakr_audit()`.
#' @param top_n Maximum number of issues to display in the summary. Defaults to 10.
#' @param show_config Whether to display the configuration details used for the audit. Defaults to `FALSE`.
#' @param config (Optional) A configuration list. This argument is not used directly in the function,
#'   but is referenced in the report metadata. Defaults to `NULL`.
#' @param audit_data (Optional) The data used for auditing. This argument is not used directly in the function,
#'   but is part of the report metadata. Defaults to `NULL`.
#' @param detectors (Optional) A vector of detectors used for the audit. This argument is not used directly in
#'   the function but is part of the report metadata. Defaults to `NULL`.
#' @param libname (Optional) The name of the library. This is included for internal package functionality.
#' @param pkgname (Optional) The name of the package. This is included for internal package functionality.
#'
#' @return An invisible `data.frame` summarizing the top `n` issues detected.
#'
#' @examples
#' \dontrun{
#' # Example of summarising the report
#' summarise_report <- leakr_summarise(report)
#' }
#'
#' @keywords internal
#' @export
leakr_summarise <- function(report, top_n = 10, show_config = FALSE, config = NULL,
                            audit_data = NULL, detectors = NULL, libname = NULL, pkgname = NULL) {
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
    cat("\u2713 No leakage issues detected.\n")
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

#' @title Print method for leakr_report
#'
#' @keywords internal
#' @param x leakr_report object
#' @param ... TODO: Add description
print.leakr_report <- function(x, ...) {
  leakr_summarise(x, ...)
}
#'
#' Initialise built-in detectors
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # Initialise built-in detectors when package loads
  # Register train/test detector
  register_detector(
    "train_test",
    train_test_detector,
    "Detects overlap and distributional differences between train/test sets"
  )
  # Register correlation detector
  register_detector(
    "correlation",
    correlation_detector,
    "Identifies suspicious correlations between features and target"
  )
  # Register duplicate detector
  register_detector(
    "duplicate",
    duplicate_detector,
    "Finds exact and near-duplicate rows in datasets"
  )
}

#' @title Run multiple detectors on audit data
#'
#' @description
#' This function runs multiple leakage detectors on the provided audit data and
#' returns the results for each detector.
#'
#' @param detectors A list of detector configurations. Each detector can be either a function
#'   or an object that contains a `func` field with the detector function.
#' @param audit_data A data.frame, tibble, or data.table to audit.
#' @param config A list of configuration settings to be passed to each detector.
#'
#' @return A list where each element contains the results of running a detector. If a detector
#'   fails, an error message is included in the result.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' detectors <- list(
#'   temporal = list(func = temporal_detector_func),
#'   train_test = new_train_test_detector()
#' )
#' results <- run_detectors(detectors, audit_data = iris, config = list(sample_size = 50000))
#' }
#'
#' @export
run_detectors <- function(detectors, audit_data, config) {
  if (is.null(detectors) || length(detectors) == 0) {
    return(list())
  }
  results <- list()
  for (detector_name in names(detectors)) {
    detector_info <- detectors[[detector_name]]
    tryCatch({
      # Handle different detector info structures
      if (is.list(detector_info) && 'func' %in% names(detector_info)) {
        # New style: detector_info has $func
        if (is.function(detector_info$func)) {
          result <- detector_info$func(audit_data, config)
          results[[detector_name]] <- result
        } else {
          warning('Detector ', detector_name, ' func is not a function')
          results[[detector_name]] <- list(error = 'Invalid function')
        }
      } else if (is.function(detector_info)) {
        # Old style: detector_info is directly a function
        result <- detector_info(audit_data, config)
        results[[detector_name]] <- result
      } else {
        # Try to get detector by name
        if (detector_name == 'temporal' && exists('new_temporal_detector')) {
          # Create and run temporal detector
          if ('date' %in% names(audit_data$data) || 'Date' %in% names(audit_data$data)) {
            time_col <- if ('date' %in% names(audit_data$data)) 'date' else 'Date'
            det <- new_temporal_detector(time_col = time_col)
            result <- run_detector(det, audit_data$data, split = audit_data$split)
            results[[detector_name]] <- result
          }
        } else if (detector_name == 'train_test' && exists('new_train_test_detector')) {
          # Create and run train-test detector
          det <- new_train_test_detector()
          result <- run_detector(det, audit_data$data, split = audit_data$split)
          results[[detector_name]] <- result
        } else {
          warning('Unknown detector: ', detector_name)
          results[[detector_name]] <- list(error = 'Unknown detector')
        }
      }
    }, error = function(e) {
      warning('Detector ', detector_name, ' failed: ', e$message)
      results[[detector_name]] <- list(error = e$message)
    })
  }
  return(results)
}