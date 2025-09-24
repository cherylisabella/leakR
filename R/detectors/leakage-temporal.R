#' @title Temporal Leakage Detector
#' @description Detects lookahead leakage from future data leaking into training splits.
#' @keywords internal

#' Constructor enforcing proper parameter types and class
#' @param time_col The time column name.
#' @param lookahead_window Numeric difftime-compatible window (days for Date, seconds for POSIXt).
#' @return S3 object "temporal_detector"
new_temporal_detector <- function(time_col = NULL, lookahead_window = 1) {
  if (!is.character(time_col) || length(time_col) != 1) {
    stop("Parameter 'time_col' must be a single character string.")
  }
  if (!is.numeric(lookahead_window) || lookahead_window <= 0) {
    stop("Parameter 'lookahead_window' must be a positive numeric value.")
  }
  create_detector(
    name = "temporal_leakage",
    params = list(time_col = time_col, lookahead_window = lookahead_window),
    class = "temporal_detector"
  )
}

#' run_detector for temporal_detector class
#' Detect temporal leakage looking for test samples earlier than max train time + window.
#' @inheritParams run_detector
#' @return List with issues and evidence
run_detector.temporal_detector <- function(detector, data, target = NULL, split = NULL, id = NULL, config = list()) {
  stopifnot(is.data.frame(data))
  params <- detector$params
  time_col <- params$time_col
  if (!(time_col %in% names(data))) {
    stop(sprintf("Time column '%s' not found in data.", time_col))
  }
  times <- data[[time_col]]
  if (!inherits(times, c("Date", "POSIXt"))) {
    stop("Time column must be Date or POSIXt class.")
  }
  if (is.null(split)) stop("Parameter 'split' is required for temporal leakage detection.")

  train_idx <- which(split == "train")
  test_idx <- which(split == "test")
  max_train_time <- max(times[train_idx], na.rm = TRUE)

  # Convert numeric window to difftime matching type
  lookahead <- if (inherits(times, "Date")) {
    as.difftime(params$lookahead_window, units = "days")
  } else {
    as.difftime(params$lookahead_window, units = "secs")
  }

  leakage_indices <- test_idx[times[test_idx] <= (max_train_time + lookahead)]
  n_leak <- length(leakage_indices)

  # Graded severity by count of leaks
  severity <- if (n_leak >= 20) "high" else if (n_leak >= 5) "medium" else if (n_leak > 0) "low" else "low"
  description <- if (n_leak > 0) {
    sprintf("Detected %d test samples with timestamps <= max training time plus lookahead window.", n_leak)
  } else {
    "No temporal leakage detected."
  }
  suggested_fix <- if (n_leak > 0) {
    "Adjust temporal splits to prevent future leakage."
  } else {
    "No action needed."
  }

  issues <- data.frame(
    detector = detector$name,
    issue_type = "temporal_lookahead_leakage",
    severity = severity,
    description = description,
    suggested_fix = suggested_fix,
    stringsAsFactors = FALSE
  )

  list(
    issues = issues,
    evidence = list(
      leakage_indices = leakage_indices,
      max_train_time = max_train_time,
      lookahead_window = lookahead
    ),
    detector_name = detector$name
  )
}
