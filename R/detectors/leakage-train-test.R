#' @title Train-Test Contamination Detector
#' @description Detects overlap contamination between train/test splits.
#' @keywords internal

#' Core detection function
#' @param data data.frame containing dataset to check
#' @param split character vector indicating train/test partition
#' @param id optional identifier column name
#' @param config optional list for future settings
#' @return list with issues data.frame and evidence list
detect_train_test_contamination <- function(data, split, id = NULL, config = list()) {
  stopifnot(is.data.frame(data))
  stopifnot(is.character(split))
  if (!is.null(id)) stopifnot(id %in% names(data))

  train_idx <- which(split == "train")
  test_idx <- which(split == "test")

  overlap_count <- 0L
  overlap_info <- list()

  if (!is.null(id) && id %in% names(data)) {
    train_ids <- unique(data[[id]][train_idx])
    test_ids <- unique(data[[id]][test_idx])
    overlap <- intersect(train_ids, test_ids)
    overlap_count <- length(overlap)
    overlap_info$overlap_ids <- overlap
  } else {
    requireNamespace("digest", quietly = TRUE)
    row_hashes <- vapply(seq_len(nrow(data)), function(i) digest::digest(data[i, , drop = FALSE]), character(1))
    train_hashes <- unique(row_hashes[train_idx])
    test_hashes <- unique(row_hashes[test_idx])
    overlap <- intersect(train_hashes, test_hashes)
    overlap_count <- length(overlap)
    overlap_info$overlap_hashes <- overlap
  }

  severity <- ifelse(overlap_count > 0, "critical", "low")
  issues <- data.frame(
    detector = "train_test_contamination",
    issue_type = "overlap",
    severity = severity,
    description = ifelse(overlap_count > 0,
                         sprintf("Detected %d overlapping IDs or rows between train/test.", overlap_count),
                         "No overlap detected between train and test sets."),
    suggested_fix = ifelse(overlap_count > 0,
                           "Remove overlapping records or properly split data.",
                           "No action needed."),
    stringsAsFactors = FALSE
  )

  list(
    issues = issues,
    evidence = overlap_info,
    detector_name = "train_test_contamination"
  )
}


#' Constructor setting class and validating threshold param.
#' @param threshold Numeric between 0 and 1 for subsetting (future)
#' @return S3 object of class "train_test_detector"
new_train_test_detector <- function(threshold = 0.7) {
  if (!is.numeric(threshold) || threshold <= 0 || threshold > 1) {
    stop("Parameter 'threshold' must be numeric in (0,1].")
  }
  create_detector(
    name = "train_test_contamination",
    params = list(threshold = threshold),
    class = "train_test_detector"
  )
}

#' run_detector for train_test_detector class
#' Wraps detect_train_test_contamination to conform to S3 generic
#' @inheritParams run_detector
#' @return List with issues data.frame and evidence list.
run_detector.train_test_detector <- function(detector, data, target = NULL, split = NULL, id = NULL, config = list()) {
  if (is.null(split)) stop("Parameter 'split' is required for train/test contamination detection.")
  detect_train_test_contamination(data, split, id, config)
}


