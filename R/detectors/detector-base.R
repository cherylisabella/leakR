#' @title Detector Base Class and Helpers
#' @description Base S3 constructor, validator, and utilities for detectors.
#' @keywords internal

# Low-level constructor: builds a new detector S3 object
new_detector <- function(name, params = list(), config = list()) {
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.list(params))
  stopifnot(is.list(config))

  structure(
    list(
      name = name,
      params = params,
      config = config
    ),
    class = c("leakr_detector", "list")
  )
}

# Validator: confirms object structure and class correctness
validate_detector <- function(detector) {
  if (!inherits(detector, "leakr_detector")) {
    stop("Object is not a 'leakr_detector'.")
  }
  stopifnot(is.character(detector$name) && length(detector$name) == 1)
  stopifnot(is.list(detector$params))
  invisible(TRUE)
}

# Friendly printer for detector objects
print.leakr_detector <- function(x, ...) {
  cat(sprintf("Detector: %s\n", x$name))
  if (length(x$params) == 0) {
    cat("No parameters.\n")
  } else {
    # Use str() for compact readable printing rather than deparse()
    cat("Parameters:\n")
    str(x$params, max.level = 2)
  }
  invisible(x)
}

# Generic run method for detectors - to be overridden per subclass
run_detector <- function(detector, data, target = NULL, split = NULL, id = NULL, config = list()) {
  UseMethod("run_detector")
}

# Default fallback: error if method not implemented
run_detector.default <- function(detector, ...) {
  stop("No run_detector method implemented for ", class(detector)[1])
}

# Convenience factory function that builds and validates a detector
create_detector <- function(name, params = list(), config = list()) {
  det <- new_detector(name, params, config)
  validate_detector(det)
  det
}
