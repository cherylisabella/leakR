#' @title Detector Registry for leakr Package
#' @description Provides registration and retrieval of detector functions within leakr.
#' @name ____TEMP_FIX
#' @keywords internal
#' @keywords internal
NULL
.detector_registry <- new.env(parent = emptyenv())
#' Register a new detector
#'
#' Register a new data leakage detector function
#'
#' @return Invisibly returns registration status
#' @export
#' @examples
#' \dontrun{
#' my_detector <- function(data) { return(list()) }
#' register_detector("my_detector", my_detector)
#' }
#' @param name Name of the detector
#' @param fun TODO: Add description
#' @param description TODO: Add description

register_detector <- function(name, fun, description = "") {
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.function(fun))
  assign(name, list(fun = fun, description = description), envir = .detector_registry)
  invisible(TRUE)
}
get_detector <- function(name) {
  if (exists(name, envir = .detector_registry, inherits = FALSE)) {
    get(name, envir = .detector_registry)
  } else {
    stop("Detector not registered: ", name)
  }
}
