# pkg-detectors.R
#' @title Detector Registry for leakr Package
#' @description
#' Provides functions to register, retrieve, and manage detector functions within the leakr package.
#' Allows modular addition of detector functions identified by unique names with descriptive metadata.
#' This registry supports dynamic lookup and usage of detectors in the package.
#'
#' @keywords internal
#' @name detector_registry
NULL

# Internal environment to store registered detectors
.detector_registry <- new.env(parent = emptyenv())

#' Register a detector function
#'
#' @param name Character string giving a unique detector name
#' @param fun Function implementing the detector logic
#' @param description Character string describing the detector
#' @return Invisibly TRUE if register succeeds
#' @keywords internal
register_detector <- function(name, fun, description = "") {
  stopifnot(is.character(name), length(name) == 1)
  stopifnot(is.function(fun))
  assign(name, list(fun = fun, description = description), envir = .detector_registry)
  invisible(TRUE)
}

#' Retrieve a registered detector by name
#'
#' @param name Character string of the detector name to retrieve
#' @return A list with elements 'fun' (function) and 'description' (character)
#' @keywords internal
get_detector <- function(name) {
  if (exists(name, envir = .detector_registry, inherits = FALSE)) {
    get(name, envir = .detector_registry)
  } else {
    stop("Detector not registered: ", name)
  }
}