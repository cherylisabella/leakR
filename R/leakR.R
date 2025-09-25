# Prevent R CMD check warnings about variables used in NSE contexts
if (getRversion() >= "2.15.1") {
  utils::globalVariables(c("severity", "detector", "issue_id", "evidence", "suggested_fix"))
}

#' leakr: Data Leakage Detection for Machine Learning in R
#'
#' The leakr package provides tools to automatically detect common data leakage
#' patterns in machine learning workflows for tabular data. It identifies
#' train/test contamination, target leakage, and duplicate rows with clear
#' diagnostic reports and visualisations.
#'
#' @section Key Features:
#' \itemize{
#' \item \strong{Train/Test Contamination}: Detects ID overlaps and distributional
#' shifts between training and test sets
#' \item \strong{Target Leakage}: Identifies features with suspicious correlations
#' to the target variable
#' \item \strong{Duplication Detection}: Finds exact and near-duplicate rows
#' \item \strong{Clear Reports}: Generates severity-ranked diagnostics with
#' actionable recommendations
#' \item \strong{Visualisations}: Creates diagnostic plots to highlight issues
#' }
#'
#' @section Main Functions:
#' \itemize{
#' \item \code{\link{leakr_audit}}: Main function for comprehensive leakage detection
#' \item \code{\link{leakr_summarise}}: Generate human-readable summaries
#' \item \code{\link{leakr_plot}}: Create diagnostic visualisations
#' }
#'
#' @section Built-in Detectors:
#' \itemize{
#' \item \code{train_test_contamination}: Checks for overlap between train/test sets
#' \item \code{target_leakage}: Identifies suspicious feature-target relationships
#' \item \code{duplication_detection}: Finds duplicate rows in datasets
#' }
#'
#' @section Data Compatibility:
#' Accepts \code{data.frame}, \code{tibble}, and \code{data.table} objects.
#'
#' @section Quick Start:
#' \preformatted{
#' # Audit a dataset for leakage
#' library(leakr)
#' report <- leakr_audit(my_data, target = "outcome")
#'
#' # View summary of issues found
#' leakr_summarise(report)
#'
#' # Create diagnostic plots
#' leakr_plot(report)
#' }
#'
#' @author
#' \strong{Maintainer}: Cheryl Isabella Lim \email{cheryl.academic@gmail.com}
#'
#' @seealso
#' \itemize{
#' \item \url{https://github.com/cherylisabella/leakr}
#' \item Report bugs at \url{https://github.com/cherylisabella/leakr/issues}
#' }
#'
#' @docType package
#' @name leakr
#' @title leakr: Data Leakage Detection for Machine Learning in R
#' @aliases leakr-package
"_PACKAGE"
#' Get detector information
#'
#' Retrieves information about detectors, optionally filtering by the detector name.
#'
#' @param name Optional detector name. If NULL, returns info for all detectors.
#'
#' @return A list with detector information, including description and registration date.
#'
#' @keywords internal
#'
#' @examples
#' # Get information for all detectors
#' get_detector_info()
#'
#' # Get information for specific detectors that actually exist
#' get_detector_info("file_format")
#'
#' @export
get_detector_info <- function(name = NULL) {
  if (is.null(name)) {
    return(lapply(.detector_registry, function(x) list(description = x$description)))
  }
  if (!name %in% names(.detector_registry)) {
    stop("Detector '", name, "' not found. Available detectors: ", 
         paste(names(.detector_registry), collapse = ", "))
  }
  result <- .detector_registry[[name]][c("description", "registered_at")]
  # Clean up any NA names
  names(result) <- names(result)[!is.na(names(result))]
  return(result)
}
