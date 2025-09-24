# Prevent R CMD check warnings about variables used in NSE contexts
if (getRversion() >= "2.15.1") {
  utils::globalVariables(c("severity", "detector", "issue_id", "evidence", "suggested_fix"))}

#' leakR: Data Leakage Detection for Machine Learning in R
#'
#' The leakR package provides tools to automatically detect common data leakage
#' patterns in machine learning workflows for tabular data. It identifies
#' train/test contamination, target leakage, and duplicate rows with clear
#' diagnostic reports and visualisations.
#'
#' @section Key Features:
#' \itemize{
#'   \item \strong{Train/Test Contamination}: Detects ID overlaps and distributional
#'     shifts between training and test sets
#'   \item \strong{Target Leakage}: Identifies features with suspicious correlations
#'     to the target variable
#'   \item \strong{Duplication Detection}: Finds exact and near-duplicate rows
#'   \item \strong{Clear Reports}: Generates severity-ranked diagnostics with
#'     actionable recommendations
#'   \item \strong{Visualisations}: Creates diagnostic plots to highlight issues
#' }
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{leakr_audit}}: Main function for comprehensive leakage detection
#'   \item \code{\link{leakr_summarise}}: Generate human-readable summaries
#'   \item \code{\link{leakr_plot}}: Create diagnostic visualisations
#' }
#'
#' @section Built-in Detectors:
#' \itemize{
#'   \item \code{train_test_contamination}: Checks for overlap between train/test sets
#'   \item \code{target_leakage}: Identifies suspicious feature-target relationships
#'   \item \code{duplication_detection}: Finds duplicate rows in datasets
#' }
#'
#' @section Data Compatibility:
#' Accepts \code{data.frame}, \code{tibble}, and \code{data.table} objects.
#'
#' @section Quick Start:
#' \preformatted{
#' # Audit a dataset for leakage
#' library(leakR)
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
#'   \item \url{https://github.com/cherylisabella/leakR}
#'   \item Report bugs at \url{https://github.com/cherylisabella/leakR/issues}
#' }
#'
#' @docType package
#' @name leakR
#' @aliases leakR-package
"_PACKAGE"

# Package startup message
.onAttach <- function(libname, pkgname) {
  packageStartupMessage(
    "leakR v", utils::packageVersion("leakR"), "\n",
    "Data Leakage Detection for Machine Learning\n",
    "Type 'vignette(\"leakr-intro\")' to get started")}
