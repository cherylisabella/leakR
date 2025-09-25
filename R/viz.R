#' @title Plot a detector_result object
#' @return A ggplot object, invisibly. Printed if interactive
#' @export
#' @param x A detector_result object
#' @param palette Optional ggplot2 discrete palette
#' @param x A detector_result object
#'
#' @title Plot a detector_result object
#'
#' @return A ggplot object, invisibly. Printed if interactive
#' @export
#' @param x A detector_result object
#' @param palette Optional ggplot2 discrete palette
plot.detector_result <- function(x, palette = NULL, ...) {
  if (!inherits(x, "detector_result")) stop("Expected a detector_result object")
  issues <- x$issues
  if (is.null(issues) || nrow(issues) == 0) {
    message("No issues detected, nothing to plot.")
    return(invisible(NULL))
  }
  df <- data.frame(
    description = issues$description,
    severity = factor(issues$severity,
                      levels = c("low", "medium", "high", "critical"),
                      ordered = TRUE)
  )
  p <- ggplot2::ggplot(df, ggplot2::aes(x = severity, fill = severity)) +
    ggplot2::geom_bar() +
    ggplot2::labs(title = "Leakage Issues by Severity",
                  x = "Severity", y = "Count") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(legend.position = "none")
  if (!is.null(palette)) {
    p <- p + ggplot2::scale_fill_brewer(palette = palette)
  }
  if (interactive()) print(p)
  invisible(p)
#' @param x TODO: Document
#' @param palette TODO: Document
#' @param ... TODO: Document
#' @param x TODO: Document
#' @param palette TODO: Document
#' @param ... TODO: Document
#' @param x TODO: Document
#' @param palette TODO: Document
#' @param x TODO: Document
}
#' @title Plot a udld_report object
#'
#' @description
#' This function generates a bar plot of leakage issues detected by different detectors.
#' The plot displays the count of issues by severity level for each detector in a `udld_report` object.
#' 
#' @param x A `udld_report` object. This object contains the detectors and their associated issues.
#' @param palette Optional. A `ggplot2` discrete palette for coloring the bars based on severity.
#' @param ... Additional arguments passed to `ggplot2` functions or other methods. These are typically
#'   used for customizing the plot further.
#'
#' @return A `ggplot` object, invisibly. The plot is printed if the session is interactive.
#'
#' @examples
#' \dontrun{
#' # Assuming 'udld_report' object is available
#' plot(udld_report, palette = "Set3")
#' }
#' 
#' @export
plot.udld_report <- function(x, palette = NULL, ...) {
  if (!inherits(x, "udld_report")) stop("Expected a udld_report object")
  issues <- do.call(rbind, lapply(x$detectors, function(d) d$issues))
  if (is.null(issues) || nrow(issues) == 0) {
    message("No issues detected, nothing to plot.")
    return(invisible(NULL))
  }
  df <- data.frame(
    detector = rep(names(x$detectors),
                   lengths(lapply(x$detectors, function(d) nrow(d$issues)))),
    severity = factor(issues$severity,
                      levels = c("low", "medium", "high", "critical"),
                      ordered = TRUE)
  )
  p <- ggplot2::ggplot(df, ggplot2::aes(x = detector, fill = severity)) +
    ggplot2::geom_bar(position = "dodge") +
    ggplot2::labs(title = "Leakage Issues by Detector",
                  x = "Detector", y = "Count") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
  if (!is.null(palette)) {
    p <- p + ggplot2::scale_fill_brewer(palette = palette)
  }
  if (interactive()) print(p)
  invisible(p)
}

