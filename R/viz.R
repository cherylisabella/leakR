#' @title Visualisation Methods for leakr Reports
#' @description Plotting helpers for detector results and reports.
#' @keywords internal
#' @import ggplot2
#'
#' @param result A detector_result object or udld_report.
#' @param palette Character, optional ggplot2 discrete palette (default = NULL).
#' @param ... Additional arguments passed to plotting functions.
#' @return A ggplot object, invisibly. Printed if interactive.
#' @export
plot.detector_result <- function(result, palette = NULL, ...) {
  if (!inherits(result, "detector_result")) {
    stop("Expected a detector_result object")
  }
  issues <- result$issues
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

  p <- ggplot(df, aes(x = severity, fill = severity)) +
    geom_bar() +
    labs(title = "Leakage Issues by Severity",
         x = "Severity",
         y = "Count") +
    theme_minimal(base_size = 12) +
    theme(legend.position = "none")

  if (!is.null(palette)) {
    p <- p + scale_fill_brewer(palette = palette)
  }

  if (interactive()) print(p)
  invisible(p)
}

#' @export
plot.udld_report <- function(result, palette = NULL, ...) {
  if (!inherits(result, "udld_report")) {
    stop("Expected a udld_report object")
  }

  issues <- do.call(rbind, lapply(result$detectors, function(d) d$issues))
  if (is.null(issues) || nrow(issues) == 0) {
    message("No issues detected, nothing to plot.")
    return(invisible(NULL))
  }

  df <- data.frame(
    detector = rep(names(result$detectors),
                   lengths(lapply(result$detectors, function(d) nrow(d$issues)))),
    severity = factor(issues$severity,
                      levels = c("low", "medium", "high", "critical"),
                      ordered = TRUE)
  )

  p <- ggplot(df, aes(x = detector, fill = severity)) +
    geom_bar(position = "dodge") +
    labs(title = "Leakage Issues by Detector",
         x = "Detector",
         y = "Count") +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))

  if (!is.null(palette)) {
    p <- p + scale_fill_brewer(palette = palette)
  }

  if (interactive()) print(p)
  invisible(p)
}
