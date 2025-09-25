#' @title Report generator
#' @description Generate an executive summary text for the leakage audit report.
#'
#' @return Formatted summary string (Markdown/HTML-friendly).
#' @keywords internal
#' @param report A 'leakr_report' object containing summarized issues.
generate_executive_summary_text <- function(report) {
  if (nrow(report$summary) == 0) {
    return("No data leakage issues were detected in your dataset. The data appears suitable for machine learning without immediate remediation.")
  }
  # Enforce fixed severity levels to control order and completeness
  severity_levels <- c("critical", "high", "medium", "low")
  report$summary$severity <- factor(report$summary$severity, levels = severity_levels)
  severity_counts <- table(report$summary$severity)
  detector_counts <- table(report$summary$detector)
  total_issues <- sum(severity_counts)
  risk_info <- determine_risk_level(severity_counts)
  parts <- c(
    sprintf("**Overall Risk Level:** <span class='%s'>%s</span>", risk_info$class, risk_info$level),
    sprintf("**Total Issues Found:** %s", format(total_issues, big.mark = ",")),
    "",
    "**Breakdown by Severity:**"
  )
  for (sev in severity_levels) {
    count <- severity_counts[[sev]]
    if (!is.na(count) && count > 0) {
      parts <- c(parts,
                 sprintf("- %s: %d issue%s", tools::toTitleCase(sev), count, ifelse(count > 1, "s", "")))
    }
  }
  if (length(detector_counts) > 0) {
    top_detector <- names(detector_counts)[which.max(detector_counts)]
    count <- detector_counts[[top_detector]]
    parts <- c(parts,
               "",
               sprintf("**Primary Concern:** %s (%d issue%s)", format_detector_name(top_detector),
                       count, ifelse(count > 1, "s", "")))
  }
  paste(parts, collapse = "\n")
}

#' @title Determine risk level and CSS class from severity counts.

#' @return List with 'level' and CSS 'class'.
#' @keywords internal

#' @param severity_counts Named integer vector of severity frequencies.
determine_risk_level <- function(severity_counts) {
  if ("critical" %in% names(severity_counts) && severity_counts["critical"] > 0) {
    list(level = "HIGH", class = "risk-high")
  } else if ("high" %in% names(severity_counts) && severity_counts["high"] > 0) {
    list(level = "ELEVATED", class = "risk-medium-high")  # refined naming and class
  } else if ("medium" %in% names(severity_counts) && severity_counts["medium"] > 0) {
    list(level = "MEDIUM", class = "risk-medium")
  } else {
    list(level = "LOW", class = "risk-low")
  }
}

#' @title Format detector names for display.
#' @description Format detector names by converting them to title case and separating words by spaces.
#'
#' @param detector_name A string to format, typically a detector name with underscores.
#' @return A title-cased, space-separated string.
#' @examples
#' \dontrun{
#' format_detector_name("some_detector_name")
#' }
#' @export
format_detector_name <- function(detector_name) {
  tools::toTitleCase(gsub("_", " ", detector_name))
}


#' @title Generate detailed issues section with output formatting and truncation.
#'
#' @return Formatted issues section string.
#' @keywords internal
#' @param report A 'leakr_report' object.
#' @param format TODO: describe
generate_issues_section <- function(report, format) {
  max_issues <- 50L
  n_issues <- nrow(report$summary)
  separator_len <- 30L  # shorter separator for better readability
  if (n_issues == 0) {
    return(ifelse(format == "html", '<div class="no-issues">No issues detected.</div>', "No issues detected."))
  }
  issues <- head(report$summary, max_issues)
  truncation_msg <- ""
  if (n_issues > max_issues) {
    truncation_msg <- switch(format,
                             html = sprintf("<p><em>Only showing first %d of %d issues.</em></p>", max_issues, n_issues),
                             markdown = sprintf("*Only showing first %d of %d issues.*", max_issues, n_issues),
                             text = sprintf("Only showing first %d of %d issues.", max_issues, n_issues))
  }
  # Define a helper to safely escape HTML text
  safe_html <- function(text) {
    if (requireNamespace("htmltools", quietly = TRUE)) {
      htmltools::htmlEscape(text)
    } else {
      # Fallback: minimal replacement
      gsub("&", "&amp;", gsub("<", "&lt;", gsub(">", "&gt;", text, fixed=TRUE), fixed=TRUE), fixed=TRUE)
    }
  }
  format_issue <- switch(format,
                         html = function(issue) {
                           sprintf(
                             '<div class="issue-box"><h3><span class="severity-%s">[%s]</span> %s</h3><p><strong>Detector:</strong> %s</p><p><strong>Description:</strong> %s</p><p><strong>Suggested Fix:</strong> %s</p></div>',
                             issue$severity,
                             toupper(issue$severity),
                             tools::toTitleCase(gsub("_", " ", issue$issue_type)),
                             tools::toTitleCase(gsub("_", " ", issue$detector)),
                             safe_html(issue$description),
                             safe_html(issue$suggested_fix)
                           )
                         },
                         markdown = function(issue) {
                           sprintf(
                             "### [%s] %s\n\n**Detector:** %s\n\n**Description:** %s\n\n**Suggested Fix:** %s\n",
                             toupper(issue$severity),
                             tools::toTitleCase(gsub("_", " ", issue$issue_type)),
                             tools::toTitleCase(gsub("_", " ", issue$detector)),
                             issue$description,
                             issue$suggested_fix
                           )
                         },
                         text = function(issue) {
                           paste0(
                             "[", toupper(issue$severity), "] ", tools::toTitleCase(gsub("_", " ", issue$issue_type)), "\n",
                             "Detector: ", tools::toTitleCase(gsub("_", " ", issue$detector)), "\n",
#' @param report TODO: Document
#' @param format TODO: Document
#' @param report TODO: Document
#' @param format TODO: Document
#' @param report TODO: Document
#' @param format TODO: Document
#' @param format TODO: Document
#' @param format TODO: Document
#' @param format TODO: Document
#' @param format TODO: Document
                             "Description: ", issue$description, "\n",
                             "Suggested Fix: ", issue$suggested_fix, "\n",
                             strrep("-", separator_len), "\n"
                           )
                         })
  issues_str <- vapply(seq_len(nrow(issues)), function(i) {
    format_issue(issues[i, ])
  }, character(1))
#' @title FIX ME
  paste(c(issues_str, truncation_msg), collapse = ifelse(format == "html", "\n", "\n\n"))
}

#' @title Generate evidence section with format-specific handling and DRY logic.
#'
#' @return Formatted evidence section string.
#' @keywords internal
#' @param report A leakr_report object.
#' @param format TODO: describe
generate_evidence_section <- function(report, format) {
  if (length(report$evidence) == 0) {
    return("No detailed evidence available.")
  }
  # Common helper for detector evidence formatting
  format_evidence <- function(evidence, detector_name, fmt) {
    # Shared duplication evidence processing
    process_duplication <- function(ev) {
      lines <- c()
      if (!is.null(ev$exact_duplicates)) {
        lines <- c(lines,
                   sprintf("- Exact Duplicates: %d groups, %d total rows",
                           ev$exact_duplicates$n_duplicate_groups,
                           ev$exact_duplicates$n_duplicate_rows))
      }
      if (!is.null(ev$near_duplicates)) {
        lines <- c(lines,
                   sprintf("- Near Duplicates: %d groups, %d total rows",
                           ev$near_duplicates$n_groups,
                           ev$near_duplicates$n_rows))
      }
      return(lines)
    }
    # Format lists to requested output format
    format_lines <- function(lines) {
      if (length(lines) == 0) return(NULL)
      switch(fmt,
             html = paste0("<ul>\n", paste0("<li>", lines, "</li>", collapse = "\n"), "\n</ul>"),
             markdown = paste0(paste0("- ", lines), collapse = "\n"),
             text = paste(lines, collapse = "\n"))
    }
    if (detector_name == "train_test_contamination") {
      lines <- c()
      if (!is.null(evidence$split_info)) {
        lines <- c(lines,
                   sprintf("Training set size: %d rows", evidence$split_info$train_size),
                   sprintf("Test set size: %d rows", evidence$split_info$test_size))
      }
      if (!is.null(evidence$hash_overlap)) {
        lines <- c(lines,
                   sprintf("Exact duplicates: %d duplicate rows (%.1f%%)",
                           evidence$hash_overlap$overlap_count,
                           evidence$hash_overlap$duplicate_percentage))
      }
      return(format_lines(lines))
    }
#' @param report TODO: Document
#' @param report TODO: Document
#' @param report TODO: Document
    if (detector_name == "duplication_detection") {
      dup_lines <- process_duplication(evidence)
      return(format_lines(dup_lines))
    }
    # Default fallback message
    switch(fmt,
           html = "<p><em>No evidence details currently available for this detector.</em></p>",
           markdown = "*No evidence details currently available for this detector.*",
           text = "No evidence details currently available for this detector.")
  }
  pieces <- lapply(names(report$evidence), function(det) {
    header <- switch(format,
                     html = sprintf("<h3>%s</h3>", format_detector_name(det)),
                     markdown = sprintf("### %s\n", format_detector_name(det)),
                     text = toupper(gsub("_", " ", det)))
    content <- format_evidence(report$evidence[[det]], det, format)
    paste(header, content, sep = ifelse(format == "html", "\n", "\n\n"))  })
  paste(pieces, collapse = ifelse(format == "html", "\n\n", "\n\n"))
}

#' @title Generate actionable recommendations based on report findings.
#'
#' @description This function generates actionable recommendations based on the findings in a `leakr_report` object.
#'
#' @param report A `leakr_report` object containing the summary of issues and metadata.
#'
#' @return A character vector of recommendations.
#'
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' recommendations <- generate_recommendations(report)
#' }
#'
#' @export
generate_recommendations <- function(report) {
  if (nrow(report$summary) == 0) return(character())
  recs <- character()
  sev <- table(report$summary$severity)

  det <- unique(report$summary$detector)

  if ("critical" %in% names(sev)) {
    recs <- c(recs, "URGENT: Resolve critical leakage issues before training models.")
  }

  if ("high" %in% names(sev)) {
    recs <- c(recs, "Address high severity leakage issues to ensure model validity.")
  }

  if ("train_test_contamination" %in% det) {
    recs <- c(recs, "Ensure proper data splits to avoid train-test contamination.")
  }

  if ("target_leakage" %in% det) {
    recs <- c(recs, "Review feature engineering pipeline to prevent target leakage.")
  }

  if ("duplication_detection" %in% det) {
    recs <- c(recs, "Remove duplicate records to minimize bias.")
  }

  recs <- c(recs,
            "Integrate automated leakage detection into your ML pipeline.",
            "Document preprocessing steps for transparency.",
            "Use appropriate cross-validation respecting data structures.")

  recs
}


#' @title Format recommendations for output.
#'
#' @return Formatted recommendation section string.
#' @keywords internal
#' @param report A leakr_report object.
#' @param format TODO: Add description
generate_recommendations_section <- function(report, format) {
  recs <- generate_recommendations(report)
  if (length(recs) == 0) {
    return(ifelse(format == "html",
                  '<div class="no-issues">No specific recommendations. Dataset appears clean.</div>',
                  "No specific recommendations. Dataset appears clean."))
  }
  if (format == "html") {
    items <- paste0("<li>", recs, "</li>", collapse = "\n")
    return(sprintf("<ol>\n%s\n</ol>", items))
  }
  if (format == "markdown") {
    items <- paste(seq_along(recs), recs, sep = ". ", collapse = "\n")
    return(items)
  }
  # Default to plain text
  paste(seq_along(recs), recs, sep = ". ", collapse = "\n")
}
