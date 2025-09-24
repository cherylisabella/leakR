#' @title Import data from various sources for leakage analysis
#'
#' @description Flexible data import function supporting multiple formats with automatic
#' format detection and preprocessing for leakage analysis.
#'
#' @param source Path to data file, data.frame, or other supported object
#' @param format Data format: "auto", "csv", "excel", "rds", "json", "parquet"
#' @param preprocessing List of preprocessing options
#' @param encoding Character encoding for text files
#' @param sheet Sheet name/number for Excel files
#' @param verbose Whether to show import messages
#' @param ... Additional arguments passed to format-specific readers
#'
#' @return Standardised data.frame suitable for leakage analysis
#' @export
#'
#' @examples
#' \dontrun{
#' # Auto-detect CSV format
#' data <- leakr_import("data.csv")
#'
#' # Import Excel with preprocessing
#' data <- leakr_import("data.xlsx", sheet = "raw_data",
#'                     preprocessing = list(remove_empty_cols = TRUE))
#'
#' # Import with custom options
#' @keywords internal
#' data <- leakr_import("data.csv", format = "csv", encoding = "UTF-8")
#' }
leakr_import <- function(source, format = "auto", preprocessing = list(),
                         encoding = "UTF-8", sheet = NULL, verbose = TRUE, ...) {

  # Handle data.frame input directly
  if (is.data.frame(source)) {
    return(preprocess_imported_data(source, preprocessing, verbose))
  }

  # Validate source file exists
  if (is.character(source) && length(source) == 1) {
    if (!file.exists(source)) {
      stop("File not found: ", source)
    }
  } else {
    stop("source must be a file path or data.frame")
  }

  # Auto-detect format if requested
  if (format == "auto") {
    format <- detect_file_format(source, verbose)
  }

  if (verbose) {
    message("Importing data using format: ", format)
  }

  # Import based on format
  data <- switch(format,
                 "csv" = import_csv(source, encoding, verbose, ...),
                 "excel" = import_excel(source, sheet, verbose, ...),
                 "xlsx" = import_excel(source, sheet, verbose, ...),
                 "xls" = import_excel(source, sheet, verbose, ...),
                 "rds" = import_rds(source, verbose, ...),
                 "json" = import_json(source, verbose, ...),
                 "parquet" = import_parquet(source, verbose, ...),
                 "tsv" = import_tsv(source, encoding, verbose, ...),
                 stop("Unsupported format: ", format)
  )

  # Apply preprocessing
  data <- preprocess_imported_data(data, preprocessing, verbose)

  # Validate imported data
  validate_imported_data(data, source)

  if (verbose) {
    message("Successfully imported data: ", nrow(data), " rows, ", ncol(data), " columns")
  }

  return(data)
}

#' Minimal quick import for typical user workflows
#'
#' Fast import with default preprocessing and minimal interaction.
#' Uses leakr_import internally.
#'
#' @param source File path or data.frame
#' @param ... Arguments passed to leakr_import for customisation
#' @return Standardised data.frame
#' @export
#' @keywords internal
leakr_quick_import <- function(source, ...) {
  leakr_import(source, preprocessing = list(), verbose = FALSE, ...)
}

#' Detect file format from extension and content
#' @keywords internal
detect_file_format <- function(file_path, verbose = TRUE) {

  # Get file extension
  ext <- tolower(tools::file_ext(file_path))

  # Map extensions to formats
  format_map <- c(
    "csv" = "csv",
    "tsv" = "tsv",
    "txt" = "csv",  # Assume CSV for .txt
    "xlsx" = "excel",
    "xls" = "excel",
    "rds" = "rds",
    "json" = "json",
    "parquet" = "parquet"
  )

  if (ext %in% names(format_map)) {
    return(format_map[ext])
  }

  # Fallback: try to detect from content
  tryCatch({
    # Read first few lines to detect format
    first_lines <- readLines(file_path, n = 3, warn = FALSE)

    if (length(first_lines) > 0) {
      # Check for JSON
      if (grepl("^\\s*[\\{\\[]", first_lines[1])) {
        if (verbose) message("Detected JSON format from content")
        return("json")
      }

      # Check for CSV (comma-separated)
      if (grepl(",", first_lines[1])) {
        if (verbose) message("Detected CSV format from content")
        return("csv")
      }

      # Check for TSV (tab-separated)
      if (grepl("\\t", first_lines[1])) {
        if (verbose) message("Detected TSV format from content")
        return("tsv")
      }
    }
  }, error = function(e) {
    if (verbose) warning("Content detection failed: ", e$message)
  })

  # Default fallback with warning
  if (verbose) {
    warning("Could not detect format for: ", file_path, ". Assuming CSV.")
  }
  return("csv")
}

#' Import CSV files with robust parsing and performance optimization
#' @keywords internal
import_csv <- function(file_path, encoding, verbose, ...) {

  # Check if file is large and use streaming if necessary
  file_size <- file.info(file_path)$size
  if (!is.na(file_size) && file_size > 1e8) {
    if (verbose) message("Large file detected: streaming first 10k rows for interactivity.")
    data <- read.csv(file_path, nrows = 10000, encoding = encoding,
                     stringsAsFactors = FALSE, ...)
    attr(data, "leakr_streaming") <- TRUE
    return(data)
  }

  # Try using data.table::fread for better performance
  if (requireNamespace("data.table", quietly = TRUE)) {
    if (verbose) message("Using data.table::fread for optimized performance")
    tryCatch({
      data <- data.table::fread(file_path, encoding = encoding,
                                na.strings = c("", "NA", "NULL", "#N/A", "N/A"), ...)
      return(as.data.frame(data))
    }, error = function(e) {
      if (verbose) warning("data.table::fread failed, falling back to base R: ", e$message)
    })
  }

  # Fallback to base R read.csv
  default_args <- list(
    file = file_path,
    header = TRUE,
    stringsAsFactors = FALSE,
    fileEncoding = encoding,
    na.strings = c("", "NA", "NULL", "#N/A", "N/A"),
    strip.white = TRUE
  )

  args <- modifyList(default_args, list(...))
  tryCatch({
    do.call(read.csv, args)
  }, error = function(e) {
    stop("Failed to import CSV file: ", e$message)
  })
}

#' Import TSV files with robust parsing and performance optimization
#' @keywords internal
import_tsv <- function(file_path, encoding, verbose, ...) {

  # Check if file is large and use streaming if necessary
  file_size <- file.info(file_path)$size
  if (!is.na(file_size) && file_size > 1e8) {
    if (verbose) message("Large TSV detected: reading first 10k rows only.")
    data <- read.delim(file_path, nrows = 10000, encoding = encoding,
                       stringsAsFactors = FALSE, ...)
    attr(data, "leakr_streaming") <- TRUE
    return(data)
  }

  # Try using data.table::fread for better performance
  if (requireNamespace("data.table", quietly = TRUE)) {
    tryCatch({
      data <- data.table::fread(file_path, sep = "\t", encoding = encoding,
                                na.strings = c("", "NA", "NULL", "#N/A", "N/A"), ...)
      return(as.data.frame(data))
    }, error = function(e) {
      if (verbose) warning("data.table::fread failed for TSV, falling back to base R: ", e$message)
    })
  }

  # Fallback to base R read.delim
  default_args <- list(
    file = file_path,
    header = TRUE,
    sep = "\t",
    stringsAsFactors = FALSE,
    fileEncoding = encoding,
    na.strings = c("", "NA", "NULL", "#N/A", "N/A"),
    strip.white = TRUE
  )

  args <- modifyList(default_args, list(...))
  tryCatch({
    do.call(read.delim, args)
  }, error = function(e) {
    stop("Failed to import TSV file: ", e$message)
  })
}

#' Import Excel files with enhanced sheet support
#' @keywords internal
import_excel <- function(file_path, sheet, verbose, ...) {

  # Check for required package
  if (!requireNamespace("readxl", quietly = TRUE)) {
    stop("The 'readxl' package is required for reading Excel files. Install it with: install.packages('readxl')")
  }

  tryCatch({
    # Get available sheets
    available_sheets <- readxl::excel_sheets(file_path)

    # Determine which sheet to load
    if (is.null(sheet)) {
      sheet <- available_sheets[1]
      if (length(available_sheets) > 1 && verbose) {
        message("Multiple sheets found. Using: ", sheet)
        message("Available sheets: ", paste(available_sheets, collapse = ", "))
      }
    } else {
      # Validate sheet selection
      if (is.character(sheet) && !sheet %in% available_sheets) {
        stop("Sheet '", sheet, "' not found. Available sheets: ", paste(available_sheets, collapse = ", "))
      }
      if (is.numeric(sheet) && (sheet < 1 || sheet > length(available_sheets))) {
        stop("Sheet number ", sheet, " out of range. Available: 1-", length(available_sheets))
      }
    }

    # Import the specified sheet
    data <- readxl::read_excel(file_path, sheet = sheet, ...)

    # Return data as a standard data.frame
    as.data.frame(data)

  }, error = function(e) {
    stop("Failed to import Excel file: ", e$message)
  })
}

#' Import RDS files with validation
#' @keywords internal
import_rds <- function(file_path, verbose, ...) {

  tryCatch({
    # Read the RDS file
    data <- readRDS(file_path)

    # Ensure data is convertible to a data.frame
    if (!is.data.frame(data)) {
      if (is.matrix(data)) {
        if (verbose) message("Converting matrix to data.frame")
        data <- as.data.frame(data)
      } else if (is.list(data) && all(lengths(data) == lengths(data)[1])) {
        if (verbose) message("Converting list to data.frame")
        data <- as.data.frame(data, stringsAsFactors = FALSE)
      } else {
        stop("RDS file does not contain tabular data that can be converted to a data.frame")
      }
    }

    return(data)

  }, error = function(e) {
    stop("Failed to import RDS file: ", e$message)
  })
}

#' Import JSON files with better structure handling
#' @keywords internal
import_json <- function(file_path, verbose, ...) {

  # Check if jsonlite is available
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("The 'jsonlite' package is required for reading JSON files. Install it with: install.packages('jsonlite')")
  }

  tryCatch({
    # Read and flatten the JSON file
    data <- jsonlite::fromJSON(file_path, flatten = TRUE, ...)

    # Ensure data is convertible to a data.frame
    if (!is.data.frame(data)) {
      if (is.list(data)) {
        lengths_vec <- lengths(data)

        # Check if all list elements have the same length
        if (length(unique(lengths_vec)) == 1) {
          if (verbose) message("Converting JSON list to data.frame")
          data <- as.data.frame(data, stringsAsFactors = FALSE)
        } else {
          stop("JSON contains an irregular structure that cannot be converted to tabular data")
        }
      } else {
        stop("JSON file does not contain tabular data")
      }
    }
    return(data)}, error = function(e) {
    stop("Failed to import JSON file: ", e$message)
  })
}

#' Import Parquet files
#' @keywords internal
import_parquet <- function(file_path, verbose, ...) {

  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("arrow package required for Parquet files. Install with: install.packages('arrow')")
  }

  tryCatch({
    data <- arrow::read_parquet(file_path, ...)
    as.data.frame(data)
  }, error = function(e) {
    stop("Failed to import Parquet file: ", e$message)
  })
}

#' Enhanced preprocessing with better performance and robustness
#' @keywords internal
preprocess_imported_data <- function(data, preprocessing, verbose) {

  # Default preprocessing options
  default_preprocessing <- list(
    remove_empty_rows = TRUE,
    remove_empty_cols = TRUE,
    clean_column_names = TRUE,
    convert_character_to_factor = FALSE,
    handle_dates = TRUE,
    remove_constant_cols = FALSE,
    max_factor_levels = 100)

  preprocessing <- modifyList(default_preprocessing, preprocessing)
  original_dims <- dim(data)
  provenance <- list()

  # Clean column names first
  if (preprocessing$clean_column_names) {
    old_names <- names(data)
    names(data) <- clean_column_names(names(data))
    if (verbose && !identical(old_names, names(data))) {
      message("Column names cleaned and standardised")
    }
    provenance$clean_column_names <- TRUE
  }

  # Remove empty rows (vectorised approach)
  if (preprocessing$remove_empty_rows) {
    # More efficient: check for rows that are all NA or empty strings
    empty_rows <- rowSums(is.na(data) | data == "", na.rm = FALSE) == ncol(data)
    if (sum(empty_rows) > 0) {
      data <- data[!empty_rows, , drop = FALSE]
      if (verbose) message("Removed ", sum(empty_rows), " empty rows")
      provenance$removed_empty_rows <- sum(empty_rows)
    }
  }

  # Remove empty columns (vectorised approach)
  if (preprocessing$remove_empty_cols) {
    empty_cols <- vapply(data, function(x) all(is.na(x) | x == ""), logical(1))
    if (sum(empty_cols) > 0) {
      data <- data[, !empty_cols, drop = FALSE]
      if (verbose) message("Removed ", sum(empty_cols), " empty columns")
      provenance$removed_empty_cols <- sum(empty_cols)
    }
  }

  # Remove constant columns (robust handling for different data types)
  if (preprocessing$remove_constant_cols) {
    constant_cols <- vapply(data, function(x) {
      tryCatch({
        # Handle different data types safely
        if (is.list(x) || is.matrix(x)) return(FALSE)
        unique_vals <- unique(x[!is.na(x)])
        length(unique_vals) <= 1
      }, error = function(e) FALSE)
    }, logical(1))

    if (sum(constant_cols) > 0) {
      data <- data[, !constant_cols, drop = FALSE]
      if (verbose) message("Removed ", sum(constant_cols), " constant columns")
      provenance$removed_constant_cols <- sum(constant_cols)
    }
  }

  # Enhanced date detection
  if (preprocessing$handle_dates) {
    data <- detect_and_convert_dates_enhanced(data, verbose)
    provenance$handle_dates <- TRUE
  }

  # Convert character to factor with level limits
  if (preprocessing$convert_character_to_factor) {
    char_cols <- vapply(data, is.character, logical(1))
    max_levels <- preprocessing$max_factor_levels

    for (col_name in names(data)[char_cols]) {
      unique_vals <- length(unique(data[[col_name]]))
      if (unique_vals <= max_levels) {
        data[[col_name]] <- as.factor(data[[col_name]])
        if (verbose) message("Converted '", col_name, "' to factor (", unique_vals, " levels)")
      } else if (verbose) {
        message("Skipped converting '", col_name, "' to factor (", unique_vals, " > ", max_levels, " levels)")
      }
    }
  }

  final_dims <- dim(data)
  if (verbose && !identical(original_dims, final_dims)) {
    message("Data shape changed from ", paste(original_dims, collapse = " x "),
            " to ", paste(final_dims, collapse = " x "))
  }
  attr(data, "leakr_provenance") <- provenance
  return(data)
}

#' Enhanced column name cleaning with better robustness
#' @keywords internal
clean_column_names <- function(names) {

  # Handle missing or NULL names
  if (is.null(names) || length(names) == 0) {
    return(character(0))
  }

  # Remove leading/trailing whitespace
  names <- trimws(names)

  # Handle empty names first
  empty_names <- is.na(names) | names == ""
  names[empty_names] <- paste0("Col_", seq_along(names))[empty_names]

  # Replace spaces and special characters with underscores
  names <- gsub("[^A-Za-z0-9_\\.]", "_", names)

  # Remove multiple consecutive underscores
  names <- gsub("_{2,}", "_", names)

  # Remove leading/trailing underscores and dots
  names <- gsub("^[_\\.]+|[_\\.]+$", "", names)

  # Ensure names start with letter, dot, or underscore (R naming rules)
  invalid_start <- grepl("^[0-9]", names)
  names[invalid_start] <- paste0("X", names[invalid_start])

  # Handle names that became empty after cleaning
  still_empty <- names == ""
  names[still_empty] <- paste0("Col_", seq_along(names))[still_empty]

  # Ensure uniqueness using make.names
  names <- make.names(names, unique = TRUE)

  return(names)
}

#' Enhanced date detection handling multiple formats and data types
#' @keywords internal
detect_and_convert_dates_enhanced <- function(data, verbose) {

  converted_count <- 0

  for (col_name in names(data)) {
    col_data <- data[[col_name]]

    # Skip if already Date or POSIXt
    if (inherits(col_data, c("Date", "POSIXt"))) {
      next
    }

    # Handle different input types
    original_type <- class(col_data)[1]

    # Convert factors to character for processing
    if (is.factor(col_data)) {
      col_data <- as.character(col_data)
    }

    # Handle numeric timestamps (Unix time)
    if (is.numeric(col_data)) {
      # Check if looks like Unix timestamp (reasonable date range)
      non_na_vals <- col_data[!is.na(col_data)]
      if (length(non_na_vals) > 0) {
        # Unix timestamps are typically between 1970 and 2050
        min_unix <- as.numeric(as.Date("1970-01-01"))
        max_unix <- as.numeric(as.Date("2050-01-01"))

        # Check both seconds and milliseconds timestamps
        if (all(non_na_vals >= min_unix & non_na_vals <= max_unix) ||
            all(non_na_vals >= min_unix * 1000 & non_na_vals <= max_unix * 1000)) {

          tryCatch({
            # Try as seconds first, then milliseconds
            if (all(non_na_vals < max_unix)) {
              converted_dates <- as.Date(as.POSIXct(col_data, origin = "1970-01-01"))
            } else {
              converted_dates <- as.Date(as.POSIXct(col_data / 1000, origin = "1970-01-01"))
            }

            data[[col_name]] <- converted_dates
            converted_count <- converted_count + 1
            if (verbose) message("Converted '", col_name, "' from ", original_type, " to Date (Unix timestamp)")
            next
          }, error = function(e) {})
            # Continue to text-based detection
        }
      }
    }

    # Skip if not character after factor conversion
    if (!is.character(col_data)) {
      next
    }

    # Check if looks like dates (sample approach for performance)
    sample_size <- min(20, length(col_data[!is.na(col_data)]))
    sample_values <- col_data[!is.na(col_data)][1:sample_size]

    if (length(sample_values) == 0) {
      next
    }

    # Try common date patterns
    date_patterns <- list(
      list(pattern = "%Y-%m-%d", name = "ISO date (YYYY-MM-DD)"),
      list(pattern = "%d/%m/%Y", name = "UK format (DD/MM/YYYY)"),
      list(pattern = "%m/%d/%Y", name = "US format (MM/DD/YYYY)"),
      list(pattern = "%Y-%m-%d %H:%M:%S", name = "ISO datetime"),
      list(pattern = "%d-%m-%Y", name = "European (DD-MM-YYYY)"),
      list(pattern = "%Y%m%d", name = "Compact (YYYYMMDD)"),
      list(pattern = "%d %B %Y", name = "Long format (DD Month YYYY)"),
      list(pattern = "%B %d, %Y", name = "US long format (Month DD, YYYY)")
    )

    for (pattern_info in date_patterns) {
      pattern <- pattern_info$pattern
      pattern_name <- pattern_info$name

      parsed_dates <- suppressWarnings(as.Date(sample_values, format = pattern))

      # If at least 70% parse successfully, convert the column
      success_rate <- sum(!is.na(parsed_dates)) / length(sample_values)

      if (success_rate >= 0.7) {
        tryCatch({
          data[[col_name]] <- as.Date(col_data, format = pattern)
          converted_count <- converted_count + 1
          if (verbose) {
            message("Converted '", col_name, "' to Date using ", pattern_name)
          }
          break
        }, error = function(e) {})
          # Continue to next pattern
      }
    }
  }

  if (verbose && converted_count > 0) {
    message("Successfully converted ", converted_count, " column(s) to Date format")
  }

  return(data)
}

#' Enhanced data validation with better error messages
#' @keywords internal
validate_imported_data <- function(data, source) {

  # Check if data is empty
  if (nrow(data) == 0) {
    stop("Imported data has no rows: ", source)
  }

  if (ncol(data) == 0) {
    stop("Imported data has no columns: ", source)
  }

  # Check for reasonable dimensions
  if (nrow(data) > 1000000) {
    warning("Large dataset imported (", format(nrow(data), big.mark = ","),
            " rows). Consider sampling for analysis to improve performance.")
  }

  # Check column name issues
  if (any(duplicated(names(data)))) {
    warning("Duplicate column names detected and made unique.")
    names(data) <- make.names(names(data), unique = TRUE)
  }

  # Check for high missing data rates
  missing_rates <- vapply(data, function(x) sum(is.na(x)) / length(x), numeric(1))
  high_missing <- missing_rates > 0.9

  if (any(high_missing)) {
    warning("Columns with >90% missing data: ",
            paste(names(data)[high_missing], collapse = ", "))
  }

  # Check for problematic data types
  list_cols <- vapply(data, is.list, logical(1))
  if (any(list_cols)) {
    warning("List columns detected (may cause issues): ",
            paste(names(data)[list_cols], collapse = ", "))
  }

  return(invisible(TRUE))
}

#' Convert mlr3 Task objects to standard format
#'
#' Extract data from mlr3 Task objects for leakage analysis.
#'
#' @param task mlr3 Task object (TaskClassif, TaskRegr, etc.)
#' @param include_target Whether to include target variable in output
#'
#' @return List with data, target, and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' library(mlr3)
#' task <- tsk("iris")
#' leakr_data <- leakr_from_mlr3(task, include_target = TRUE)
#' report <- leakr_audit(leakr_data$data, target = leakr_data$target)
#' }
#' @keywords internal
leakr_from_mlr3 <- function(task, include_target = TRUE) {

  if (!requireNamespace("mlr3", quietly = TRUE)) {
    stop("mlr3 package required. Install with: install.packages('mlr3')")
  }

  if (!inherits(task, "Task")) {
    stop("Input must be an mlr3 Task object")
  }

  # Extract data safely
  tryCatch({
    data <- task$data()
    target_col <- task$target_names

    # Prepare output
    result <- list(
      data = if (include_target) data else data[, !names(data) %in% target_col, drop = FALSE],
      target = target_col,
      task_type = class(task)[1],
      feature_names = task$feature_names,
      n_rows = task$nrow,
      n_features = length(task$feature_names)
    )

    class(result) <- "leakr_mlr3_data"
    return(result)
  }, error = function(e) {
    stop("Failed to extract data from mlr3 task: ", e$message)
  })
}

#' Convert caret training objects to standard format
#'
#' Extract data from caret train objects for leakage analysis.
#'
#' @param train_obj caret train object
#' @param original_data Original training data (if available)
#' @param target_name Custom name for target variable (default: "target")
#'
#' @return List with data and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' library(caret)
#' model <- train(Species ~ ., data = iris, method = "rf")
#' leakr_data <- leakr_from_caret(model, original_data = iris, target_name = "Species")
#' }
#' @keywords internal
leakr_from_caret <- function(train_obj, original_data = NULL, target_name = "target") {

  if (!requireNamespace("caret", quietly = TRUE)) {
    stop("caret package required. Install with: install.packages('caret')")
  }

  if (!inherits(train_obj, "train")) {
    stop("Input must be a caret train object")
  }

  # Extract available information
  result <- list(
    model_info = train_obj$modelInfo,
    method = train_obj$method,
    n_features = if (!is.null(train_obj$trainingData)) ncol(train_obj$trainingData) - 1 else NA,
    performance = train_obj$results
  )

  # Include original data if provided
  if (!is.null(original_data)) {
    if (!is.data.frame(original_data)) {
      stop("original_data must be a data.frame")
    }
    result$data <- original_data
  } else if (!is.null(train_obj$trainingData)) {
    # Reconstruct from training data
    training_data <- train_obj$trainingData
    # Rename .outcome to specified target name
    if (".outcome" %in% names(training_data)) {
      names(training_data)[names(training_data) == ".outcome"] <- target_name
    }
    result$data <- training_data
  }

  class(result) <- "leakr_caret_data"
  return(result)
}

#' Convert tidymodels workflow to standard format
#'
#' Extract data from tidymodels workflows for leakage analysis.
#'
#' @param workflow tidymodels workflow object
#' @param data Original training data
#'
#' @return List with data and metadata
#' @export
#'
#' @examples
#' \dontrun{
#' library(tidymodels)
#' recipe_spec <- recipe(Species ~ ., data = iris)
#' model_spec <- rand_forest() %>% set_engine("ranger") %>% set_mode("classification")
#' workflow <- workflow() %>% add_recipe(recipe_spec) %>% add_model(model_spec)
#' leakr_data <- leakr_from_tidymodels(workflow, iris)
#' }
#' @keywords internal
leakr_from_tidymodels <- function(workflow, data) {

  if (!requireNamespace("workflows", quietly = TRUE)) {
    stop("workflows package (tidymodels) required. Install with: install.packages('tidymodels')")
  }

  if (!inherits(workflow, "workflow")) {
    stop("Input must be a tidymodels workflow object")
  }

  if (!is.data.frame(data)) {
    stop("data must be a data.frame")
  }

  # Safely extract workflow components
  result <- list(
    data = data,
    has_preprocessor = FALSE,
    has_spec = FALSE,
    preprocessor_type = NULL,
    model_spec = NULL
  )

  # Check components safely
  tryCatch({
    result$has_preprocessor <- workflows::has_preprocessor(workflow)
    if (result$has_preprocessor) {
      preprocessor <- workflows::pull_workflow_preprocessor(workflow)
      if (!is.null(preprocessor)) {
        result$preprocessor_type <- class(preprocessor)[1]
      }
    }
  }, error = function(e) {
    warning("Could not extract preprocessor information: ", e$message)
  })

  tryCatch({
    result$has_spec <- workflows::has_spec(workflow)
    if (result$has_spec) {
      spec <- workflows::pull_workflow_spec(workflow)
      if (!is.null(spec)) {
        result$model_spec <- list(
          engine = spec$engine %||% "unknown",
          mode = spec$mode %||% "unknown"
        )
      }
    }
  }, error = function(e) {
    warning("Could not extract model specification: ", e$message)
  })

  class(result) <- "leakr_tidymodels_data"
  return(result)
}

#' Export data with consistent messaging
#' @keywords internal
export_data_internal <- function(data, file_path, format, verbose, ...) {

  # Create directory if needed
  dir_path <- dirname(file_path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }

  # Export based on format
  switch(format,
         "csv" = {
           write.csv(data, file_path, row.names = FALSE, ...)
         },
         "excel" = {
           if (!requireNamespace("openxlsx", quietly = TRUE)) {
             stop("openxlsx package required for Excel export. Install with: install.packages('openxlsx')")
           }
           openxlsx::write.xlsx(data, file_path, ...)
         },
         "rds" = {
           saveRDS(data, file_path, ...)
         },
         "json" = {
           if (!requireNamespace("jsonlite", quietly = TRUE)) {
             stop("jsonlite package required for JSON export. Install with: install.packages('jsonlite')")
           }
           jsonlite::write_json(data, file_path, ...)
         },
         "parquet" = {
           if (!requireNamespace("arrow", quietly = TRUE)) {
             stop("arrow package required for Parquet export. Install with: install.packages('arrow')")
           }
           arrow::write_parquet(data, file_path, ...)
         },
         stop("Unsupported export format: ", format)
  )

  if (verbose) {
    message("Data exported to: ", file_path)
  }

  return(file_path)
}

#' Export data in various formats
#'
#' Save processed data to different file formats with consistent behaviour.
#'
#' @param data Data.frame to export
#' @param file_path Output file path
#' @param format Output format: "csv", "excel", "rds", "json", "parquet"
#' @param verbose Whether to show export messages
#' @param ... Additional arguments for format-specific writers
#'
#' @return Path to exported file (invisibly)
#' @export
#' @keywords internal
leakr_export_data <- function(data, file_path, format = "csv", verbose = TRUE, ...) {

  if (!is.data.frame(data)) {
    stop("data must be a data.frame")
  }

  result <- export_data_internal(data, file_path, format, verbose, ...)
  return(invisible(result))
}

#' Create data snapshots with improved metadata handling
#'
#' Save data and metadata for reproducible leakage analysis with optimised performance.
#'
#' @param data Data.frame to snapshot
#' @param output_dir Directory for snapshot files
#' @param snapshot_name Name for this snapshot
#' @param metadata Additional metadata to store
#' @param sample_for_hash Whether to sample large datasets for faster hashing
#'
#' @return Path to snapshot directory
#' @export
#' @keywords internal
leakr_create_snapshot <- function(data, output_dir = "leakr_snapshots",
                                  snapshot_name = NULL, metadata = list(),
                                  sample_for_hash = TRUE) {

  # Ensure 'data' is a data.frame
  if (!is.data.frame(data)) stop("data must be a data.frame")

  # Generate snapshot name if not provided
  if (is.null(snapshot_name)) {
    snapshot_name <- paste0("snapshot_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }

  # Create snapshot directory
  snapshot_dir <- file.path(output_dir, snapshot_name)
  if (!dir.exists(snapshot_dir)) dir.create(snapshot_dir, recursive = TRUE)

  # Export data in CSV and RDS formats
  export_data_internal(data, file.path(snapshot_dir, "data.csv"), "csv", verbose = FALSE)
  export_data_internal(data, file.path(snapshot_dir, "data.rds"), "rds", verbose = FALSE)

  # Get provenance attribute from data
  provenance <- attr(data, "leakr_provenance")

  # Generate hash for large datasets (using sampling for performance)
  data_hash <- if (nrow(data) > 10000 && sample_for_hash) {
    sample_indices <- sample(nrow(data), min(1000, nrow(data)))
    sampled_data <- data[sample_indices, , drop = FALSE]
    paste0("sample_", digest::digest(sampled_data))
  } else {
    digest::digest(data)
  }

  # Create metadata with necessary information
  full_metadata <- list(
    created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC"),
    leakr_version = as.character(utils::packageVersion("leakR")),
    r_version = R.version.string,
    data_shape = dim(data),
    column_names = names(data),
    column_types = vapply(data, function(x) class(x)[1], character(1)),
    provenance = provenance,
    data_hash = data_hash,
    hash_method = if (nrow(data) > 10000 && sample_for_hash) "sample_based" else "full_data",
    user_metadata = metadata
  )

  # Save metadata to JSON
  metadata_json <- jsonlite::toJSON(full_metadata, auto_unbox = TRUE, pretty = TRUE)
  writeLines(metadata_json, file.path(snapshot_dir, "metadata.json"))

  # Create and write the README content
  readme_content <- paste0(
    "# Data Snapshot: ", snapshot_name, "\n\n",
    "**Created:** ", full_metadata$created_at, "\n",
    "**leakR Version:** ", full_metadata$leakr_version, "\n",
    "**R Version:** ", full_metadata$r_version, "\n\n",
    "## Files:\n",
    "- `data.csv`: Data in CSV format\n",
    "- `data.rds`: Data in R binary format (recommended)\n",
    "- `metadata.json`: Snapshot metadata and provenance\n",
    "- `README.md`: This documentation file\n\n",
    "## Data Summary:\n",
    "- **Dimensions:** ", paste(full_metadata$data_shape, collapse = " × "), " (rows × columns)\n",
    "- **Hash:** ", full_metadata$data_hash, "\n",
    "- **Hash Method:** ", full_metadata$hash_method, "\n\n",
    "## Column Information:\n",
    "| Column | Type |\n",
    "|--------|------|\n"
  )

  # Add column details to README
  readme_content <- paste0(readme_content,
                           paste0("| ", full_metadata$column_names, " | ", full_metadata$column_types, " |\n", collapse = "")
  )

  # Add usage instructions
  readme_content <- paste0(readme_content,
                           "\n## Usage:\n",
                           "```r\n",
                           "# Load snapshot\n",
                           "data <- leakr_load_snapshot('", snapshot_dir, "')\n",
                           "\n# Or load manually\n",
                           "data <- readRDS('", file.path(snapshot_dir, "data.rds"), "')\n",
                           "```"
  )

  # Write README to file
  writeLines(readme_content, file.path(snapshot_dir, "README.md"))

  # Display messages
  message("Snapshot created: ", snapshot_dir)
  message("Data shape: ", paste(full_metadata$data_shape, collapse = " × "))
  message("Hash: ", full_metadata$data_hash)

  # Return snapshot directory
  invisible(snapshot_dir)
}

#' Load data snapshot with enhanced validation
#'
#' Restore data from a previously created snapshot with integrity checking.
#'
#' @param snapshot_path Path to snapshot directory
#' @param format Format to load: "rds" (recommended), "csv"
#' @param verify_integrity Whether to verify data integrity using hash
#'
#' @return Data.frame from snapshot
#' @export
#' @keywords internal
leakr_load_snapshot <- function(snapshot_path, format = "rds", verify_integrity = TRUE) {

  # Check if the snapshot directory exists
  if (!dir.exists(snapshot_path)) {
    stop("Snapshot directory not found: ", snapshot_path)
  }

  # Determine the appropriate data file based on the format
  data_file <- switch(format,
                      "rds" = file.path(snapshot_path, "data.rds"),
                      "csv" = file.path(snapshot_path, "data.csv"),
                      stop("Unsupported format: ", format, ". Use 'rds' or 'csv'")
  )

  # Check if the data file exists
  if (!file.exists(data_file)) {
    stop("Data file not found: ", data_file)
  }

  # Load the data from the file
  data <- switch(format,
                 "rds" = readRDS(data_file),
                 "csv" = read.csv(data_file, stringsAsFactors = FALSE)
  )

  # Check and load metadata if available
  metadata_file <- file.path(snapshot_path, "metadata.json")
  if (file.exists(metadata_file)) {
    tryCatch({
      metadata <- jsonlite::fromJSON(metadata_file)

      # Log metadata information
      message("Loaded snapshot from: ", metadata$created_at)
      message("leakR version: ", metadata$leakr_version)
      message("Data shape: ", paste(metadata$data_shape, collapse = " × "))

      # Verify data integrity if requested
      if (verify_integrity && !is.null(metadata$data_hash)) {
        current_hash <- if (metadata$hash_method == "sample_based" && nrow(data) > 10000) {
          # Use the same sampling approach as during snapshot creation
          sample_indices <- sample(nrow(data), min(1000, nrow(data)))
          sampled_data <- data[sample_indices, , drop = FALSE]
          paste0("sample_", digest::digest(sampled_data))
        } else {
          digest::digest(data)
        }

        # Check if the hash matches
        if (current_hash != metadata$data_hash) {
          warning(sprintf("Data hash mismatch. Data may be corrupted or modified.\nExpected: %s\nActual: %s",
                          metadata$data_hash, current_hash))
        } else {
          message("✓ Data integrity verified")
        }
      }

      return(data)
    }, error = function(e) {
      warning("Could not load metadata: ", e$message)
      return(data)
    })
  } else {
    warning("No metadata found. Unable to verify data integrity.")
    return(data)
  }
}


#' List available snapshots with enhanced information
#'
#' Display comprehensive information about available data snapshots.
#'
#' @param snapshots_dir Directory containing snapshots
#' @param include_metadata Whether to load detailed metadata for each snapshot
#'
#' @return Data.frame with snapshot information
#' @export
#' @keywords internal
leakr_list_snapshots <- function(snapshots_dir = "leakr_snapshots", include_metadata = TRUE) {

  # Check if the snapshots directory exists
  if (!dir.exists(snapshots_dir)) {
    message("No snapshots directory found: ", snapshots_dir)
    return(empty_snapshot_info())
  }

  # List directories inside the snapshots directory
  snapshot_dirs <- list.dirs(snapshots_dir, full.names = TRUE, recursive = FALSE)

  # If no snapshots found, return empty info
  if (length(snapshot_dirs) == 0) {
    message("No snapshots found in: ", snapshots_dir)
    return(empty_snapshot_info())
  }

  # Initialize empty data frame for snapshot info
  snapshot_info <- data.frame()

  for (snapshot_dir in snapshot_dirs) {
    snapshot_name <- basename(snapshot_dir)
    metadata_file <- file.path(snapshot_dir, "metadata.json")
    rds_file <- file.path(snapshot_dir, "data.rds")

    # Calculate size of the snapshot directory
    dir_size_mb <- if (file.exists(rds_file)) {
      round(file.info(rds_file)$size / (1024^2), 2)
    } else {
      NA
    }

    # If metadata exists and we want it, load and process it
    if (file.exists(metadata_file) && include_metadata) {
      tryCatch({
        metadata <- jsonlite::fromJSON(metadata_file)
        info <- data.frame(
          name = snapshot_name,
          created = as.character(metadata$created_at),
          rows = metadata$data_shape[1],
          cols = metadata$data_shape[2],
          leakr_version = as.character(metadata$leakr_version),
          size_mb = dir_size_mb,
          path = snapshot_dir,
          stringsAsFactors = FALSE
        )
        snapshot_info <- rbind(snapshot_info, info)
      }, error = function(e) {
        warning("Could not read metadata for snapshot: ", snapshot_name)
        info <- data.frame(
          name = snapshot_name,
          created = "Metadata Error",
          rows = NA,
          cols = NA,
          leakr_version = "Unknown",
          size_mb = dir_size_mb,
          path = snapshot_dir,
          stringsAsFactors = FALSE
        )
        snapshot_info <- rbind(snapshot_info, info)
      })
    } else {
      # If metadata doesn't exist, use file modification time as creation date
      info <- data.frame(
        name = snapshot_name,
        created = if (file.exists(rds_file)) {
          format(file.info(rds_file)$mtime, "%Y-%m-%d %H:%M:%S")
        } else {
          "Unknown"
        },
        rows = NA,
        cols = NA,
        leakr_version = "Unknown",
        size_mb = dir_size_mb,
        path = snapshot_dir,
        stringsAsFactors = FALSE
      )
      snapshot_info <- rbind(snapshot_info, info)
    }
  }

  # Sort snapshots by creation time (newest first) and reset row names
  if (nrow(snapshot_info) > 0) {
    snapshot_info <- snapshot_info[order(snapshot_info$created, decreasing = TRUE), ]
    rownames(snapshot_info) <- NULL
    message("Found ", nrow(snapshot_info), " snapshot(s) in: ", snapshots_dir)
  }

  return(snapshot_info)
}

# Helper function to return an empty snapshot info dataframe
empty_snapshot_info <- function() {
  return(data.frame(
    name = character(0),
    created = character(0),
    rows = numeric(0),
    cols = numeric(0),
    leakr_version = character(0),
    size_mb = numeric(0),
    path = character(0),
    stringsAsFactors = FALSE
  ))
}

# Null-coalescing operator for internal use
# @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Null-coalescing operator for clean default value handling
#'
#' @param x First value to check
#' @param y Default value if x is NULL
#' @return x if not NULL, otherwise y
#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
