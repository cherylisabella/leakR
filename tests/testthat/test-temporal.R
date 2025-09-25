
library(testthat)

test_that("Constructor validates params", {
  expect_error(new_temporal_detector(time_col = 123))
  expect_error(new_temporal_detector(time_col = "time", lookahead_window = 0))
  det <- new_temporal_detector(time_col = "time", lookahead_window = 2)
  expect_s3_class(det, c("temporal_detector", "detector"))
})

test_that("Errors if time_col missing or wrong type", {
  det <- new_temporal_detector(time_col = "time", lookahead_window = 1)

  # Create test data with proper structure
  df <- data.frame(date = Sys.Date() + 1:10, value = 1:10)
  split <- c(rep("train", 5), rep("test", 5))

  # Test with NULL time_col - this should fail in constructor
  expect_error(new_temporal_detector(time_col = NULL), "time_col must be")
  
  # Test with wrong type - this should fail in constructor  
  expect_error(new_temporal_detector(time_col = 123), "time_col must be")
  
  # Test with non-existent column - this should fail in run_detector
  det_missing <- new_temporal_detector(time_col = "nonexistent")
  expect_error(run_detector(det_missing, df, split = split), "time_col not found")

  # Test with wrong type column
  df2 <- data.frame(time = as.character(1:10), value = 1:10)
  det2 <- new_temporal_detector(time_col = "time", lookahead_window = 1)
  expect_error(run_detector(det2, df2, split = split))
})

test_that("Detect leakage and severity boundaries", {
  # Create test data
  df <- data.frame(
    date = seq.Date(from = as.Date("2020-01-01"), length.out = 21, by = "day"),
    value = 1:21
  )
  
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)

  # Low severity test (4 leaks or fewer)
  split <- c(rep("train", 17), rep("test", 4))
  res <- run_detector(det, df, split = split)
  expect_equal(res$issues$severity, "low")

  # Medium severity test (5-19 leaks) 
  split <- c(rep("train", 16), rep("test", 5))
  res <- run_detector(det, df, split = split)
  expect_equal(res$issues$severity, "medium")

  # High severity test (20+ leaks)
  df_big <- data.frame(
    date = seq.Date(from = as.Date("2020-01-01"), length.out = 100, by = "day"),
    value = 1:100
  )
  n_train <- 50
  n_test <- 50
  split_big <- c(rep("train", n_train), rep("test", n_test))
  res_big <- run_detector(det, df_big, split = split_big)
  expect_equal(res_big$issues$severity, "high")
})

test_that("Handles edge cases: empty df, all-test split", {
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)

  # Empty dataframe
  empty_df <- data.frame(date = as.Date(character(0)))
  result <- run_detector(det, empty_df, split = character(0))
  expect_equal(result$issues$severity, "low")

  # All-test split
  df_all_test <- data.frame(date = as.Date("2025-01-01") + 0:2)
  split <- rep("test", 3)
  res <- run_detector(det, df_all_test, split = split)
  expect_equal(res$issues$severity, "low")
})

test_that("Config argument accepted and passed", {
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)
  df <- data.frame(date = Sys.Date() + 0:4)
  split <- c("train", "train", "test", "test", "test")
  res <- run_detector(det, df, split = split, config = list(debug = TRUE))
  expect_true("issues" %in% names(res))
})

