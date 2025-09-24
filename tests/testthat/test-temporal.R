library(testthat)

test_that("Constructor validates params", {
  expect_error(new_temporal_detector(time_col = 123))
  expect_error(new_temporal_detector(time_col = "time", lookahead_window = 0))
  det <- new_temporal_detector(time_col = "time", lookahead_window = 2)
  expect_s3_class(det, c("temporal_detector", "leakr_detector"))
})

test_that("Errors if time_col missing or wrong type", {
  det <- new_temporal_detector(time_col = "time", lookahead_window = 1)
  df <- data.frame(date = Sys.Date() + 1:3)
  expect_error(run_detector(det, df))
  df2 <- data.frame(time = c(1, 2, 3))
  split <- c("train", "train", "test")
  expect_error(run_detector(det, df2, split = split))
})

test_that("Detect leakage and severity boundaries", {
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)

  df <- data.frame(date = as.Date("2025-01-01") + 0:20)

  # low severity (4 leaks)
  split <- c(rep("train", 17), rep("test", 4))
  res <- run_detector(det, df, split = split)
  expect_equal(res$issues$severity, "low")

  # medium severity (5 leaks)
  split <- c(rep("train", 16), rep("test", 5))
  res <- run_detector(det, df, split = split)
  expect_equal(res$issues$severity, "medium")

  # high severity (>20 leaks simulated by repetition)
  df_big <- df[rep(seq_len(nrow(df)), 3), ]
  split_big <- c(rep("train", 50), rep("test", nrow(df_big) - 50))
  res_big <- run_detector(det, df_big, split = split_big)
  expect_equal(res_big$issues$severity, "high")
})

test_that("Handles edge cases: empty df, all-test split", {
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)
  empty_df <- data.frame(date = as.Date(character(0)))
  expect_error(run_detector(det, empty_df, split = character(0)), NA)

  df_all_test <- data.frame(date = as.Date("2025-01-01") + 0:2)
  split <- rep("test", 3)
  res <- run_detector(det, df_all_test, split = split)
  expect_equal(res$issues$severity, "low")
})

test_that("Config argument accepted and passed", {
  det <- new_temporal_detector(time_col = "date", lookahead_window = 1)
  df <- data.frame(date = Sys.Date() + 0:4)
  split <- c("train", "train", "test", "test", "test")
  res <- run_detector(det, df, split = split, config = list(debug=TRUE))
  expect_true("issues" %in% names(res))
})
