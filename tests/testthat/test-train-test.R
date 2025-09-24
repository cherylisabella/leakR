library(testthat)

test_that("Constructor validates & builds correct object", {
  expect_s3_class(new_train_test_detector(0.5), c("train_test_detector", "leakr_detector"))
  expect_error(new_train_test_detector(-0.2))
  expect_error(new_train_test_detector("string"))
})

test_that("Detects no overlap with unique IDs", {
  det <- new_train_test_detector()
  df <- data.frame(id = 1:6, val = rnorm(6))
  split <- c("train","train","train","test","test","test")
  res <- run_detector(det, df, split = split, id = "id")
  expect_equal(res$issues$severity, "low")
  expect_match(res$issues$description, "No overlap")
  expect_length(res$evidence$overlap_ids, 0)
})

test_that("Detects exact overlap and evidence IDs returned", {
  det <- new_train_test_detector()
  df <- data.frame(id = c(1,2,3,4,2,3), val = 1:6)
  split <- c("train","train","train","train","test","test")
  res <- run_detector(det, df, split = split, id = "id")
  expect_equal(res$issues$severity, "critical")
  expect_match(res$issues$description, "Detected 2 overlapping")
  expect_setequal(res$evidence$overlap_ids, c(2,3))
})

test_that("Detects overlap by row-hash fallback", {
  det <- new_train_test_detector()
  df <- data.frame(val = c(1,1,2,3,1,3))
  split <- c("train","train","train","test","test","test")
  res <- run_detector(det, df, split = split)
  expect_true(res$issues$severity %in% c("low","critical"))
  expect_true("overlap_hashes" %in% names(res$evidence))
})

test_that("Errors without split vector", {
  det <- new_train_test_detector()
  df <- data.frame(id = 1:5)
  expect_error(run_detector(det, df, id = "id"))
})

test_that("Handles edge cases: empty df, all train", {
  det <- new_train_test_detector()
  empty_df <- data.frame(id=integer(0), val=numeric(0))
  expect_error(run_detector(det, empty_df, split = character(0)), NA)

  all_train <- data.frame(id = 1:3)
  split <- rep("train", 3)
  res <- run_detector(det, all_train, split = split, id = "id")
  expect_equal(res$issues$severity, "low")
})

test_that("Config param is accepted and passed", {
  det <- new_train_test_detector()
  df <- data.frame(id = 1:4)
  split <- c("train","train","test","test")
  res <- run_detector(det, df, split = split, id = "id", config = list(debug=TRUE))
  expect_true("issues" %in% names(res))
})
