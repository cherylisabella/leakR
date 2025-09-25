
# Test helper functions

# Mock functions for testing when package functions are not available
mock_temporal_detector <- function(time_col, lookahead_window = 1) {
  if (exists("new_temporal_detector")) {
    return(new_temporal_detector(time_col, lookahead_window))
  } else {
    # Return mock object
    structure(list(
      time_col = time_col,
      lookahead_window = lookahead_window
    ), class = "temporal_detector")
  }
}

mock_train_test_detector <- function(threshold = 0.1) {
  if (exists("new_train_test_detector")) {
    return(new_train_test_detector(threshold))
  } else {
    # Return mock object
    structure(list(
      threshold = threshold
    ), class = "train_test_detector")
  }
}

