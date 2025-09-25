#' @title Detect data duplication
#'
#' @description Identifies exact and near-duplicate rows that may indicate data leakage
#' or preprocessing errors in machine learning workflows.
#'
#' @param audit_data Prepared audit data structure
#' @param config Configuration list with similarity thresholds and clustering parameters
#'
#' @return detector_result object with duplication findings
#' @keywords internal
detect_duplication <- function(audit_data, config) {

  issues <- data.frame()
  evidence <- list()

  data <- audit_data$data
  n_rows <- nrow(data)

  if (n_rows < 2) {
    return(create_detector_result(issues, list(note = "Insufficient rows for duplication analysis")))
  }

  # Store basic duplication statistics
  evidence$data_info <- list(
    n_rows = n_rows,
    n_cols = ncol(data),
    feature_types = sapply(data, class)
  )

  # 1. Exact duplicate detection using row hashes
  exact_result <- detect_exact_duplicates(data, config)
  if (nrow(exact_result$issues) > 0) {
    issues <- rbind(issues, exact_result$issues)
  }
  evidence$exact_duplicates <- exact_result$evidence

  # 2. Near-duplicate detection using similarity measures
  if (n_rows <= 10000) {  # Only run on manageable datasets
    near_result <- detect_near_duplicates(data, audit_data$numeric_features,
                                          audit_data$categorical_features, config)
    if (nrow(near_result$issues) > 0) {
      issues <- rbind(issues, near_result$issues)
    }
    evidence$near_duplicates <- near_result$evidence
  } else {
    evidence$near_duplicates <- list(note = "Dataset too large for near-duplicate analysis")
  }

  # 3. ID-based duplicate detection (if ID column provided)
  if (!is.null(audit_data$id)) {
    id_result <- detect_id_duplicates(data, audit_data$id, config)
    if (nrow(id_result$issues) > 0) {
      issues <- rbind(issues, id_result$issues)
    }
    evidence$id_duplicates <- id_result$evidence
  }

  # 4. Subset duplication (rows that are subsets of other rows)
  if (ncol(data) > 2 && n_rows <= 5000) {
    subset_result <- detect_subset_duplicates(data, config)
    if (nrow(subset_result$issues) > 0) {
      issues <- rbind(issues, subset_result$issues)
    }
    evidence$subset_duplicates <- subset_result$evidence
  }

  # 5. Clustering-based duplicate detection
  if (length(audit_data$numeric_features) >= 2 && n_rows <= 5000) {
    cluster_result <- detect_cluster_based_duplicates(data, audit_data$numeric_features, config)
    if (nrow(cluster_result$issues) > 0) {
      issues <- rbind(issues, cluster_result$issues)
    }
    evidence$cluster_duplicates <- cluster_result$evidence
  }

  return(create_detector_result(issues, evidence))
}

#' @title Detect exact duplicate rows using cryptographic hashing
#'
#' @param data Dataset to analyse
#' @param config Configuration parameters
#' @return List with exact duplicate results
#' @keywords internal
detect_exact_duplicates <- function(data, config) {

  issues <- data.frame()

  # Generate row hashes for exact matching
  row_hashes <- generate_row_hashes(data)

  # Find duplicate hashes
  hash_counts <- table(row_hashes)
  duplicate_hashes <- names(hash_counts)[hash_counts > 1]

  if (length(duplicate_hashes) == 0) {
    evidence <- list(
      n_unique_rows = length(unique(row_hashes)),
      n_duplicate_groups = 0,
      duplication_rate = 0
    )
    return(list(issues = issues, evidence = evidence))
  }

  # Analyse duplicate groups
  duplicate_groups <- list()
  total_duplicate_rows <- 0

  for (hash in duplicate_hashes) {
    duplicate_indices <- which(row_hashes == hash)
    duplicate_groups[[hash]] <- list(
      indices = duplicate_indices,
      count = length(duplicate_indices),
      sample_row = data[duplicate_indices[1], , drop = FALSE]
    )
    total_duplicate_rows <- total_duplicate_rows + length(duplicate_indices) - 1  # Subtract 1 to count extras
  }

  # Calculate severity based on duplication rate
  duplication_rate <- total_duplicate_rows / nrow(data)
  severity <- determine_duplication_severity(duplication_rate, length(duplicate_hashes))

  if (length(duplicate_hashes) > 0) {
    issue <- data.frame(
      issue_type = "exact_duplicates",
      severity = severity,
      description = sprintf("%d exact duplicate groups found (%d total duplicate rows, %.1f%% of data)",
                            length(duplicate_hashes), total_duplicate_rows, duplication_rate * 100),
      suggested_fix = "Remove duplicate rows or investigate data collection process"
    )

    issues <- rbind(issues, issue)
  }

  evidence <- list(
    duplicate_groups = duplicate_groups,
    n_duplicate_groups = length(duplicate_hashes),
    n_duplicate_rows = total_duplicate_rows,
    duplication_rate = duplication_rate,
    largest_group_size = max(sapply(duplicate_groups, function(x) x$count)),
    hash_method = "md5"
  )

  return(list(issues = issues, evidence = evidence))
}

#' @title Determine duplication severity
#'
#' @keywords internal
determine_duplication_severity <- function(duplication_rate, n_groups) {
  if (duplication_rate > 0.1 || n_groups > 50) {
    "high"
  } else if (duplication_rate > 0.05 || n_groups > 20) {
    "medium"
  } else if (duplication_rate > 0.01 || n_groups > 5) {
    "low"
  } else {
    "low"
  }
}

#' @title Detect near-duplicate rows using similarity measures
#'
#' @param data Dataset to analyse
#' @param numeric_features Names of numeric features
#' @param categorical_features Names of categorical features
#' @param config Configuration parameters
#' @return List with near-duplicate results
#' @keywords internal
detect_near_duplicates <- function(data, numeric_features, categorical_features, config) {

  issues <- data.frame()

  # Configuration for near-duplicate detection
  similarity_threshold <- config$similarity_threshold %||% 0.95
  min_features_match <- config$min_features_match %||% 0.8

  tryCatch({
    # Calculate pairwise similarities
    similarity_result <- calculate_pairwise_similarity(data, numeric_features,
                                                       categorical_features,
                                                       similarity_threshold)

    near_duplicate_pairs <- similarity_result$similar_pairs

    if (nrow(near_duplicate_pairs) == 0) {
      evidence <- list(
        n_comparisons = similarity_result$n_comparisons,
        similarity_threshold = similarity_threshold,
        n_similar_pairs = 0
      )
      return(list(issues = issues, evidence = evidence))
    }

    # Group similar pairs into clusters
    duplicate_clusters <- cluster_similar_pairs(near_duplicate_pairs, nrow(data))

    # Calculate statistics
    n_clustered_rows <- sum(sapply(duplicate_clusters, length))
    near_duplication_rate <- (n_clustered_rows - length(duplicate_clusters)) / nrow(data)

    if (length(duplicate_clusters) > 0) {
      severity <- if (near_duplication_rate > 0.05) "medium" else "low"

      issue <- data.frame(
        issue_type = "near_duplicates",
        severity = severity,
        description = sprintf("%d near-duplicate clusters found (%d rows affected, %.1f%% similarity threshold)",
                              length(duplicate_clusters), n_clustered_rows, similarity_threshold * 100),
        suggested_fix = "Review similar rows for potential data quality issues or consider deduplication"
      )

      issues <- rbind(issues, issue)
    }

    evidence <- list(
      duplicate_clusters = duplicate_clusters,
      similar_pairs = near_duplicate_pairs,
      n_clusters = length(duplicate_clusters),
      n_affected_rows = n_clustered_rows,
      similarity_threshold = similarity_threshold,
      average_cluster_size = mean(sapply(duplicate_clusters, length)),
      largest_cluster_size = max(sapply(duplicate_clusters, length))
    )

  }, error = function(e) {
    evidence <- list(error = as.character(e), note = "Near-duplicate detection failed")
  })

  return(list(issues = issues, evidence = evidence))
}

#' @title Calculate pairwise similarity between rows
#'
#' @keywords internal
calculate_pairwise_similarity <- function(data, numeric_features, categorical_features, threshold) {

  n_rows <- nrow(data)
  similar_pairs <- data.frame()

  # Prepare data for efficient comparison
  if (length(numeric_features) > 0) {
    numeric_data <- as.matrix(data[, numeric_features, drop = FALSE])
    # Standardise numeric features
    numeric_data <- scale(numeric_data)
  } else {
    numeric_data <- NULL
  }

  if (length(categorical_features) > 0) {
    categorical_data <- data[, categorical_features, drop = FALSE]
  } else {
    categorical_data <- NULL
  }

  # Use sampling for large datasets
  max_comparisons <- 10000
  if (n_rows * (n_rows - 1) / 2 > max_comparisons) {
    # Sample pairs to compare
    sample_indices <- sample(n_rows, min(200, n_rows))
    comparison_indices <- expand.grid(i = sample_indices, j = sample_indices)
    comparison_indices <- comparison_indices[comparison_indices$i < comparison_indices$j, ]
  } else {
    # Compare all pairs
    comparison_indices <- expand.grid(i = 1:n_rows, j = 1:n_rows)
    comparison_indices <- comparison_indices[comparison_indices$i < comparison_indices$j, ]
  }

  n_comparisons <- nrow(comparison_indices)

  # Calculate similarities
  for (k in seq_len(min(n_comparisons, max_comparisons))) {
    i <- comparison_indices$i[k]
    j <- comparison_indices$j[k]

    similarity <- calculate_row_similarity(i, j, numeric_data, categorical_data)

    if (similarity >= threshold) {
      similar_pairs <- rbind(similar_pairs, data.frame(
        row1 = i,
        row2 = j,
        similarity = similarity
      ))
    }
  }

  list(
    similar_pairs = similar_pairs,
    n_comparisons = k,
    threshold_used = threshold
  )
}

#' @title Calculate similarity between two specific rows
#'
#' @keywords internal
calculate_row_similarity <- function(i, j, numeric_data, categorical_data) {

  similarities <- numeric(0)

  # Numeric similarity (cosine similarity)
  if (!is.null(numeric_data)) {
    vec1 <- numeric_data[i, ]
    vec2 <- numeric_data[j, ]

    # Handle missing values
    valid_cols <- !is.na(vec1) & !is.na(vec2)
    if (sum(valid_cols) > 0) {
      vec1_clean <- vec1[valid_cols]
      vec2_clean <- vec2[valid_cols]

      # Cosine similarity
      dot_product <- sum(vec1_clean * vec2_clean)
      norm1 <- sqrt(sum(vec1_clean^2))
      norm2 <- sqrt(sum(vec2_clean^2))

      if (norm1 > 0 && norm2 > 0) {
        numeric_similarity <- dot_product / (norm1 * norm2)
        similarities <- c(similarities, numeric_similarity)
      }
    }
  }

  # Categorical similarity (Jaccard similarity)
  if (!is.null(categorical_data)) {
    cat1 <- categorical_data[i, ]
    cat2 <- categorical_data[j, ]

    # Convert to character for comparison
    cat1_char <- as.character(unlist(cat1))
    cat2_char <- as.character(unlist(cat2))

    # Handle missing values
    valid_positions <- !is.na(cat1_char) & !is.na(cat2_char)
    if (sum(valid_positions) > 0) {
      cat1_valid <- cat1_char[valid_positions]
      cat2_valid <- cat2_char[valid_positions]

      # Jaccard similarity
      intersection <- sum(cat1_valid == cat2_valid)
      union <- length(cat1_valid)  # Same length as cat2_valid

      if (union > 0) {
        categorical_similarity <- intersection / union
        similarities <- c(similarities, categorical_similarity)
      }
    }
  }

  # Weighted average of similarities
  if (length(similarities) > 0) {
    mean(similarities)
  } else {
    0
  }
}

#' @title Cluster similar pairs into groups
#'
#' @keywords internal
cluster_similar_pairs <- function(similar_pairs, n_rows) {

  if (nrow(similar_pairs) == 0) {
    return(list())
  }

  # Create adjacency list
  adjacency <- vector("list", n_rows)

  for (k in seq_len(nrow(similar_pairs))) {
    i <- similar_pairs$row1[k]
    j <- similar_pairs$row2[k]

    adjacency[[i]] <- c(adjacency[[i]], j)
    adjacency[[j]] <- c(adjacency[[j]], i)
  }

  # Find connected components using DFS
  visited <- rep(FALSE, n_rows)
  clusters <- list()

  for (i in 1:n_rows) {
    if (!visited[i] && length(adjacency[[i]]) > 0) {
      # Start DFS from this node
      cluster <- c()
      stack <- i

      while (length(stack) > 0) {
        node <- stack[length(stack)]
        stack <- stack[-length(stack)]

        if (!visited[node]) {
          visited[node] <- TRUE
          cluster <- c(cluster, node)

          # Add unvisited neighbours to stack
          neighbours <- adjacency[[node]]
          unvisited_neighbours <- neighbours[!visited[neighbours]]
          stack <- c(stack, unvisited_neighbours)
        }
      }

      if (length(cluster) > 1) {  # Only keep clusters with multiple items
        clusters[[length(clusters) + 1]] <- sort(cluster)
      }
    }
  }

  clusters
}

#' @title Detect ID-based duplicates
#'
#' @param data Dataset
#' @param id_col ID column name
#' @param config Configuration parameters
#' @return List with ID duplicate results
#' @keywords internal
detect_id_duplicates <- function(data, id_col, config) {

  issues <- data.frame()

  ids <- data[[id_col]]

  # Check for missing IDs
  missing_ids <- sum(is.na(ids))

  # Find duplicate IDs
  id_counts <- table(ids, useNA = "ifany")
  duplicate_ids <- names(id_counts)[id_counts > 1]

  # Remove NA from duplicate_ids if present
  duplicate_ids <- duplicate_ids[!is.na(duplicate_ids)]

  if (length(duplicate_ids) > 0) {
    total_duplicate_rows <- sum(id_counts[duplicate_ids]) - length(duplicate_ids)

    severity <- if (length(duplicate_ids) > nrow(data) * 0.01) "high" else "medium"

    issue <- data.frame(
      issue_type = "duplicate_ids",
      severity = severity,
      description = sprintf("%d duplicate ID values found (%d total duplicate rows)",
                            length(duplicate_ids), total_duplicate_rows),
      suggested_fix = sprintf("Remove duplicate entries for ID column '%s' or investigate data integrity", id_col)
    )

    issues <- rbind(issues, issue)
  }

  # Check for missing IDs
  if (missing_ids > 0) {
    missing_rate <- missing_ids / length(ids)
    if (missing_rate > 0.05) {  # More than 5% missing
      severity <- if (missing_rate > 0.2) "medium" else "low"

      issue <- data.frame(
        issue_type = "missing_ids",
        severity = severity,
        description = sprintf("%.1f%% of rows have missing ID values (%d out of %d)",
                              missing_rate * 100, missing_ids, length(ids)),
        suggested_fix = "Investigate missing ID values and consider data quality improvements"
      )

      issues <- rbind(issues, issue)
    }
  }

  evidence <- list(
    duplicate_ids = duplicate_ids,
    n_duplicate_ids = length(duplicate_ids),
    missing_ids = missing_ids,
    missing_id_rate = missing_ids / length(ids),
    id_uniqueness_rate = length(unique(ids[!is.na(ids)])) / sum(!is.na(ids))
  )

  return(list(issues = issues, evidence = evidence))
}

#' @title Detect subset duplicates (rows that are subsets of other rows)
#'
#' @param data Dataset
#' @param config Configuration parameters
#' @return List with subset duplicate results
#' @keywords internal
detect_subset_duplicates <- function(data, config) {

  issues <- data.frame()

  tryCatch({
    # Look for rows where one is a subset of another (considering missing values)
    subset_relationships <- find_subset_relationships(data)

    if (length(subset_relationships) == 0) {
      evidence <- list(n_subset_relationships = 0)
      return(list(issues = issues, evidence = evidence))
    }

    # Count affected rows
    affected_rows <- unique(unlist(subset_relationships))

    if (length(subset_relationships) > 0) {
      severity <- if (length(affected_rows) > nrow(data) * 0.02) "medium" else "low"

      issue <- data.frame(
        issue_type = "subset_duplicates",
        severity = severity,
        description = sprintf("%d subset relationships found among %d rows",
                              length(subset_relationships), length(affected_rows)),
        suggested_fix = "Review rows with subset relationships for data quality issues"
      )

      issues <- rbind(issues, issue)
    }

    evidence <- list(
      subset_relationships = subset_relationships,
      n_relationships = length(subset_relationships),
      n_affected_rows = length(affected_rows)
    )

  }, error = function(e) {
    evidence <- list(error = as.character(e), note = "Subset duplicate detection failed")
  })

  return(list(issues = issues, evidence = evidence))
}

#' @title Find subset relationships between rows
#'
#' @keywords internal
find_subset_relationships <- function(data) {

  n_rows <- nrow(data)
  relationships <- list()

  # Only check reasonable number of pairs
  if (n_rows > 1000) {
    # Sample for large datasets
    sample_size <- min(500, n_rows)
    sample_indices <- sample(n_rows, sample_size)
    check_indices <- expand.grid(i = sample_indices, j = sample_indices)
  } else {
    check_indices <- expand.grid(i = 1:n_rows, j = 1:n_rows)
  }

  check_indices <- check_indices[check_indices$i != check_indices$j, ]

  for (k in seq_len(min(nrow(check_indices), 5000))) {  # Limit comparisons
    i <- check_indices$i[k]
    j <- check_indices$j[k]

    if (is_subset_row(data[i, ], data[j, ])) {
      relationships[[length(relationships) + 1]] <- c(i, j)
    }
  }

  relationships
}

#' @title Check if row1 is a subset of row2
#'
#' @keywords internal
is_subset_row <- function(row1, row2) {

  # Row1 is a subset of row2 if all non-missing values in row1 match row2
  # and row1 has more missing values than row2

  row1_vals <- unlist(row1)
  row2_vals <- unlist(row2)

  row1_missing <- is.na(row1_vals)
  row2_missing <- is.na(row2_vals)

  # Row1 should have more missing values than row2
  if (sum(row1_missing) <= sum(row2_missing)) {
    return(FALSE)
  }

  # All non-missing values in row1 should match row2
  non_missing_row1 <- !row1_missing

  if (sum(non_missing_row1) == 0) {
    return(FALSE)  # Row1 is all missing
  }

  # Check if non-missing values match
  matches <- row1_vals[non_missing_row1] == row2_vals[non_missing_row1]

  # Handle potential NA comparisons
  matches[is.na(matches)] <- FALSE

  all(matches)
}

#' @title Detect cluster-based duplicates using unsupervised clustering
#'
#' @param data Dataset
#' @param numeric_features Numeric feature names
#' @param config Configuration parameters
#' @return List with cluster-based duplicate results
#' @keywords internal
detect_cluster_based_duplicates <- function(data, numeric_features, config) {

  issues <- data.frame()

  if (length(numeric_features) < 2) {
    evidence <- list(note = "Insufficient numeric features for clustering analysis")
    return(list(issues = issues, evidence = evidence))
  }

  tryCatch({
    # Prepare numeric data for clustering
    numeric_data <- data[, numeric_features, drop = FALSE]

    # Remove rows with too many missing values
    missing_threshold <- 0.5
    row_missing_rate <- apply(is.na(numeric_data), 1, mean)
    valid_rows <- row_missing_rate < missing_threshold

    if (sum(valid_rows) < 10) {
      evidence <- list(note = "Insufficient valid rows for clustering analysis")
      return(list(issues = issues, evidence = evidence))
    }

    clean_data <- numeric_data[valid_rows, , drop = FALSE]

    # Impute remaining missing values with median
    for (col in names(clean_data)) {
      clean_data[[col]][is.na(clean_data[[col]])] <- median(clean_data[[col]], na.rm = TRUE)
    }

    # Standardise data
    scaled_data <- scale(clean_data)

    # Perform clustering (using kmeans with automatic k selection)
    cluster_result <- perform_duplicate_clustering(scaled_data, config)

    # Analyse clusters for potential duplicates
    duplicate_clusters <- analyse_cluster_duplicates(cluster_result, clean_data, config)

    if (length(duplicate_clusters$suspicious_clusters) > 0) {
      n_suspicious_rows <- sum(sapply(duplicate_clusters$suspicious_clusters, length))

      severity <- if (n_suspicious_rows > nrow(data) * 0.03) "medium" else "low"

      issue <- data.frame(
        issue_type = "cluster_duplicates",
        severity = severity,
        description = sprintf("%d suspicious clusters found with %d potentially duplicate rows",
                              length(duplicate_clusters$suspicious_clusters), n_suspicious_rows),
        suggested_fix = "Review clustered groups for potential near-duplicates or data anomalies"
      )

      issues <- rbind(issues, issue)
    }

    evidence <- list(
      clustering_method = cluster_result$method,
      n_clusters = cluster_result$n_clusters,
      suspicious_clusters = duplicate_clusters$suspicious_clusters,
      cluster_sizes = cluster_result$cluster_sizes,
      within_cluster_ss = cluster_result$within_ss
    )

  }, error = function(e) {
    evidence <- list(error = as.character(e), note = "Cluster-based duplicate detection failed")
  })

  return(list(issues = issues, evidence = evidence))
}

#' @title Perform clustering for duplicate detection
#'
#' @keywords internal
perform_duplicate_clustering <- function(scaled_data, config) {

  n_rows <- nrow(scaled_data)

  # Determine optimal number of clusters
  max_k <- min(10, floor(n_rows / 3))

  if (max_k < 2) {
    return(list(method = "insufficient_data", n_clusters = 0))
  }

  # Try different k values and select based on within-cluster sum of squares
  wss_values <- numeric(max_k)

  for (k in 2:max_k) {
    tryCatch({
      kmeans_result <- kmeans(scaled_data, centers = k, nstart = 10)
      wss_values[k] <- kmeans_result$tot.withinss
    }, error = function(e) {
      wss_values[k] <- Inf
    })
  }

  # Select k using elbow method (simplified)
  if (sum(is.finite(wss_values)) >= 2) {
    # Find the "elbow" - where improvement starts to diminish
    wss_diff <- diff(wss_values[is.finite(wss_values)])
    optimal_k <- which.max(wss_diff) + 1
    optimal_k <- max(2, min(optimal_k, max_k))
  } else {
    optimal_k <- 2
  }

  # Perform final clustering
  final_kmeans <- kmeans(scaled_data, centers = optimal_k, nstart = 20)

  list(
    method = "kmeans",
    n_clusters = optimal_k,
    cluster_assignments = final_kmeans$cluster,
    cluster_centers = final_kmeans$centers,
    within_ss = final_kmeans$tot.withinss,
    cluster_sizes = table(final_kmeans$cluster)
  )
}

#' @title Analyse clusters for duplicate patterns
#' @keywords internal
analyse_cluster_duplicates <- function(cluster_result, original_data, config) {

  if (cluster_result$method == "insufficient_data") {
    return(list(suspicious_clusters = list()))
  }

  cluster_assignments <- cluster_result$cluster_assignments
  suspicious_clusters <- list()

  # Analyse each cluster
  for (cluster_id in unique(cluster_assignments)) {
    cluster_indices <- which(cluster_assignments == cluster_id)

    if (length(cluster_indices) < 2) {
      next
    }

    # Calculate within-cluster similarity
    cluster_data <- original_data[cluster_indices, , drop = FALSE]

    # Check if cluster members are very similar
    similarity_score <- calculate_cluster_similarity(cluster_data)

    # Flag clusters with high internal similarity
    similarity_threshold <- config$cluster_similarity_threshold %||% 0.9

    if (similarity_score > similarity_threshold) {
      suspicious_clusters[[paste0("cluster_", cluster_id)]] <- cluster_indices
    }
  }

  list(suspicious_clusters = suspicious_clusters)
}

#' @title Calculate average within-cluster similarity
#' @keywords internal
calculate_cluster_similarity <- function(cluster_data) {

  n_rows <- nrow(cluster_data)

  if (n_rows < 2) {
    return(0)
  }

  # Calculate pairwise similarities within cluster
  similarities <- numeric(0)

  for (i in 1:(n_rows - 1)) {
    for (j in (i + 1):n_rows) {
      # Calculate cosine similarity for numeric data
      vec1 <- as.numeric(cluster_data[i, ])
      vec2 <- as.numeric(cluster_data[j, ])

      # Handle missing values
      valid_positions <- !is.na(vec1) & !is.na(vec2)

      if (sum(valid_positions) > 0) {
        vec1_clean <- vec1[valid_positions]
        vec2_clean <- vec2[valid_positions]

        # Cosine similarity
        dot_product <- sum(vec1_clean * vec2_clean)
        norm1 <- sqrt(sum(vec1_clean^2))
        norm2 <- sqrt(sum(vec2_clean^2))

        if (norm1 > 0 && norm2 > 0) {
          similarity <- dot_product / (norm1 * norm2)
          similarities <- c(similarities, similarity)
        }
      }
    }
  }

  if (length(similarities) > 0) {
    mean(similarities)
  } else {
    0
  }
}

#' @title Null-coalescing operator
#' @keywords internal
