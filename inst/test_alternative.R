
# Alternative: Fix the test data creation
# Instead of: df_big <- df[rep(seq_len(nrow(df)), 3), ]
# Use: df_big <- do.call(rbind, replicate(3, df, simplify = FALSE))

# Or even simpler, just create df_big properly:
df <- data.frame(date = as.Date("2025-01-01") + 0:20)
df_big <- data.frame(date = rep(df$date, 3))  # This preserves column names

