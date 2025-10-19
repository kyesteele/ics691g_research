import pandas as pd
import re
from bug_fix_list import bug_words
from internal_list import internal_words
from external_list import external_words
from functional_list import functional_words
from code_smell_list import smell_words

# How do time-to-first-review and time-to-merge differ between PRs with vs. without SAR patterns?
# -- How do these metrics vary by agent?
# -- How do they compare against non-SAR PRs?
# Compare individual agent times for SAR vs non-SAR


pd.set_option('display.max_rows', None)
details_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
pull_request_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")

# adds agent, created_at, and merged_at from pull_request_df into details_df
combined_df = details_df.merge(
    pull_request_df[['id', 'agent', 'created_at', 'merged_at']],
    how='left',
    left_on='pr_id',
    right_on='id'
)
# drop duplicate id
combined_df.drop(columns=['id'])

# combine all pattern
patterns = bug_words + internal_words + external_words + functional_words + smell_words
regex = re.compile('|'.join(map(re.escape, patterns)), re.IGNORECASE)

# search for any pattern
combined_df['is_sar'] = combined_df['message'].str.contains(regex, na=False)
# add column for merge time in DAYS instead of seconds
combined_df['merge_time'] = (pd.to_datetime(combined_df['merged_at']) - pd.to_datetime(combined_df['created_at'])).dt.total_seconds() / 86400

# find average of both sar and non_sar, and drop unmerged PRs
averages = (
    combined_df[combined_df['merged_at'].notna()]
    .groupby(['agent', 'is_sar'])['merge_time']
    .mean()
    .reset_index()
)

print(averages)
