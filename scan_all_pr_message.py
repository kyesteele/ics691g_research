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

print('Starting script.')
pd.set_option('display.max_rows', None)
details_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
pull_request_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
comments_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_comments.parquet")
print('Finished querying parquets.')

# adds agent, created_at, and merged_at from pull_request_df into details_df
combined_df = details_df.merge(
    pull_request_df[['id', 'title', 'body', 'agent', 'created_at', 'merged_at']],
    how='left',
    left_on='pr_id',
    right_on='id'
)
# drop duplicate id
combined_df.drop(columns=['id'])

# also need body from pr_comments
combined_df = combined_df.merge(
    comments_df[['id', 'body']].add_prefix('comment_'),
    how='left',
    left_on='pr_id',
    right_on='comment_id'
)
combined_df.drop(columns=['comment_id'])

print('Beginning regex searches.')

# combine all pattern
patterns = bug_words + internal_words + external_words + functional_words + smell_words
all_patterns = re.compile('|'.join(map(re.escape, patterns)), re.IGNORECASE)
bug_patterns = re.compile('|'.join(map(re.escape, bug_words)), re.IGNORECASE)
internal_patterns = re.compile('|'.join(map(re.escape, internal_words)), re.IGNORECASE)
external_patterns = re.compile('|'.join(map(re.escape, external_words)), re.IGNORECASE)
functional_patterns = re.compile('|'.join(map(re.escape, functional_words)), re.IGNORECASE)
smell_patterns = re.compile('|'.join(map(re.escape, smell_words)), re.IGNORECASE)
print('Finished assembling regex patterns.')

print('Searching for sar patterns.')
# search for any pattern
combined_df['is_sar'] = (
    combined_df['message'].str.contains(all_patterns, na=False) |
    combined_df['body'].str.contains(all_patterns, na=False) |
    combined_df['comment_body'].str.contains(all_patterns, na=False) |
    combined_df['title'].str.contains(all_patterns, na=False)
)
print('Finished searching any pattern.')

# search for specific category of pattern (bug, internal, external, functional, smell)
combined_df['bug'] = (
    combined_df['message'].str.contains(bug_patterns, na=False) |
    combined_df['body'].str.contains(bug_patterns, na=False) |
    combined_df['comment_body'].str.contains(bug_patterns, na=False) |
    combined_df['title'].str.contains(bug_patterns, na=False)
)
print('Finished searching bug patterns.')
combined_df['internal'] = (
    combined_df['message'].str.contains(internal_patterns, na=False) |
    combined_df['body'].str.contains(internal_patterns, na=False) |
    combined_df['comment_body'].str.contains(internal_patterns, na=False) |
    combined_df['title'].str.contains(internal_patterns, na=False)
)
print('Finished searching internal patterns.')
combined_df['external'] = (
    combined_df['message'].str.contains(external_patterns, na=False) |
    combined_df['body'].str.contains(external_patterns, na=False) |
    combined_df['comment_body'].str.contains(external_patterns, na=False) |
    combined_df['title'].str.contains(external_patterns, na=False)
)
print('Finished searching external patterns.')
combined_df['functional'] = (
    combined_df['message'].str.contains(functional_patterns, na=False) |
    combined_df['body'].str.contains(functional_patterns, na=False) |
    combined_df['comment_body'].str.contains(functional_patterns, na=False) |
    combined_df['title'].str.contains(functional_patterns, na=False)
)
print('Finished searching functional patterns.')
combined_df['smell'] = (
    combined_df['message'].str.contains(smell_patterns, na=False) |
    combined_df['body'].str.contains(smell_patterns, na=False) |
    combined_df['comment_body'].str.contains(smell_patterns, na=False) |
    combined_df['title'].str.contains(smell_patterns, na=False)
)
print('Finished searching smell patterns.')

# search any category pattern in specific locations in the PR
combined_df['sar_in_pr_title'] = combined_df['title'].str.contains(all_patterns, na=False)
print('Finished searching for sar in title.')
combined_df['sar_in_pr_body'] = combined_df['body'].str.contains(all_patterns, na=False)
print('Finished searching for sar in body.')
combined_df['sar_in_pr_comment'] = combined_df['comment_body'].str.contains(all_patterns, na=False)
print('Finished searching for sar in comment.')
combined_df['sar_in_pr_commit'] = combined_df['message'].str.contains(all_patterns, na=False)
print('Finished searching for sar in commit.')

# add column for merge time in DAYS instead of seconds
combined_df['merge_time'] = (pd.to_datetime(combined_df['merged_at']) - pd.to_datetime(combined_df['created_at'])).dt.total_seconds() / 86400

total_requests = (
    combined_df
    .groupby(['agent', 'is_sar'])
    .size()
    # ** issue with my python LSP, not a problem at runtime **
    .reset_index(name='total_requests') # type: ignore
)

total_merged = (
    combined_df[combined_df['merged_at'].notna()]
    .groupby(['agent', 'is_sar'])['merge_time']
    .size()
    .reset_index(name='total_merged') # type: ignore
)

# find average of both sar and non_sar, and drop unmerged PRs
average_merged = (
    combined_df[combined_df['merged_at'].notna()]
    .groupby(['agent', 'is_sar'])['merge_time']
    .mean()
    .reset_index(name='average_merge_time') # type: ignore
)

sar_categories = (
    combined_df
    .groupby(['agent', 'is_sar'])
    [['bug', 'internal', 'external', 'functional', 'smell']]
    .sum()
    .reset_index()
)

sar_locations = (
    combined_df
    .groupby(['agent', 'is_sar'])
    [['sar_in_pr_title', 'sar_in_pr_body', 'sar_in_pr_comment', 'sar_in_pr_commit']]
    .sum()
    .reset_index()
)


summary = (
    total_requests
    .merge(total_merged, on=['agent', 'is_sar'], how='left')
    .merge(average_merged, on=['agent', 'is_sar'], how='left')
    .merge(sar_categories, on=['agent', 'is_sar'], how='left')
    .merge(sar_locations, on=['agent', 'is_sar'], how='left')
)
summary['merge_rate'] = summary['total_merged'] / summary['total_requests']

with open("scan_all_pr_message_output.txt", "w") as f:
    f.write(summary.to_string())
