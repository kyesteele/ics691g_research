from enum import unique
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
pull_request_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
comments_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_comments.parquet")
pr_task_type_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_task_type.parquet")
print('Finished querying parquets.')
print(len(pull_request_df))

pull_request_df.rename(columns={'id': 'pr_id'}, inplace=True)

# need body from pr_comments
combined_df = pull_request_df.merge(
    comments_df[['id', 'body']].add_prefix('comment_'),
    how='left',
    left_on='pr_id',
    right_on='comment_id',
    suffixes=('', '_task')
)
combined_df.drop(columns=['comment_id'])

# only look at refactor
combined_df = combined_df.merge(
    pr_task_type_df[['id', 'type']],
    how='left',
    left_on='pr_id',
    right_on='id'
)
combined_df.rename(columns={'type': 'pr_type'}, inplace=True)
combined_df = combined_df.loc[combined_df['pr_type'].str.contains('refactor', na=False)].copy()

print(combined_df['pr_id'].nunique())

print(f"Length of combined_df: {len(combined_df)}")

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
    combined_df['body'].str.contains(all_patterns, na=False) |
    combined_df['comment_body'].str.contains(all_patterns, na=False) |
    combined_df['title'].str.contains(all_patterns, na=False)
)
print('Finished searching any pattern.')

# search for specific category of pattern (bug, internal, external, functional, smell)
combined_df['bug'] = (
    combined_df['body'].str.contains(bug_patterns, na=False) |
    combined_df['comment_body'].str.contains(bug_patterns, na=False) |
    combined_df['title'].str.contains(bug_patterns, na=False)
)
print('Finished searching bug patterns.')
combined_df['internal'] = (
    combined_df['body'].str.contains(internal_patterns, na=False) |
    combined_df['comment_body'].str.contains(internal_patterns, na=False) |
    combined_df['title'].str.contains(internal_patterns, na=False)
)
print('Finished searching internal patterns.')
combined_df['external'] = (
    combined_df['body'].str.contains(external_patterns, na=False) |
    combined_df['comment_body'].str.contains(external_patterns, na=False) |
    combined_df['title'].str.contains(external_patterns, na=False)
)
print('Finished searching external patterns.')
combined_df['functional'] = (
    combined_df['body'].str.contains(functional_patterns, na=False) |
    combined_df['comment_body'].str.contains(functional_patterns, na=False) |
    combined_df['title'].str.contains(functional_patterns, na=False)
)
print('Finished searching functional patterns.')
combined_df['smell'] = (
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

# add column for merge time in DAYS instead of seconds
combined_df['merge_time'] = (pd.to_datetime(combined_df['merged_at']) - pd.to_datetime(combined_df['created_at'])).dt.total_seconds() / 86400

unique_prs = combined_df.drop_duplicates(subset=['pr_id'])

total_requests = (
    unique_prs
    .groupby(['agent', 'is_sar'])
    .size()
    # ** issue with my python LSP, not a problem at runtime **
    .reset_index(name='total_requests') # type: ignore
)

agents = unique_prs['agent'].unique()
sar_flags = [True, False]
all_groups = pd.MultiIndex.from_product([agents, sar_flags], names=['agent', 'is_sar'])

total_merged = (
    unique_prs[unique_prs['merged_at'].notna()]
    .groupby(['agent', 'is_sar'])['merge_time']
    .size()
    .reindex(all_groups, fill_value=0)
    .reset_index(name='total_merged') # type: ignore
)

# find average of both sar and non_sar, and drop unmerged PRs
average_merged = (
    unique_prs[unique_prs['merged_at'].notna()]
    .groupby(['agent', 'is_sar'])['merge_time']
    .mean()
    .round(2)
    .reindex(all_groups, fill_value='n/a')
    .reset_index(name='average_merge_time(days)') # type: ignore
)

sar_categories = (
    unique_prs
    .groupby(['agent', 'is_sar'])
    [['bug', 'internal', 'external', 'functional', 'smell']]
    .sum()
    .reset_index()
)

sar_locations = (
    unique_prs
    .groupby(['agent', 'is_sar'])
    [['sar_in_pr_title', 'sar_in_pr_body', 'sar_in_pr_comment']]
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
summary['merge_rate(%)'] = ((summary['total_merged'] / summary['total_requests']).round(2) * 100).astype(int)

with open("scan_all_pr_message_output.txt", "w") as f:
    f.write(summary.to_string())
