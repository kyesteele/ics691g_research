import pandas as pd

pd.set_option('display.max_rows', None)

# load datasets
pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
pr_reviews_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_reviews.parquet")
comment_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_review_comments.parquet")

# How does the programming language of an agentic PR relate to the number of reviewers assigned per pull request?
# ** If there is a better way, let me know. Otherwise, I will use the number of unique users that comment on a PR review (excluding the PR author) for PRs that were actually reviewed. **
languages = {}

# merge PRs with language
pr_with_lang = pr_df.merge(
    repo_df[['id','language']],
    left_on='repo_id',
    right_on='id',
    how='left',
    suffixes=('_pr','_repo')
)
pr_with_lang['num_reviewers'] = 0

# merge comments with pr_reviews
comments_with_pr = comment_df.merge(
    pr_reviews_df[['id','pr_id']],
    left_on='pull_request_review_id',
    right_on='id',
    how='left',
    suffixes=('_comment','_review')
)

# count unique reviewers (excluding pr author) for each pull request
for i, pr in pr_with_lang.iterrows():
    pr_id = pr['id_pr']
    author = pr['user_id']
    comments = comments_with_pr[comments_with_pr['pr_id'] == pr_id]
    unique_reviewers = set(comments['user'])
    if author in unique_reviewers:
        unique_reviewers.remove(author)
    pr_with_lang.at[i,'num_reviewers'] = len(unique_reviewers)


reviewed_prs = pr_with_lang[pr_with_lang['num_reviewers'] > 0]
avg_reviewers_lang = reviewed_prs.groupby('language')['num_reviewers'].mean().sort_values(ascending=False).round(2)

with open("pr_lang_reviewer_counts.txt", "w") as f:
    f.write(avg_reviewers_lang.to_string())

first_five = avg_reviewers_lang.head(5)
print(first_five)
