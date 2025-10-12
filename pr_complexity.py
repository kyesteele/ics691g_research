import pandas as pd

pd.set_option('display.max_rows', None)

# load datasets
pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
pr_reviews_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_reviews.parquet")
comment_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_review_comments.parquet")

# How does the programming language of an agentic PR relate to the depth and complexity of review comments (e.g. comment count, word count, or discussion length)?
languages = {}

# merge PRs with language
pr_with_lang = pr_df.merge(
    repo_df[['id','language']],
    left_on='repo_id',
    right_on='id',
    how='left',
    suffixes=('_pr','_repo')
)

# merge comments with pr_reviews
comments_with_pr = comment_df.merge(
    pr_reviews_df[['id','pr_id']],
    left_on='pull_request_review_id',
    right_on='id',
    how='left',
    suffixes=('_comment','_review')
)

comment_counts = comments_with_pr.groupby('pr_id').size().reset_index(name='num_comments')
comments_with_pr['word_count'] = comments_with_pr['body'].str.split().str.len()

# merge comment counts with pr language
pr_with_lang = pr_with_lang.merge(
    comment_counts,
    left_on='id_pr',
    right_on='pr_id',
    how='left'
)

# merge word count with comments
avg_word_count_lang = comments_with_pr.merge(
    pr_with_lang[['id_pr','language']],
    left_on='pr_id',
    right_on='id_pr',
    how='left'
).groupby('language')['word_count'].mean().sort_values(ascending=False).round(2)

# only look at reviewed PRs
pr_with_lang['num_comments'] = pr_with_lang['num_comments'].fillna(0).astype(int)
reviewed_prs = pr_with_lang[pr_with_lang['num_comments'] > 0]
avg_comments_lang = reviewed_prs.groupby('language')['num_comments'].mean().sort_values(ascending=False).round(2)

with open("pr_lang_comment_counts.txt", "w") as f:
    f.write(avg_comments_lang.to_string())
with open("pr_lang_comment_word_count.txt", "w") as f:
    f.write(avg_word_count_lang.to_string())
print("Average number of comments by language:")
print(avg_comments_lang)
print("Average word count of comments by language:")
print(avg_word_count_lang)
