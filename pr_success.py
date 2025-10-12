import pandas as pd

pd.set_option('display.max_rows', None)

pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")

# How does the programming language of an agentic PR relate to PR merge success rates?
pr_with_lang = pr_df.merge(pd.DataFrame(repo_df[['id', 'language']]), left_on='repo_id', right_on='id', how='left')
total_counts = pd.Series(pr_with_lang['language']).value_counts()
merged_counts = pd.Series(pr_with_lang[pr_with_lang['merged_at'].notna()]['language']).value_counts()
merged_counts = merged_counts.reindex(total_counts.index, fill_value=0)
merge_percentage = (merged_counts / total_counts * 100).sort_values(ascending=False).round(2)


with open("pr_lang_success.txt", "w") as f:
    f.write(merge_percentage.to_string())
print(merge_percentage.head(5))
