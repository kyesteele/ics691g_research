import pandas as pd

pd.set_option('display.max_rows', None)

# This is for the FULL 900k pr dataset
all_pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")
all_repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")

# Which programming languages are most commonly used in agentic PRs?
languages = {}
pr_with_lang = all_pr_df.merge(all_repo_df[['id', 'language']], left_on='repo_id', right_on='id', how='left')
with open("full_pr_lang_counts.txt", "w") as f:
    f.write(pr_with_lang['language'].value_counts().to_string())

first_five = pr_with_lang['language'].value_counts().head(5)
print(first_five)
