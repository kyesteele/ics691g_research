import pandas as pd

pd.set_option('display.max_rows', None)

pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")

# Which programming languages are most commonly used in agentic PRs?
languages = {}
pr_with_lang = pr_df.merge(repo_df[['id', 'language']], left_on='repo_id', right_on='id', how='left')
with open("pr_lang_counts.txt", "w") as f:
    f.write(pr_with_lang['language'].value_counts().to_string())

first_five = pr_with_lang['language'].value_counts().head(5)
print(first_five)
