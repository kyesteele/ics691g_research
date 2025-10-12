import pandas as pd

# pd.set_option('display.max_rows', None)

all_pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")
all_repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")
# all_user_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_user.parquet")

# random tests
# print(all_pr_df.columns)
# print(all_pr_df['agent'].head())
# repo_id = all_pr_df['repo_id'].iat[0]
# repo_row = all_repo_df[all_repo_df['id'] == repo_id]
# print(repo_row['language'])
