import pandas as pd
import re
from bug_fix_list import bug_words
from internal_list import internal_words
from external_list import external_words
from functional_list import functional_words
from code_smell_list import smell_words

patterns = bug_words + internal_words + external_words + functional_words + smell_words

pd.set_option('display.max_rows', None)
details_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

regex = re.compile('|'.join(map(re.escape, patterns)), re.IGNORECASE)
mask = details_df['message'].str.contains(regex, na=False)
num_matches = mask.sum()

print(f"Number of matching commit messages: {num_matches}")
