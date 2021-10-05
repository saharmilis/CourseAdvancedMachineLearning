## Imports
import pandas as pd
import json
from tqdm import tqdm

class JsonFullBodyText:
    def __init__(self, file_path):
        with open(file_path) as file:
            content = json.load(file)
        self.body_text = []
        # get all body text
        for entry in content['body_text']:
            self.body_text.append(entry['text'])
        self.body_text = '\n'.join(self.body_text)

# load data
root_path = r"C:\IDC\third year\Advanced ML\HW\HW3\\"
metadata_filename = "metadata.csv"
# filepath = r"C:\IDC\third year\Advanced ML\HW\HW3\\metadata.csv"
df = pd.read_csv(root_path + metadata_filename)
print(f"Total of {df.shape[0]} articles in the dataset")

# keep only papers with text
df.dropna(subset=["pdf_json_files"], inplace=True)
print(f"Total of {df.shape[0]} articles after removing records without text")

# sort by date
df.sort_values(by=["publish_time"], ascending=False, inplace=True)
df.reset_index(inplace = True)

# run over dataframe and get 10,000 articles
MAX_ARTICLES_TO_PROCESS = 10_000
num_valid_articles = 0
final_df = pd.DataFrame(columns=["title", "cord_uid", "abstract", "body_text"])

for ind, row in df.iterrows():
    temp_df = pd.DataFrame(columns=["title", "cord_uid", "abstract", "body_text"])
    json_filepath = row["pmc_json_files"] if type(row["pmc_json_files"]) == str else row["pdf_json_files"]
    if ";" in json_filepath:
        print(f"Multiple json paths for record {ind}")
        continue
    json_full_body_text = JsonFullBodyText(root_path + json_filepath)

    # get title
    title = row["title"]
    temp_df['title'] = [title]

    # get paper id
    cord_uid = row["cord_uid"]
    temp_df['cord_uid'] = [cord_uid]

    # get abstract
    temp_df['abstract'] = row["abstract"] if type(row["abstract"]) == str and len(row["abstract"]) > 0 else "no abstract"

    # get body text
    temp_df['body_text'] = [json_full_body_text.body_text]

    final_df = final_df.append(temp_df)
    num_valid_articles += 1

    print(f"Processed {num_valid_articles} / {MAX_ARTICLES_TO_PROCESS}")
    if num_valid_articles >= MAX_ARTICLES_TO_PROCESS:
        break

final_df.to_excel(root_path + "full_articles_10k.xlsx", index=False)
