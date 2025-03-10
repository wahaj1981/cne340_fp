# Install dependencies as needed:
# pip install kagglehub[pandas-datasets]
import kagglehub
from kagglehub import KaggleDatasetAdapter

dataset_name = "jishnukoliyadan/gold-price-1979-present"
dataset_files = kagglehub.list_files(dataset_name)
print("Available files in dataset:", dataset_files)

# Set the path to the file you'd like to load
file_path = "gold_price_data_.csv"
try:
    # Load the latest dataset version
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        dataset_name,
        file_path,
    )

    # Display first few records
    print("First 5 records:\n", df.head())

except Exception as e:
    print("Error loading dataset:", e)
