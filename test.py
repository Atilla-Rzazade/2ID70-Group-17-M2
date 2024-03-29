import pandas as pd
data = pd.read_csv('./src/main/resources/smallDataset/events.csv', on_bad_lines = 'skip', names=["SeriesId", "Timestamp", "Event"])
data['Event'] = pd.to_numeric(data['Event'], errors='coerce')
data = data.dropna(subset=['Event'], axis=0)

# Print the resulting DataFrame
print(data.head())

value_counts = data['Timestamp'].value_counts()

# Filter values appearing less than 4 times
less_than_4 = value_counts[value_counts < 5]

# Filter values appearing more than 4 times
more_than_4 = value_counts[value_counts > 5]

print("Values appearing less than 5 times:")
print(len(less_than_4))

print("\nValues appearing more than 5 times:")
print(len(more_than_4))

print(data[data["Timestamp"] == 192144])
