from pyjarowinkler import distance
import pandas as pd
import re

# Il confronto avviene da golden standard a final_matches_cleaned

def main():
    df = pd.read_csv(
        "./data/restaurants_integrated/output_recordlinkage/final_matches_ecm_cleaned.csv", sep=";")
    with open('./data/restaurants/restaurants_golden.txt', 'r', encoding='UTF-8') as f:
        # targets = [line.rstrip('\n') for line in f if 'Y' in line]    se serve estrarre solo aperti/chiusi inserire y o n
        data = pd.DataFrame([re.split(r'\t+', record)
                             for record in f], columns=('Name', 'Open'))    # Splitto le righe per tabulazione
        data.apply(find_closest, df=df, axis=1)


def find_closest(row: pd.Series, df: pd.DataFrame):
    match_dist = [0]
    match_row = [0]
    # Per ogni riga del dataset cleaned trova la distanza Jaro Winkler maggiore
    df.apply(jaro_winkler, golden_name=row['Name'],
             match_dist=match_dist, match_row=match_row, axis=1)
    # Viene stampato il match migliore per ogni record del golden standard
    print(
        f"Golden: {row['Name']}\nBest match -> Distance: {match_dist[0]}\n{match_row[0]}\n")


def jaro_winkler(row: pd.Series, golden_name: str, match_dist: list, match_row: list):
    dist = distance.get_jaro_distance(
        golden_name, row['Name'], winkler=True, scaling=0.1)
    # match_dist e match_row sono liste. Ora aggiorno sempre il primo elemento per tornare il massimo
    # ma si possono anche appendere e lavorare sulle liste in output.
    if dist > match_dist[0]:
        match_dist[0] = dist
        match_row[0] = row.copy()


if __name__ == "__main__":
	main()
