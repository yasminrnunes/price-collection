import re
import unicodedata
from sentence_transformers import SentenceTransformer, util, CrossEncoder
from rapidfuzz import fuzz, process
#from sql_client import create_query_client
import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score, accuracy_score
import time



## Analysis 1: Applying only normalization
## Normalization: Removing accents, special characters, duplicated spaces and capitalizing the first letter
## Creating variants by removing spaces and adding 's' to the end of the word 
## Then, comparing the variants with the existing brands

def normalize_word(word):
    # Convert to string if not already
    if not isinstance(word, str):
        word = str(word)
    # lowercase
    word = word.lower()
    # remove accents
    word = ''.join(c for c in unicodedata.normalize('NFD', word) if unicodedata.category(c) != 'Mn')
    # remove special caracters
    word = re.sub(r'[^a-z0-9 ]', '', word)
    # remover duplicated space
    word = re.sub(r'\s+', ' ', word).strip()
    # capitalize first letter
    word = word.capitalize()
    return word


def generate_variants(word: str) -> list[str]:
    """
    Generate normalized variants of a word to improve fuzzy matching.
    Includes:
    - Base normalized word
    - Version without spaces
    - Singular/plural variants
    """
    base = normalize_word(word)
    variants = {base}  # use a set to avoid duplicates

    # Remove spaces
    no_space = base.replace(" ", "")
    variants.add(no_space)

    # Handle plural/singular (basic rule: add or remove final 's')
    if base.endswith("s"):
        variants.add(base[:-1])   # remove final 's'
    else:
        variants.add(base + "s")  # add final 's'

    # Do the same for no_space version
    if no_space.endswith("s"):
        variants.add(no_space[:-1])
    else:
        variants.add(no_space + "s")

    return list(variants)


def check_brand_exists(new_brand: str, existing_brands: list[str]):
    """
    Check if a new brand already exists in the database (direct).
    Returns:
        - (True, match) if found
        - (False, None) if not found
    """
    # normalize existing brands
    normalized_existing = {normalize_word(b).replace(" ", ""): b for b in existing_brands}

    # generate variants for the new brand
    #variants = {v.replace(" ", "") for v in generate_variants(new_brand)}
    variants = generate_variants(new_brand)

    # Usar operações de lista
    for variant in variants:
        if variant in normalized_existing:
            return 1, variant
    return 0, None



## Aanalysis 2: Normalizing and applying fuzzy matching in the brand name
## Normalization: Removing accents, special characters, duplicated spaces and capitalizing the first letter
## Creating variants by removing spaces and adding 's' to the end of the word 
## Then, comparing the variants with the existing brands directly and using fuzzy matching
def check_brand_exists_levenshtein(new_brand: str, existing_brands: list[str],threshold: int = 90):
    """
    Compare brand_1 variants against normalized brand_2 (without variants of brand_2).
    Returns best score and match result.
    """
    # gerar variantes só para brand_1
    variants = generate_variants(new_brand)

    # normalizar brand_2 (sem variantes)
    normalized_existing = {normalize_word(b): b for b in existing_brands}

    best_score = 0
    best_match = 0

    for variant in variants:
        if variant in normalized_existing:
            return 1, variant, None
        else:
            match, score, _ = process.extractOne(
                normalize_word(new_brand), 
                list(normalized_existing.keys()), 
                scorer=fuzz.ratio
            )

            if score > best_score:
                best_score = score
                best_match = variant
            
    if best_score >= threshold:
        return 1, best_match,best_score
    else:
        return 0, best_match,best_score



## Applying the analysis
# Reading the train data from an excel file
train_data = pd.read_excel("mapping_brand.xlsx")

for index, raw_data in train_data.iterrows():
    brand_id = raw_data["id"]
    brand_1 = raw_data["brand_1"]
    brand_2 = normalize_word(raw_data["brand_2"])  # normalizar apenas brand_2

    
    result_levenshtein = check_brand_exists_levenshtein(brand_1, [brand_2])
    match_levenshtein = result_levenshtein[0]
    brand_match_levenshtein = result_levenshtein[1]
    score_levenshtein = result_levenshtein[2]

    #print(f"{brand_1} vs {brand_2} -> match: {match_levenshtein}, score: {score_levenshtein}, brand_match: {brand_match_levenshtein}")

    result_norm = check_brand_exists(brand_1, [brand_2])
    match_norm = result_norm[0]
    print(f"{brand_1} vs {brand_2} ->  match: {match_norm}")

    # Adding the result to a dataframe train_data
    train_data.loc[index, "match_levenshtein"] = int(match_levenshtein)
    train_data.loc[index, "score_levenshtein"] = score_levenshtein
    train_data.loc[index, "brand_match_levenshtein"] = brand_match_levenshtein
    train_data.loc[index, "match_norm"] = int(match_norm)


# Creating the confusion matrix for option 1 - normalization and variant generation
tn, fp, fn, tp = confusion_matrix(train_data["label"], train_data["match_norm"]).ravel()

print("\nConfusion matrix for norm_match:")
print(f"TP: {tp}, FP: {fp}, FN: {fn}, TN: {tn}")

# Creating the confusion matrix for the levensthein analysis - threshold 90
tn, fp, fn, tp = confusion_matrix(train_data["label"], train_data["match_levenshtein"]).ravel()

print("\nConfusion matrix for levensthein - threshold 90:")
print(f"TP: {tp}, FP: {fp}, FN: {fn}, TN: {tn}")



# Function to evaluate fuzzy matching performance given a threshold
def evaluate_fuzzy(df: pd.DataFrame):
    """
    Evaluate fuzzy matching results:
    - Finds the optimal threshold that maximizes precision (on valid data only)
    - Applies it to create predictions
    - Rejoins NaN rows keeping their original match_levenshtein values
    - Computes confusion matrix and performance metrics on the full dataset

    Args:
        df (pd.DataFrame): DataFrame with columns:
            - 'score_levenshtein': numeric similarity score (may contain NaN)
            - 'label': true label (0 or 1)
            - 'match_levenshtein': optional existing match result (for NaN rows)

    Returns:
        dict with:
            - 'best_threshold': threshold maximizing precision
            - 'metrics': dict with TP, FP, FN, TN, precision, recall, f1, accuracy
            - 'confusion_matrix': 2x2 array
            - 'df_final': combined DataFrame
    """
    # Separate valid and NaN scores
    df_nan = df[df["score_levenshtein"].isna()].copy()
    df_valid = df[df["score_levenshtein"].notna()].copy()

    if df_valid.empty:
        raise ValueError("No valid (non-NaN) scores found in 'score_levenshtein'.")

    # Find best threshold (maximize precision on valid data)
    thresholds = range(70,101,2)
    best_threshold = 0
    best_precision = 0

    for t in thresholds:
        preds = df_valid["score_levenshtein"] >= t
        precision = precision_score(df_valid["label"], preds, zero_division=0)
        if precision > best_precision:
            best_precision = precision
            best_threshold = t

    # Apply best threshold to valid data
    df_valid["temp_match_levenshtein"] = (df_valid["score_levenshtein"] >= best_threshold).astype(int)

    # Prepare df_nan:
    # keep its original match_levenshtein column (rename to temp for consistency)
    if "match_levenshtein" in df_nan.columns:
        df_nan["temp_match_levenshtein"] = df_nan["match_levenshtein"]
    else:
        df_nan["temp_match_levenshtein"] = np.nan

    # Combine valid + NaN DataFrames
    df_final = pd.concat([df_valid, df_nan], ignore_index=True)

    # Compute confusion matrix and metrics on full dataset
    tn, fp, fn, tp = confusion_matrix(
        df_final["label"], df_final["temp_match_levenshtein"]
    ).ravel()

    precision = precision_score(df_final["label"], df_final["temp_match_levenshtein"])
    recall = recall_score(df_final["label"], df_final["temp_match_levenshtein"])
    f1 = f1_score(df_final["label"], df_final["temp_match_levenshtein"])
    accuracy = accuracy_score(df_final["label"], df_final["temp_match_levenshtein"])

    metrics = {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
    }

    return {
        "best_threshold": best_threshold,
        "metrics": metrics,
        "confusion_matrix": np.array([[tp, fp], [fn, tn]]),
        "df_final": df_final,
    }

# Test thresholds from 70 to 95
result = evaluate_fuzzy(train_data)
print("Best Threshold:", result["best_threshold"])
print("Metrics with best threshold:", result["metrics"])
print("Confusion Matrix with best threshold:\n", result["confusion_matrix"])
final_df_levenshtein = result["df_final"]

# Check products considered the same - score_threshold >= 90 and score_threshold <= result["best_threshold"] - show all columns
print(final_df_levenshtein[final_df_levenshtein["score_levenshtein"].between(90, result["best_threshold"])])


