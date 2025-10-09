import re
import unicodedata
from sentence_transformers import SentenceTransformer, util, CrossEncoder
from rapidfuzz import fuzz, process
from sql_client import create_query_client
import pandas as pd
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score
import time

## Aanalysis 1: Normalizing and applying fuzzy matching in the brand name
## Normalization: Removing accents, special characters, duplicated spaces and capitalizing the first letter
## Creating variants by removing spaces and adding 's' to the end of the word 
## Then, comparing the variants with the existing brands directly and using fuzzy matching

def normalize_word(word):
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


## Fuzzy Matching brands variant
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


## Fuzzy Matching for brands
def check_brand_exists(new_brand: str, existing_brands: list[str], threshold: int = 90):
    """
    Check if a new brand already exists in the database (direct or fuzzy match).
    Returns:
        - (True, match) if found
        - (False, None) if not found
    """
    # normalize existing brands
    normalized_existing = {normalize_word(b): b for b in existing_brands}

    # generate variants for the new brand
    variants = generate_variants(new_brand)

    # 1) Direct check (exact match with normalization/variants)
    for variant in variants:
        if variant in normalized_existing:
            return True, normalized_existing[variant],150

    # 2) Fuzzy matching if no exact match
    match, score, _ = process.extractOne(
        normalize_word(new_brand), 
        list(normalized_existing.keys()), 
        scorer=fuzz.ratio
    )
    if score >= threshold:
        return True, normalized_existing[match],score

    # 3) Not found
    return False, None,score

# Comparing brands
def compare_brands(brand_1: str, brand_2: str):
    """
    Compare brand_1 variants against normalized brand_2 (without variants of brand_2).
    Returns best score and match result.
    """
    # gerar variantes só para brand_1
    variants_brand_1 = generate_variants(brand_1)

    # normalizar brand_2 (sem variantes)
    normalized_brand_2 = normalize_word(brand_2).replace(" ", "")

    best_score = 0
    best_match = False

    for variant in variants_brand_1:
        clean_variant = variant.replace(" ", "")
        match_found, match_brand, score = check_brand_exists(clean_variant, [normalized_brand_2])

        if score > best_score:
            best_score = score
            best_match = match_found

    #return {"score": best_score, "match": best_match}
    return best_score, best_match

## Analysis 2: Applying only normalization
## Normalization: Removing accents, special characters, duplicated spaces and capitalizing the first letter
## Creating variants by removing spaces and adding 's' to the end of the word 
## Then, comparing the variants with the existing brands

def check_brand_exists2(new_brand: str, existing_brands: list[str]):
    """
    Check if a new brand already exists in the database (direct).
    Returns:
        - (True, match) if found
        - (False, None) if not found
    """
    # normalize existing brands
    normalized_existing = {normalize_word(b).replace(" ", ""): b for b in existing_brands}

    # generate variants for the new brand
    variants = {v.replace(" ", "") for v in generate_variants(new_brand)}

    # Use set intersection for fast lookup
    common = variants.intersection(normalized_existing.keys())
    if common:
        key = common.pop()
        return True, normalized_existing[key]

    return False, None


## Applying the analysis
## Accessing the brand database for validation

client = create_query_client("custom_queries")

brands_analysis_query = """
        SELECT id,brand_1,brand_2
        FROM analysis_brand;
        """
brands_analysis=client.execute_query(brands_analysis_query)

# Iterar sobre cada registro
# start = time.perf_counter()
for brand_analysis in brands_analysis:
    brand_id = brand_analysis["id"]
    brand_1 = brand_analysis["brand_1"]
    brand_2 = normalize_word(brand_analysis["brand_2"])  # normalizar apenas brand_2

    # Função que retorna score e match (True/False)
    result = compare_brands(brand_1, brand_2)
    score = result[0]
    match = result[1]

    print(f"{brand_1} vs {brand_2} -> score: {score}, match: {match}")
# end = time.perf_counter()
# print("Fuzzy time:", end - start)

# start = time.perf_counter()
# for brand_analysis in brands_analysis:
#     brand_id = brand_analysis["id"]
#     brand_1 = brand_analysis["brand_1"]
#     brand_2 = normalize_word(brand_analysis["brand_2"])  # normalizar apenas brand_2

    result2 = check_brand_exists2(brand_1, [brand_2])
    match2 = result2[0]
    print(f"{brand_1} vs {brand_2} -> score: {score2}, match: {match2}")
# end = time.perf_counter()
# print("Normalization time:", end - start)

    #Atualizar a tabela com o resultado
    update_analysis_brand_query = """
        UPDATE analysis_brand
        SET fuzzy_score = %s, fuzzy_match = %s, norm_match = %s
        WHERE id = %s;
    """
    client.execute_non_query(update_analysis_brand_query, (score,match,match2,brand_id,))

## Validation for the analysis
# Load data from database (example using psycopg2 or sqlalchemy connection)
query = "SELECT id, brand_1, brand_2, is_same_brand, fuzzy_score, norm_match FROM analysis_brand;"
df = client.execute_query(query)

# Convert to DataFrame
df = pd.DataFrame(df)

# Function to evaluate fuzzy matching performance given a threshold
def evaluate_fuzzy(df, threshold):
    # Generate fuzzy predictions based on the threshold
    df["pred_fuzzy"] = df["fuzzy_score"] >= threshold
    
    # Compute confusion matrix values
    tn, fp, fn, tp = confusion_matrix(df["is_same_brand"], df["pred_fuzzy"]).ravel()
    
    # Compute metrics
    precision = precision_score(df["is_same_brand"], df["pred_fuzzy"])
    recall = recall_score(df["is_same_brand"], df["pred_fuzzy"])
    f1 = f1_score(df["is_same_brand"], df["pred_fuzzy"])
    
    return {
        "threshold": threshold,
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1
    }

# Test thresholds from 70 to 95
results = [evaluate_fuzzy(df, t) for t in range(70, 96)]
df_results = pd.DataFrame(results)

# Find the best threshold (criteria: minimize FP, then maximize F1)
best = df_results.sort_values(by=["FP", "f1"], ascending=[True, False]).iloc[0]

print("Best fuzzy threshold:")
print(best)

# ---- Comparison with norm_match ----
df["pred_norm"] = df["norm_match"]

tn, fp, fn, tp = confusion_matrix(df["is_same_brand"], df["pred_norm"]).ravel()

print("\nConfusion matrix for norm_match:")
print(f"TP: {tp}, FP: {fp}, FN: {fn}, TN: {tn}")