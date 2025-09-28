import re
import unicodedata

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