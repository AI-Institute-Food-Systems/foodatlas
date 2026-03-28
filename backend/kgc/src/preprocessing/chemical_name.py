"""Chemical name standardization."""

from .constants import GREEK_LETTERS, PUNCTUATIONS


def standardize_chemical_name(chemical_name: str) -> str:
    """Standardize a chemical name by replacing Greek letters and punctuation.

    Replaces Unicode Greek letter variants with their English equivalents
    and normalizes punctuation characters to ASCII forms.

    Args:
        chemical_name: The raw chemical name string.

    Returns:
        The standardized chemical name.

    """
    for eng, greek_letters in GREEK_LETTERS.items():
        for greek_letter in greek_letters:
            chemical_name = chemical_name.replace(greek_letter, eng)

    for punc_new, punc_old_list in PUNCTUATIONS.items():
        for punc_old in punc_old_list:
            chemical_name = chemical_name.replace(punc_old, punc_new)

    return chemical_name
