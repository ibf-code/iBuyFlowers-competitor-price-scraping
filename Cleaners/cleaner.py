import re
from datetime import datetime

def clean_product_name(name, strategy="safe"):
    if not name:
        return None

    name = name.strip()
    name = re.sub(r'\s+', ' ', name)

    if strategy == "safe":
        return name

    name = re.sub(r'SpRose\s*', '', name, flags=re.IGNORECASE)

    replacements = {
        r'\bWht\b': 'White',
        r'\bAsst\b': 'Assorted',
        r'\bPk\b': 'Pink',
        r'\bDbl\b': 'Double',
        r'\bLav\b': 'Lavender',
        r'\bOrg\b': 'Orange',
        r'\bYel\b': 'Yellow',
        r'\bYlw\b': 'Yellow',
        r'\bBlue-Lt\b': 'Light Blue',
        r'\bBlue-Dk\b': 'Dark Blue',
        r'\bBlue Dark\b': 'Dark Blue',
        r'\bBfly\b': 'Butterfly',
        r'\bRd\b': 'Red',
        r'\bGrn\b': 'Green',
        r'\bGr\b': 'Green',
        r'\bpch\b': 'Peach',
        r'\bprp\b': 'Purple',
        r'\bBiClr\b': 'Bicolor',
        r'\bPnky\b': 'Pinky',
        r'\bDeep Ppl\b': 'Deep Purple',
        r'\bBlnca\b': 'Blanca',
        r'\bprl\b': 'Pearl',
        r'\bSnoflk\b': 'Snowflake',
        r'\bHPk\b': 'Hot Pink',
        r'\bCrm\b': 'Cream',
        r'\bPpl\b': 'Purple',
        r'\bHtpk\b': 'Hot Pink',
        r'\bSwt\b': 'Sweet',
        r'\bVin Choic\b': 'Vincent Choice',
        r'\bFCY\b': 'Fancy',
        r'\bsel\b': 'Select',
        r'\bPink Lt\b': 'Light Pink',
    }

    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)

    name = re.sub(r'\s+\d+[\s/\-]*\d*\s*(?:mm|cm|in|inch|inches|c)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+\d+\s*(?:pk|p|cs|ct|ea|bx|bunch|bunches|stems?|st)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+(?:pk|p|cs|ct|ea|bx|bunch|bunches|stems?|st)\d+\b|\b\d+x\b|\+\d+\b|\bx\d+\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\b(?:EC|CR|CL|VA|Mex)\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+\d+\s*$', '', name)

    return ' '.join(name.split())


def extract_stem_length(raw_product_name):
    if not raw_product_name:
        return None

    # Try to find patterns with units first (e.g., "70/80cm", "70-80cm")
    stem_length_match = re.search(r'(\d+)(?:[\s/\-]\d+)?\s*(?:cm|mm|m|in|inch|inches|c)\b', raw_product_name, flags=re.IGNORECASE)
    if stem_length_match:
        # Extract just the first number in the range
        return int(stem_length_match.group(1))
        
    # Try to find patterns without units (e.g., "70/80", "70-80")
    stem_length_match = re.search(r'(\d+)(?:[\s/\-]\d+)?(?!\w)', raw_product_name, flags=re.IGNORECASE)
    if stem_length_match and 20 <= int(stem_length_match.group(1)) <= 120:  # Reasonable stem length range
        return int(stem_length_match.group(1))
        
    # Finally, check for merged patterns like "7080"
    merged_match = re.search(r'\b(\d{2})(\d{2})\b', raw_product_name)
    if merged_match and 20 <= int(merged_match.group(1)) <= 120:
        return int(merged_match.group(1))  # First two digits

    return None


def extract_stems_per_unit(product_name):
    if not product_name:
        return 1

    patterns = [
        r'(\d+)\s*(?:st|stem|stems)[/ ]?(?:bu|bunch)?',
        r'[xX](\d+)',
        r'(\d+)[/](\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, product_name, re.IGNORECASE)
        if match:
            try:
                if len(match.groups()) == 2:
                    return int(match.group(1))  # Lower bound of range
                else:
                    return int(match.group(1))
            except ValueError:
                continue

    return 1