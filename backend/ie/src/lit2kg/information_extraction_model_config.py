"""Configuration for information extraction model prompts."""

SYSTEM_PROMPT: str = "You are an expert in food science and chemistry. "

MAX_NEW_TOKENS: int = 512
TEMPERATURE: float = 0.0

PROMPT_TEMPLATE: str = (
    "Given a sentence, extract the following in the format: "
    "food, food part, chemical, chemical concentration.\n"
    "Food and chemical must exist to be a valid entry.\n"
    "Food part and chemical concentration can be left empty if not found.\n"
    "Do not return anything if none found.\n"
    "Oil should be included in the food, not food part.\n"
    "\n"
    "Sentence: 'Total phenols and flavonoids in the olive leaf extract "
    "were 169.10 +/- 0.57 mg/g and 98.15 +/- 0.7 mg/g, respectively.'\n"
    "\n"
    "olive, leaf, phenols, 169.10 +/- 0.57 mg/g\n"
    "olive, leaf, flavonoids, 98.15 +/- 0.7 mg/g\n"
    "\n"
    "Sentence: '{sentence}'"
)
