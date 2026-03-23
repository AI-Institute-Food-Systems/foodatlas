SYSTEM_PROMPT = (
    "You are an expert in food science and chemistry. "
)

MAX_NEW_TOKENS = 512
TEMPERATURE = 0.0

PROMPT_TEMPLATE ="""Given a sentence, extract the following in the format: food, food part, chemical, chemical concentration.
Food and chemical must exist to be a valid entry.
Food part and chemical concentration can be left empty if not found.
Do not return anything if none found.
Oil should be included in the food, not food part.

Sentence: 'Total phenols and flavonoids in the olive leaf extract were 169.10 ± 0.57 mg/g and 98.15 ± 0.7 mg/g, respectively.’

olive, leaf, phenols, 169.10 ± 0.57 mg/g
olive, leaf, flavonoids, 98.15 ± 0.7 mg/g

Sentence: '{sentence}'"""


# (
#     "Given a sentence, extract in CSV format the following: food, food part, chemical, and chemical concentration. Food and chemical must exist to be a valid entry. Food part and chemical concentration can be left empty if not found. Do not return anything if none found. Oil should be included in the food, not food part.",
#     "Example:\n"
#     "Sentence: 'Total phenols and flavonoids in the olive leaf extract were 169.10 ± 0.57 mg/g and 98.15 ± 0.7 mg/g, respectively.'\n",
#     "olive, leaf, phenols, 169.10 ± 0.57 mg/g\n"
#     "olive, leaf, flavonoids, 98.15 ± 0.7 mg/g\n"
#     "Sentence: '{sentence}'\n"
# )
