"""Name-standardization constants ported from FoodAtlas-KGv2 preprocessing."""

# Greek Unicode variants → English name.
# Reference: https://en.wikipedia.org/wiki/Greek_script_in_Unicode
GREEK_LETTERS: dict[str, str] = {
    "alpha": ("α𝛂𝛼𝜶𝝰𝞪Α𝚨𝛢𝜜𝝖𝞐ἀἈἄἌᾄᾌἂἊᾂᾊἆἎᾆᾎᾀᾈἁἉἅἍᾅᾍἃἋᾃᾋἇἏᾇᾏᾁᾉάάΆΆᾴὰᾺᾲᾰᾸᾶᾷᾱᾹᾳᾼ⍺ɑ"),
    "beta": "βϐ𝛃𝛽𝜷𝝱𝞫Β𝚩𝛣𝜝𝝗𝞑ᵝᵦßꞵ",
    "gamma": "γℽ𝛄𝛾𝜸𝝲𝞬Γℾ𝚪𝛤𝜞𝝘𝞒ᵞᵧᴦɣ",
    "delta": "δ𝛅𝛿𝜹𝝳𝞭Δ𝚫𝛥𝜟𝝙𝞓ᵟ∆",
    "epsilon": ("εϵ𝛆𝛜𝜀𝜖𝜺𝝐𝝴𝞊𝞮𝟄Ε𝚬𝛦𝜠𝝚𝞔ἐἘἔἜἒἚἑἙἕἝἓἛέέΈΈὲῈɛ"),
    "zeta": "ζ𝛇𝜁𝜻𝝵𝞯Ζ𝚭𝛧𝜡𝝛𝞕",
    "eta": ("η𝛈𝜂𝜼𝝶𝞰Η𝚮𝛨𝜢𝝜𝞖ἠἨἤἬᾔᾜἢἪᾒᾚἦἮᾖᾞᾐᾘἡἩἥἭᾕᾝἣἫᾓᾛἧἯᾗᾟᾑᾙήήΉΉῄὴῊῂῆῇῃῌ"),
    "theta": "θϑ𝛉𝛝𝜃𝜗𝜽𝝑𝝷𝞋𝞱𝟅Θϴ𝚯𝚹𝛩𝛳𝜣𝜭𝝝𝝧𝞗𝞡ᶿ",
    "omega": ("ω𝛚𝜔𝝎𝞈𝟂ΩΩꭥ𝛀𝛺𝜴𝝮𝞨ὠὨὤὬᾤᾬὢὪᾢᾪὦὮᾦᾮᾠᾨὡὩὥὭᾥᾭὣὫᾣᾫὧὯᾧᾯᾡᾩώώΏΏῴὼῺῲῶῷῳῼꭥ"),
    "kappa": "κ",
    "lambda": "λ",
    "iota": "ι",
    "sigma": "∑σ",
    "tao": "τ",
    "mu": "μ",
    "nu": "ν",
    "xi": "ξ",
    "omicron": "ο",
    "pi": "π",
    "rho": "ρ",
    "upsilon": "υ",
    "phi": "φ",
    "chi": "χ",
    "psi": "ψ",
}

# Unicode punctuation variants → ASCII canonical form.
# Reference: https://www.compart.com/en/unicode/category/Pd
PUNCTUATIONS: dict[str, str] = {
    "": "\u00ad\u00a0\u2009",  # soft hyphen, NBSP, thin space
    "-": "\u2010\u2212\u2013\u2011\u2014\u2012",  # various hyphens/dashes
    '"': "\u201c\u201d\u2033",  # smart double quotes, double prime
    "'": "\u2018\u2019\u2032\u02b9\u02cb",  # smart single quotes, primes
    "->": "\u2192",  # right arrow
}
