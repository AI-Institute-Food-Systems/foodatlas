import Tooltip from "@/components/basic/Tooltip";

export const createArrayInRange = (start: number, stop: number, step: number) =>
  Array.from(
    { length: (stop - start) / step + 1 },
    (value, index) => start + index * step
  );

export function customToFixed(value: number) {
  // Convert to string if not already
  let strValue = value.toString();

  // Find the position of the first non-zero digit after the decimal
  let firstNonZeroPos = strValue.indexOf(".") + 1; // Start looking right after the decimal point
  while (strValue.charAt(firstNonZeroPos) === "0") {
    firstNonZeroPos++;
  }

  // Calculate the total number of digits to keep after the decimal
  // This is the number of leading zeros + 2 digits after the first non-zero digit
  let totalDigits = firstNonZeroPos - strValue.indexOf(".") + 2;

  // Use toFixed with the calculated totalDigits, but need to handle cases where it might exceed the actual number of decimals
  let fixedValue = parseFloat(strValue).toFixed(
    Math.min(totalDigits, strValue.length - strValue.indexOf(".") - 1)
  );

  // Remove any trailing zeros introduced by toFixed
  return fixedValue.replace(/(\.\d*?[1-9])0+$/, "$1");
}

export const SOURCE_LOOKUP = {
  fdc: "FDC",
  "lit2kg:gpt-4": "FoodAtlas",
  "lit2kg:biobert": "FoodAtlas",
  Frida: "Frida",
  MeSH: "MeSH",
  NCBI_taxonomy: "NCBI Taxonomy",
  "Phenol-Explorer": "Phenol Explorer",
  ptfi: "PTFI",
};

export const ENTITY_COLOR_LOOKUP = {
  contains: { bg: "#dcf0cb", text: "#000" },
  "has child": { bg: "#fbdc33", text: "#000" },
  "has part": { bg: "#bd6814", text: "#fff" },
  "is a": { bg: "#f9b380", text: "#000" },
  chemical: { bg: "#ff6343", text: "#fff" },
  organism: { bg: "#43a82f", text: "#fff" },
  organism_with_part: { bg: "#dfef59", text: "#000" },
};

export const EXTERNAL_REFERENCE_LOOKUP = {
  fdc_nutrient_ids: {
    displayName: "FDC Nutrient ID",
  },
  fdc_ids: {
    displayName: "FDC ID",
    url: "https://fdc.nal.usda.gov/fdc-app.html#/food-details/$ID",
  },
  ncbi_taxon_id: {
    displayName: "NCBI Taxonomy ID",
    url: "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=$ID",
  },
  pubchem_cid: {
    displayName: "PubChem ID",
    url: "https://pubchem.ncbi.nlm.nih.gov/compound/$ID",
  },
  foodb_ids: {
    displayName: "FooDB ID",
    url: "https://foodb.ca/foods/$ID",
  },
};

export const calculateProbabilityString = (citation: any[]) => {
  // extract probabilities from each citation
  const probabilities = citation.map((item) => item.probability.mean);

  // calculate max, mean, and standard deviation
  const maxMean = Math.round(Math.max(...probabilities) * 1000) / 1000;
  const meanMean =
    Math.round(
      (probabilities.reduce((acc, val) => acc + val, 0) /
        probabilities.length) *
        1000
    ) / 1000;
  const meanStd =
    Math.round(
      Math.sqrt(
        probabilities.reduce(
          (acc, val) => acc + Math.pow(val - meanMean, 2),
          0
        ) / probabilities.length
      ) * 1000
    ) / 1000;

  return `${maxMean} (${meanMean} ± ${meanStd})`;
};

export const removeExponent = (number: number): string => {
  var data = String(number).split(/[eE]/);
  if (data.length == 1) return data[0];

  var z = "",
    sign = number < 0 ? "-" : "",
    str = data[0].replace(".", ""),
    mag = Number(data[1]) + 1;

  if (mag < 0) {
    z = sign + "0.";
    while (mag++) z += "0";
    return z + str.replace(/^\-/, "");
  }
  mag -= str.length;
  while (mag--) z += "0";
  return str + z;
};

export const formatConcentrationValue = (value: number) => {
  const numberWithoutExponents = removeExponent(value);
  const postDecimalNumber = numberWithoutExponents.split(".", 2);

  // no decimal point, e.g. 4
  if (postDecimalNumber.length === 1) {
    return numberWithoutExponents;
  }
  // decimal point & non zero before, e.g. 4.02 or 4.00
  else if (postDecimalNumber[0] !== "0") {
    return `${postDecimalNumber[0]}.${postDecimalNumber[1].slice(0, 2)}`;
  }
  // decimal point & zero before, e.g. 0.004
  else {
    for (let index = 0; index < postDecimalNumber[1].length; index++) {
      const element = postDecimalNumber[1][index];
      if (element !== "0") {
        return `${postDecimalNumber[0]}.${Array(index)
          .fill(0)
          .join("")}${postDecimalNumber[1].slice(index, index + 2)}`;
      }
    }
  }

  return numberWithoutExponents;
};

// formats concentration values using scientific notation for small numbers
export const formatConcentrationValueAlt = (value: number | undefined) => {
  if (!value) return "—";

  const numValue = Number(value);

  // if value is less than 0.01, use scientific notation
  if (Math.abs(numValue) < 0.01) {
    return numValue.toExponential(2);
  }

  // otherwise format with up to 2 decimal places
  const formatted = numValue.toFixed(2);

  // remove trailing zeros after decimal
  return formatted.replace(/\.?0+$/, "");
};

// format concentration to table field
export const formatConcentration = (concentrationData: any) => {
  // if nullable or empty
  if (
    !concentrationData ||
    concentrationData.length === 0 ||
    (!concentrationData[0].value && !concentrationData[0].extracted[0])
  )
    return "—";

  // process each concentration and return an array of JSX elements
  const elements = concentrationData.flatMap(
    (concentration: any, index: number) => {
      // for the standardized case, return a string wrapped in a fragment with a key
      if (concentration.standardized) {
        return [
          <span key={index}>
            {/* add a comma separator before items, except the first */}
            {index > 0 ? ", " : ""}{" "}
            {`${formatConcentrationValueAlt(concentration.value)} ${
              concentration?.unit
            }`}
          </span>,
        ];
      } else {
        // for non-standardized, return a span element
        // make sure to handle the comma separator if you have multiple items
        return [
          index > 0 && ", ",
          <Tooltip
            key={index}
            content={
              <p>
                This concentration has not been
                <br /> standardized yet and shows the
                <br /> value as extracted from the <br />
                publication.
              </p>
            }
          >
            <span className="w-fit h-fit decoration-accent-600/50 underline underline-offset-2 decoration-wavy">
              {concentration.extracted.join(", ")}
            </span>
            {index < concentrationData.length - 1 && ", "}
          </Tooltip>,
        ];
      }
    }
  );

  return elements;
};

// @ts-ignore
export const hexToRGB = (hex, alpha) => {
  var r = parseInt(hex.slice(1, 3), 16),
    g = parseInt(hex.slice(3, 5), 16),
    b = parseInt(hex.slice(5, 7), 16);

  if (alpha) {
    return "rgba(" + r + ", " + g + ", " + b + ", " + alpha + ")";
  } else {
    return "rgb(" + r + ", " + g + ", " + b + ")";
  }
};

export function capitalizeFirstLetter(string: string) {
  return string.charAt(0).toUpperCase() + string.slice(1);
}

// credit: https://stackoverflow.com/questions/63802425/javascript-convert-string-title-case-with-hyphen-in-title
export function toTitleCase(phrase: string) {
  return phrase.toLowerCase().replace(/\b[a-z]/g, function (s) {
    return s.toUpperCase();
  });
}

// encode a space to be used in a URL
export function encodeSpace(phrase: string) {
  return phrase.replace(/ /g, "--");
}

// decode a space in a string from a URL
export function decodeSpace(phrase: string) {
  return phrase.replace(/--/g, " ");
}
