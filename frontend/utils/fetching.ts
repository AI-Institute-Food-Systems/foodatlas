import { MacroAndMicroData, Metadata, TaxonomyData } from "@/types";

// fetch metadata for a given entity
export async function getMetaData(
  commonName: string,
  entityType: string
): Promise<Metadata> {
  const res = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/${entityType}/metadata?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!res.ok) {
    throw new Error(`Failed to fetch metadata for ${entityType} ${commonName}`);
  }

  const data = await res.json();

  return data.data[0];
}

// fetch taxonomy ancestry for a given entity
export async function getTaxonomyData(
  commonName: string,
  entityType: string
): Promise<TaxonomyData> {
  const res = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/${entityType}/taxonomy?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!res.ok) {
    throw new Error(
      `Failed to fetch taxonomy for ${entityType} ${commonName}`
    );
  }

  const data = await res.json();

  return data.data;
}

// fetch food macro & micro data
export async function getFoodMacroAndMicroData(
  commonName: string
): Promise<MacroAndMicroData> {
  const response = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/food/profile?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed to fetch macro and micro data for food ${commonName}`
    );
  }

  const { data } = await response.json();

  return data;
}

// fetch food composition data, i.e. its chemical composition
export async function getFoodCompositionData(
  commonName: string,
  currentPage: number,
  sourceFilters: string[],
  searchTerm: string,
  sort: { column: string; direction: string },
  showAllConcentrations: boolean,
  classificationFilters: string[] = []
) {
  const clsParam =
    classificationFilters.length > 0
      ? `&filter_classification=${classificationFilters.map(encodeURIComponent).join("%2B")}`
      : "";
  const response = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/food/composition?common_name=${encodeURIComponent(
      commonName
    )}&page=${currentPage}&filter_source=${sourceFilters.join(
      "%2B"
    )}&search=${encodeURIComponent(searchTerm)}&sort_by=${
      sort.column
    }&sort_dir=${sort.direction}&show_all_rows=${showAllConcentrations}${clsParam}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to fetch composition data for food ${commonName}`);
  }

  const data = await response.json();

  return data;
}

// fetch food composition counts (classification + source counts in one call)
export async function getFoodCompositionCounts(commonName: string) {
  const response = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/food/composition/counts?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed to fetch composition counts for food ${commonName}`
    );
  }

  const data = await response.json();
  return data.data as {
    classification_counts: Record<string, number>;
    source_counts: Record<string, number>;
  };
}

// fetch chemical composition data, i.e. the foods containing it
export async function getChemicalCompositionData(commonName: string) {
  const res = await fetch(
    `${
      process.env.NEXT_PUBLIC_API_URL
    }/chemical/composition?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!res.ok) {
    throw new Error(
      `Failed to fetch composition data for chemical ${commonName}`
    );
  }

  const { data } = await res.json();

  return data;
}

// fetch disease correlation data for a certain chemical, either negative or positive
export async function getDiseaseData(
  commonName: string,
  currentPage: number,
  tableLocation: string,
  correlationType: "positive" | "negative"
) {
  const url = `${
    process.env.NEXT_PUBLIC_API_URL
  }/${tableLocation}/correlation?common_name=${encodeURIComponent(
    commonName
  )}&page=${currentPage}&relation=${correlationType}`;
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
    next: { revalidate: 86400 },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch data for ${tableLocation} ${commonName}`);
  }

  const data = await response.json();

  return data;
}

// fetch db bundle download entries
export async function getDownloadEntries() {
  const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/download`, {
    headers: {
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
    next: { revalidate: 86400 },
  });

  if (!response.ok) {
    throw new Error("Failed to fetch food composition downloads");
  }

  const data = await response.json();

  return data;
}

// cache & fetching testing function
export async function getTime() {
  const response = await fetch("https://worldtimeapi.org/api/timezone/Etc/UTC");

  const data = await response.json();

  return data.unixtime;
}
