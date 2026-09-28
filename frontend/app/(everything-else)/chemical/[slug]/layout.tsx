import type { ReactNode } from "react";

import { requireEntity } from "@/components/entities/requireEntity";

// Existence check above loading.tsx, so an unknown slug is a real 404.
const ChemicalEntityLayout = async ({
  children,
  params,
}: {
  children: ReactNode;
  params: { slug: string };
}) => {
  await requireEntity("chemical", params.slug);
  return <>{children}</>;
};

ChemicalEntityLayout.displayName = "ChemicalEntityLayout";

export default ChemicalEntityLayout;
