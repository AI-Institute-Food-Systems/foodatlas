import type { ReactNode } from "react";

import { requireEntity } from "@/components/entities/requireEntity";

// Existence check above loading.tsx, so an unknown slug is a real 404.
const DiseaseEntityLayout = async ({
  children,
  params,
}: {
  children: ReactNode;
  params: { slug: string };
}) => {
  await requireEntity("disease", params.slug);
  return <>{children}</>;
};

DiseaseEntityLayout.displayName = "DiseaseEntityLayout";

export default DiseaseEntityLayout;
