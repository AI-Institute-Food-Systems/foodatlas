import type { ReactNode } from "react";

import { requireEntity } from "@/components/entities/requireEntity";

// Existence check above loading.tsx, so an unknown slug is a real 404.
const BioactivityEntityLayout = async ({
  children,
  params,
}: {
  children: ReactNode;
  params: { slug: string };
}) => {
  await requireEntity("bioactivity", params.slug);
  return <>{children}</>;
};

BioactivityEntityLayout.displayName = "BioactivityEntityLayout";

export default BioactivityEntityLayout;
