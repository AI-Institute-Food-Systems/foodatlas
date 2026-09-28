import type { ReactNode } from "react";

import { requireEntity } from "@/components/entities/requireEntity";

// Existence check above loading.tsx, so an unknown slug is a real 404.
const FoodEntityLayout = async ({
  children,
  params,
}: {
  children: ReactNode;
  params: { slug: string };
}) => {
  await requireEntity("food", params.slug);
  return <>{children}</>;
};

FoodEntityLayout.displayName = "FoodEntityLayout";

export default FoodEntityLayout;
