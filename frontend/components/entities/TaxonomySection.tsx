import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import TaxonomyTree from "@/components/entities/TaxonomyTree";
import { getTaxonomyData } from "@/utils/fetching";
import { TaxonomyEdge, TaxonomyNode } from "@/types";
import type { TreeNode } from "@/components/entities/TaxonomyTree";

interface TaxonomySectionProps {
  commonName: string;
  entityType: string;
}

const ENTITY_COLOR: Record<string, string> = {
  food: "text-amber-500",
  chemical: "text-cyan-500",
  disease: "text-purple-400",
};

/**
 * Build a tree from roots down to the target entity.
 * In a DAG a node may appear under multiple parents (branches).
 */
function buildTree(
  entityId: string,
  nodes: TaxonomyNode[],
  edges: TaxonomyEdge[]
): TreeNode[] {
  const nameMap = new Map(nodes.map((n) => [n.id, n]));

  const childrenOf = new Map<string, string[]>();
  const hasParent = new Set<string>();
  for (const e of edges) {
    const existing = childrenOf.get(e.parent_id) ?? [];
    existing.push(e.child_id);
    childrenOf.set(e.parent_id, existing);
    hasParent.add(e.child_id);
  }

  const allIds = new Set(nodes.map((n) => n.id));
  const roots = Array.from(allIds).filter((id) => !hasParent.has(id));

  const reachable = new Set<string>();
  function markReachable(nodeId: string): boolean {
    if (nodeId === entityId) {
      reachable.add(nodeId);
      return true;
    }
    const kids = childrenOf.get(nodeId) ?? [];
    let reaches = false;
    for (const kid of kids) {
      if (markReachable(kid)) reaches = true;
    }
    if (reaches) reachable.add(nodeId);
    return reaches;
  }
  for (const r of roots) markReachable(r);

  function build(nodeId: string, visited: Set<string>): TreeNode | null {
    const node = nameMap.get(nodeId);
    if (!node || visited.has(nodeId)) return null;
    const next = new Set(visited).add(nodeId);
    const kids = (childrenOf.get(nodeId) ?? [])
      .filter((kid) => reachable.has(kid))
      .map((kid) => build(kid, next))
      .filter((t): t is TreeNode => t !== null);
    return { node, children: kids };
  }

  return roots
    .filter((r) => reachable.has(r))
    .map((r) => build(r, new Set()))
    .filter((t): t is TreeNode => t !== null);
}

const TaxonomySection = async ({
  commonName,
  entityType,
}: TaxonomySectionProps) => {
  let data;
  try {
    data = await getTaxonomyData(commonName, entityType);
  } catch {
    return null;
  }

  if (!data.entity_id || data.edges.length === 0) {
    return null;
  }

  const trees = buildTree(data.entity_id, data.nodes, data.edges);
  if (trees.length === 0) return null;

  const colorClass = ENTITY_COLOR[entityType] ?? "text-light-100";

  return (
    <Card>
      <Heading type="h4" className="font-mono italic text-light-400 text-xs">
        Taxonomy
      </Heading>
      <div className="mt-3">
        <TaxonomyTree
          trees={trees}
          entityId={data.entity_id}
          entityType={entityType}
          colorClass={colorClass}
        />
      </div>
    </Card>
  );
};

TaxonomySection.displayName = "TaxonomySection";

export default TaxonomySection;
