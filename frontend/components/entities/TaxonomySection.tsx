import { Fragment } from "react";

import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import { getTaxonomyData } from "@/utils/fetching";
import { TaxonomyEdge, TaxonomyNode } from "@/types";
import { encodeSpace } from "@/utils/utils";

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
 * Build all root-to-entity paths from a flat node+edge graph.
 * Walks from the entity upward, then reverses each path.
 */
function buildPaths(
  entityId: string,
  nodes: TaxonomyNode[],
  edges: TaxonomyEdge[]
): TaxonomyNode[][] {
  const nameMap = new Map(nodes.map((n) => [n.id, n]));
  // child -> parents
  const parentMap = new Map<string, string[]>();
  for (const e of edges) {
    const existing = parentMap.get(e.child_id) ?? [];
    existing.push(e.parent_id);
    parentMap.set(e.child_id, existing);
  }

  const paths: TaxonomyNode[][] = [];
  const MAX_PATHS = 500;

  function walk(nodeId: string, path: TaxonomyNode[], visited: Set<string>) {
    if (paths.length >= MAX_PATHS) return;
    const node = nameMap.get(nodeId);
    if (!node || visited.has(nodeId)) return;
    const current = [node, ...path];
    const next = new Set(visited).add(nodeId);
    const parents = parentMap.get(nodeId);
    if (!parents || parents.length === 0) {
      paths.push(current);
    } else {
      for (const pid of parents) {
        walk(pid, current, next);
      }
    }
  }

  walk(entityId, [], new Set());
  return paths;
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

  const paths = buildPaths(data.entity_id, data.nodes, data.edges);

  if (paths.length === 0) {
    return null;
  }

  const colorClass = ENTITY_COLOR[entityType] ?? "text-light-100";

  return (
    <Card>
      <Heading type="h4" className="font-mono italic text-light-400 text-xs">
        Taxonomy
      </Heading>
      <div className="mt-3 flex flex-col gap-1.5">
        {paths.map((path, i) => (
          <div
            key={i}
            className="flex flex-wrap items-center gap-1 text-sm leading-relaxed"
          >
            {path.map((node, j) => {
              const isEntity = node.id === data.entity_id;
              return (
                <Fragment key={`${node.id}-${j}`}>
                  {j > 0 && (
                    <span className="text-light-400 mx-1 text-base font-bold">&#8594;</span>
                  )}
                  {isEntity ? (
                    <span className={`font-medium ${colorClass} capitalize`}>
                      {node.name}
                    </span>
                  ) : node.has_page ? (
                    <a
                      href={`/${entityType}/${encodeURIComponent(encodeSpace(node.name))}`}
                      className="text-light-300 capitalize underline decoration-1 underline-offset-4 hover:text-light-100 transition duration-300"
                    >
                      {node.name}
                    </a>
                  ) : (
                    <span className="text-light-500 capitalize">
                      {node.name}
                    </span>
                  )}
                </Fragment>
              );
            })}
          </div>
        ))}
      </div>
    </Card>
  );
};

TaxonomySection.displayName = "TaxonomySection";

export default TaxonomySection;
