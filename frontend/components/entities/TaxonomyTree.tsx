"use client";

import { useState } from "react";
import { TaxonomyNode } from "@/types";
import { encodeSpace } from "@/utils/utils";
import Button from "@/components/basic/Button";

type TreeNode = {
  node: TaxonomyNode;
  children: TreeNode[];
};

function countPaths(tree: TreeNode): number {
  if (tree.children.length === 0) return 1;
  return tree.children.reduce((sum, c) => sum + countPaths(c), 0);
}

/** Extract the leftmost root-to-leaf path as a linear chain. */
function primaryPath(tree: TreeNode): TreeNode {
  if (tree.children.length === 0) return { node: tree.node, children: [] };
  return {
    node: tree.node,
    children: [primaryPath(tree.children[0])],
  };
}

function NodeLabel({
  node,
  entityId,
  entityType,
  colorClass,
}: {
  node: TaxonomyNode;
  entityId: string;
  entityType: string;
  colorClass: string;
}) {
  const isEntity = node.id === entityId;
  if (isEntity) {
    return (
      <span className={`font-medium ${colorClass} capitalize`}>
        {node.name}
      </span>
    );
  }
  if (node.has_page) {
    return (
      <a
        href={`/${entityType}/${encodeURIComponent(encodeSpace(node.name))}`}
        className="text-light-300 capitalize underline decoration-1 underline-offset-4 hover:text-light-100 transition duration-300"
      >
        {node.name}
      </a>
    );
  }
  return <span className="text-light-500 capitalize">{node.name}</span>;
}

function TreeBranch({
  tree,
  entityId,
  entityType,
  colorClass,
  depth,
  isLast,
}: {
  tree: TreeNode;
  entityId: string;
  entityType: string;
  colorClass: string;
  depth: number;
  isLast: boolean;
}) {
  return (
    <div
      className={
        depth > 0
          ? `ml-4 border-l pl-3 ${
              isLast ? "border-transparent" : "border-light-50/25"
            }`
          : ""
      }
    >
      <div
        className={`relative py-0.5 text-sm ${
          depth > 0
            ? `before:content-[''] before:absolute before:top-0 before:left-[-13px] before:w-[13px] before:h-[50%] before:border-b before:border-light-50/25 ${
                isLast ? "before:border-l" : ""
              }`
            : ""
        }`}
      >
        <NodeLabel
          node={tree.node}
          entityId={entityId}
          entityType={entityType}
          colorClass={colorClass}
        />
      </div>
      {tree.children.map((child, i) => (
        <TreeBranch
          key={`${child.node.id}-${i}`}
          tree={child}
          entityId={entityId}
          entityType={entityType}
          colorClass={colorClass}
          depth={depth + 1}
          isLast={i === tree.children.length - 1}
        />
      ))}
    </div>
  );
}

interface TaxonomyTreeProps {
  trees: TreeNode[];
  entityId: string;
  entityType: string;
  colorClass: string;
}

const TaxonomyTree = ({
  trees,
  entityId,
  entityType,
  colorClass,
}: TaxonomyTreeProps) => {
  const totalPaths = trees.reduce((sum, t) => sum + countPaths(t), 0);
  const collapsible = totalPaths > 1;
  const [expanded, setExpanded] = useState(false);

  const displayTrees =
    expanded || !collapsible ? trees : [primaryPath(trees[0])];

  return (
    <div>
      <div>
        {displayTrees.map((tree, i) => (
          <TreeBranch
            key={`root-${i}`}
            tree={tree}
            entityId={entityId}
            entityType={entityType}
            colorClass={colorClass}
            depth={0}
            isLast={i === displayTrees.length - 1}
          />
        ))}
      </div>
      {collapsible && (
        <div className="mt-2">
          <Button
            variant="outlined"
            size="xs"
            className="rounded-full"
            onClick={() => setExpanded(!expanded)}
          >
            {expanded
              ? "Collapse"
              : `+ ${totalPaths - 1} more path${totalPaths - 1 > 1 ? "s" : ""}`}
          </Button>
        </div>
      )}
    </div>
  );
};

TaxonomyTree.displayName = "TaxonomyTree";

export type { TreeNode };
export default TaxonomyTree;
