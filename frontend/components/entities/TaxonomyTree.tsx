"use client";

import { useState } from "react";
import { TaxonomyNode } from "@/types";
import { encodeSpace } from "@/utils/utils";
import Button from "@/components/basic/Button";
import Modal from "@/components/basic/Modal";
import { useReportRows } from "@/context/reportModeContext";

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
  entitySlug,
}: {
  node: TaxonomyNode;
  entityId: string;
  entityType: string;
  colorClass: string;
  entitySlug?: string;
}) {
  const { getRowProps } = useReportRows();
  const isEntity = node.id === entityId;
  // Only claim-nodes (ancestors + siblings that got assigned to this
  // subtree) are reportable — the entity node itself is the page's own
  // identity and isn't a taxonomy claim to flag.
  const reportProps = isEntity
    ? {}
    : getRowProps({
        kind: "metadata-item",
        entityType: entityType as
          | "food"
          | "chemical"
          | "bioactivity"
          | "disease",
        entitySlug,
        field: "taxonomy_node",
        label: "Taxonomy node",
        value: node.name,
        source: node.id,
      });
  const reportClass =
    (reportProps as { className?: string }).className ?? "";
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
        {...reportProps}
        href={`/${entityType}/${encodeURIComponent(encodeSpace(node.name))}`}
        className={`text-light-300 capitalize underline decoration-1 underline-offset-4 hover:text-light-100 transition duration-300 ${reportClass}`.trim()}
      >
        {node.name}
      </a>
    );
  }
  return (
    <span
      {...reportProps}
      className={`text-light-500 capitalize ${reportClass}`.trim()}
    >
      {node.name}
    </span>
  );
}

function TreeBranch({
  tree,
  entityId,
  entityType,
  colorClass,
  depth,
  isLast,
  entitySlug,
}: {
  tree: TreeNode;
  entityId: string;
  entityType: string;
  colorClass: string;
  depth: number;
  isLast: boolean;
  entitySlug?: string;
}) {
  return (
    <div
      className={
        depth > 0
          ? `ml-1.5 border-l pl-2 ${
              isLast ? "border-transparent" : "border-light-50/25"
            }`
          : ""
      }
    >
      <div
        className={`relative py-0.5 text-sm ${
          depth > 0
            ? `before:content-[''] before:absolute before:top-0 before:left-[-9px] before:w-[9px] before:h-[50%] before:border-b before:border-light-50/25 ${
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
          entitySlug={entitySlug}
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
          entitySlug={entitySlug}
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
  // Entity's common_name — surfaced in the taxonomy-node ReportContext
  // so ops can identify which page a taxonomy report is about.
  entitySlug?: string;
}

const TaxonomyTree = ({
  trees,
  entityId,
  entityType,
  colorClass,
  entitySlug,
}: TaxonomyTreeProps) => {
  const totalPaths = trees.reduce((sum, t) => sum + countPaths(t), 0);
  const collapsible = totalPaths > 1;
  const [isModalOpen, setIsModalOpen] = useState(false);

  // Sidebar always shows just the primary path (the leftmost root-to-leaf
  // chain); the full DAG opens in a modal so the sidebar stays narrow.
  const previewTrees = collapsible ? [primaryPath(trees[0])] : trees;

  return (
    <div>
      <div>
        {previewTrees.map((tree, i) => (
          <TreeBranch
            key={`root-${i}`}
            tree={tree}
            entityId={entityId}
            entityType={entityType}
            colorClass={colorClass}
            depth={0}
            isLast={i === previewTrees.length - 1}
            entitySlug={entitySlug}
          />
        ))}
      </div>
      {collapsible && (
        <div className="mt-2 flex items-center gap-2 flex-wrap">
          <span className="font-mono italic text-[11px] text-light-500">
            1 of {totalPaths} branches
          </span>
          <Button
            variant="outlined"
            size="xs"
            className="rounded-full"
            onClick={() => setIsModalOpen(true)}
          >
            Show full tree
          </Button>
        </div>
      )}
      {collapsible && (
        <Modal
          title="Taxonomy"
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
        >
          <div className="mt-2 max-h-[70vh] overflow-y-auto pr-2">
            {trees.map((tree, i) => (
              <TreeBranch
                key={`full-root-${i}`}
                tree={tree}
                entityId={entityId}
                entityType={entityType}
                colorClass={colorClass}
                depth={0}
                isLast={i === trees.length - 1}
                entitySlug={entitySlug}
              />
            ))}
          </div>
        </Modal>
      )}
    </div>
  );
};

TaxonomyTree.displayName = "TaxonomyTree";

export type { TreeNode };
export default TaxonomyTree;
