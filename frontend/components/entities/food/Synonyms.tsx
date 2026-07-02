"use client";

import { useState } from "react";

import Card from "@/components/basic/Card";
import Button from "@/components/basic/Button";
import Modal from "@/components/basic/Modal";

const SYNONYM_LENGTH_LIMIT = 10;

interface SynonymsModalProps {
  synonyms: string[];
  // "naked" drops the Card wrapper + heading and renders just the body —
  // for variants that supply their own section labels (Card Catalog, Index Spine).
  naked?: boolean;
  // "inline" is naked but prefixed with serif italic "Also known as " — for
  // the Apothecary variant which has no section headings.
  inline?: boolean;
}

const SynonymsModal = ({
  synonyms,
  naked = false,
  inline = false,
}: SynonymsModalProps) => {
  const [isSynonymModalOpen, setIsSynonymModalOpen] = useState(false);

  const handleSynonymShowAllClick = () => {
    setIsSynonymModalOpen(true);
  };

  const handleSynonymModalClose = () => {
    setIsSynonymModalOpen(false);
  };

  const previewList = synonyms.slice(0, SYNONYM_LENGTH_LIMIT);
  const preview = previewList.join("; ");
  const remaining = synonyms.length - SYNONYM_LENGTH_LIMIT;
  const hasMore = remaining > 0;

  const showAllPill = hasMore && (
    <button
      type="button"
      onClick={handleSynonymShowAllClick}
      className="font-serif italic text-light-400 hover:text-light-200 underline-offset-2 hover:underline transition-colors"
    >
      ({remaining} more)
    </button>
  );

  let body: JSX.Element;
  if (inline) {
    body = (
      <p className="text-sm text-light-300 leading-relaxed">
        <em className="font-serif text-light-400 not-italic"> </em>
        <span className="font-serif italic text-light-400">
          Also known as{" "}
        </span>
        <span className="capitalize text-light-200">{preview}</span>
        {hasMore && <> … {showAllPill}</>}
      </p>
    );
  } else if (naked) {
    // Chip-style synonyms: flex-wrap so long names break onto new rows
    // instead of forcing the sidebar to scroll horizontally; `break-all`
    // keeps unspaced chemistry strings (e.g. 1,5-anhydro-…) wrapping inside
    // a single chip. Low vertical padding keeps the row svelte.
    body = (
      <div className="flex flex-wrap gap-1 min-w-0">
        {previewList.map((name, i) => (
          <span
            key={`${name}-${i}`}
            className="capitalize text-xs leading-tight px-2 py-0.5 rounded-full border border-light-700/70 bg-light-900/40 text-light-200 break-all max-w-full"
          >
            {name}
          </span>
        ))}
        {hasMore && (
          <button
            type="button"
            onClick={handleSynonymShowAllClick}
            className="text-xs leading-tight px-2 py-0.5 rounded-full border border-dashed border-light-600 text-light-400 hover:text-light-100 hover:border-light-400 transition-colors"
          >
            +{remaining} more
          </button>
        )}
      </div>
    );
  } else {
    body = (
      <Card>
        <h4 className="font-mono italic text-light-400 text-xs">Synonyms</h4>
        <div className="mt-3">
          <span className="capitalize">{preview}</span>
          {hasMore && ` ...  ${remaining} more`}
        </div>
        {hasMore && (
          <div className="absolute right-3 top-3.5">
            <Button
              className="rounded-full"
              variant="outlined"
              size="xs"
              onClick={handleSynonymShowAllClick}
            >
              + Show all
            </Button>
          </div>
        )}
      </Card>
    );
  }

  return (
    <>
      {body}
      <Modal
        title="Synonyms"
        isOpen={isSynonymModalOpen}
        onClose={handleSynonymModalClose}
      >
        <div className="capitalize leading-relaxed">{synonyms.join("; ")}</div>
      </Modal>
    </>
  );
};

SynonymsModal.displayName = "Synonyms";

export default SynonymsModal;
