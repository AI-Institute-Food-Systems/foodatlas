"use client";

import { useState } from "react";

import Card from "@/components/basic/Card";
import Button from "@/components/basic/Button";
import Heading from "@/components/basic/Heading";
import Modal from "@/components/basic/Modal";

const SYNONYM_LENGTH_LIMIT = 10;

interface SynonymsModalProps {
  synonyms: string[];
}

const SynonymsModal = ({ synonyms }: SynonymsModalProps) => {
  const [isSynonymModalOpen, setIsSynonymModalOpen] = useState(false);

  const handleSynonymShowAllClick = () => {
    setIsSynonymModalOpen(true);
  };

  const handleSynonymModalClose = () => {
    setIsSynonymModalOpen(false);
  };

  return (
    <>
      <Card>
        <Heading type="h4" className="font-mono italic text-light-400 text-xs">
          Synonyms
        </Heading>
        <div className="mt-3">
          <span className="capitalize">
            {synonyms.slice(0, SYNONYM_LENGTH_LIMIT).join("; ")}
          </span>
          {synonyms.length > SYNONYM_LENGTH_LIMIT &&
            ` ...  ${synonyms.length - SYNONYM_LENGTH_LIMIT} more`}
        </div>
        {synonyms.length >= SYNONYM_LENGTH_LIMIT && (
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
