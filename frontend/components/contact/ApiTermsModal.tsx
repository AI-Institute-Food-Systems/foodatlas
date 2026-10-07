"use client";

import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { CANONICAL_PUBLICATION, doiUrl } from "@/utils/publications";

interface ApiTermsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAccept: () => void;
}

const LICENSE_URL = "https://creativecommons.org/licenses/by-nc/4.0/";

const ApiTermsModal = ({ isOpen, onClose, onAccept }: ApiTermsModalProps) => (
  <Modal
    isOpen={isOpen}
    onClose={onClose}
    title="API terms of use"
    panelClassName="max-w-2xl"
    description="To request a key, agree to these terms."
    footer={
      <div className="flex justify-end gap-3">
        <Button variant="outlined" type="button" onClick={onClose}>
          Cancel
        </Button>
        <Button variant="filled" type="button" onClick={onAccept}>
          I agree — send request
        </Button>
      </div>
    }
  >
    <ul className="max-w-prose list-disc space-y-3 pl-5 text-base font-light text-light-300">
      <li>
        FoodAtlas data is licensed under the{" "}
        <Link href={LICENSE_URL}>
          Creative Commons Attribution-NonCommercial 4.0 International License
          (CC BY-NC 4.0)
        </Link>
        . This covers data from the API and the downloadable bundles.
      </li>
      <li>
        <strong className="font-medium text-light-100">
          Non-commercial use only.
        </strong>{" "}
        You must not use the data or the API for commercial purposes. For
        commercial use, contact us first for a separate agreement.
      </li>
      <li>
        Give credit: cite <i>FoodAtlas</i> in any published work that uses the
        data:{" "}
        <Link href={doiUrl(CANONICAL_PUBLICATION.doi)}>
          {CANONICAL_PUBLICATION.venue} ({CANONICAL_PUBLICATION.year})
        </Link>
        .
      </li>
    </ul>
  </Modal>
);

ApiTermsModal.displayName = "ApiTermsModal";
export default ApiTermsModal;
