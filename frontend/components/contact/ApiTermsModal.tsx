"use client";

import { useEffect, useState } from "react";

import Button from "@/components/basic/Button";
import CopyCitation from "@/components/basic/CopyCitation";
import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { CANONICAL_PUBLICATION } from "@/utils/publications";

interface ApiTermsModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAccept: () => void;
}

const LICENSE_URL = "https://creativecommons.org/licenses/by-nc/4.0/";

const ApiTermsModal = ({ isOpen, onClose, onAccept }: ApiTermsModalProps) => {
  const [agreed, setAgreed] = useState(false);

  // Every opening asks again; a past tick doesn't carry over.
  useEffect(() => {
    if (isOpen) setAgreed(false);
  }, [isOpen]);

  return (
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
          <Button
            variant="filled"
            type="button"
            isDisabled={!agreed}
            onClick={onAccept}
          >
            Send request
          </Button>
        </div>
      }
    >
      <ul className="max-w-prose list-disc space-y-3 pl-5 text-base font-light text-light-300">
        <li>
          FoodAtlas data is licensed under the Creative Commons
          Attribution-NonCommercial 4.0 International License (
          <Link href={LICENSE_URL}>CC BY-NC 4.0</Link>). This covers data from
          the API and the downloadable bundles.
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
          data.
        </li>
      </ul>
      <div className="mt-4 max-w-prose">
        <CopyCitation publication={CANONICAL_PUBLICATION} boxed />
      </div>
      <label className="mt-6 flex max-w-prose cursor-pointer items-start gap-3 text-base text-light-100">
        <input
          type="checkbox"
          className="mt-1 accent-accent-500"
          checked={agreed}
          onChange={(e) => setAgreed(e.target.checked)}
        />
        I have read these terms and agree to them.
      </label>
    </Modal>
  );
};

ApiTermsModal.displayName = "ApiTermsModal";
export default ApiTermsModal;
