// modal wrapper for headless ui dialog

import { Dialog, DialogPanel } from "@headlessui/react";
import { MdClose } from "react-icons/md";

import Heading from "@/components/basic/Heading";
import Button from "@/components/basic/Button";

interface ModalProps {
  children: React.ReactNode;
  title: string;
  description?: React.ReactNode;
  isOpen: boolean;
  onClose: () => void;
}

const Modal = ({
  isOpen,
  onClose,
  children,
  title,
  description,
}: ModalProps) => {
  // Headless UI Dialog handles scroll locking internally.
  // scrollbar-gutter: stable on <html> (globals.css) reserves scrollbar
  // space, preventing layout shift when the scrollbar is hidden.

  return (
    <Dialog
      as="div"
      className="relative z-50 focus:outline-none"
      open={isOpen}
      onClose={onClose}
    >
      {/* backdrop   */}
      <div className="fixed inset-0 w-screen backdrop-blur-md bg-neutral-800/50" />
      {/* modal */}
      <div className="fixed inset-0 overflow-y-auto md:p-12">
        {/* center content */}
        <div className="flex min-h-full items-center justify-center">
          <DialogPanel className="w-full max-w-5xl md:rounded-xl border border-light-50/5 bg-light-950 backdrop-blur-2xl shadow-inner shadow-light-700/20 p-5 md:p-7">
            {/* modal header */}
            <div className="flex justify-between items-center">
              <Heading className="capitalize" type="h3" variant="boxed">
                {title}
              </Heading>
              <Button
                className="text-lg text-light-400"
                isIconOnly
                onClick={onClose}
              >
                <MdClose />
              </Button>
            </div>
            {/* (optional) modal description */}
            {description && (
              <div className="my-3 text-light-400">{description}</div>
            )}
            {/* modal content */}
            <div className="mt-5">{children}</div>
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
};

export default Modal;
