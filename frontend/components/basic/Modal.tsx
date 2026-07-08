// modal wrapper for headless ui dialog

import { Dialog, DialogPanel } from "@headlessui/react";
import { useEffect } from "react";
import { MdClose } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Heading from "@/components/basic/Heading";
import Button from "@/components/basic/Button";

interface ModalProps {
  children: React.ReactNode;
  title: string;
  description?: React.ReactNode;
  isOpen: boolean;
  onClose: () => void;
  // When true, the dialog panel takes a fixed viewport-bounded height and
  // its children stack as a flex column. The `children` area scrolls
  // internally (flex-1 overflow-y-auto); the optional `footer` pins to
  // the bottom. Use this when content height varies with pagination/state
  // and you don't want the dialog re-centering on every page change.
  fullHeight?: boolean;
  footer?: React.ReactNode;
  // Optional filter sidebar. Rendered visually OUTSIDE the dialog
  // panel's left edge at min-[1440px] (via absolute right-full),
  // but DOM-wise INSIDE the panel so Headless UI's outside-click
  // detection treats sidebar interactions as inside the dialog and
  // doesn't dismiss it. Hidden below 1440; callers offer a Filters
  // drawer inside `children` for narrower viewports.
  sidebar?: React.ReactNode;
}

const Modal = ({
  isOpen,
  onClose,
  children,
  title,
  description,
  fullHeight,
  footer,
  sidebar,
}: ModalProps) => {
  // Lock page scroll while the dialog is open — Headless UI's Dialog
  // doesn't do this on its own in v2. The scroll container in this app
  // is <html> (globals.css forces `overflow-y: scroll !important`),
  // and inline-style `!important` doesn't reliably beat the stylesheet
  // rule across browsers, so we toggle a class that the stylesheet
  // raises above it via higher specificity.
  useEffect(() => {
    if (!isOpen) return;
    document.documentElement.classList.add("modal-open");
    return () => {
      document.documentElement.classList.remove("modal-open");
    };
  }, [isOpen]);

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
      <div className="fixed inset-0 overflow-y-auto p-3 md:p-12">
        {/* center content */}
        <div className="flex min-h-full items-center justify-center">
          <DialogPanel
            className={twMerge(
              "relative w-full max-w-5xl rounded-xl border border-light-50/5 bg-light-950 backdrop-blur-2xl shadow-inner shadow-light-700/20 p-5 md:p-7",
              fullHeight && "flex flex-col h-[min(85vh,800px)]"
            )}
          >
            {/* Sidebar hangs OUTSIDE the panel visually via absolute
             * right-full, but DOM-wise it lives INSIDE DialogPanel so
             * Headless UI's outside-click detection treats sidebar
             * clicks as "inside" and doesn't dismiss the modal. */}
            {sidebar && (
              <aside className="hidden min-[1440px]:block absolute right-full mr-6 top-0 w-48">
                {sidebar}
              </aside>
            )}
            {/* modal header */}
            <div className="flex justify-between items-center shrink-0">
              <Heading className="capitalize" type="h3" variant="boxed">
                {title}
              </Heading>
              <Button
                className="text-lg text-light-400"
                isIconOnly
                onClick={onClose}
                aria-label="Close dialog"
              >
                <MdClose />
              </Button>
            </div>
            {/* (optional) modal description */}
            {description && (
              <div className="my-3 text-light-400 shrink-0">{description}</div>
            )}
            {/* modal content */}
            <div
              className={twMerge(
                fullHeight ? "mt-5 flex-1 min-h-0 flex flex-col" : "mt-5"
              )}
            >
              {children}
            </div>
            {footer && <div className="mt-4 shrink-0">{footer}</div>}
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
};

export default Modal;
