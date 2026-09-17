// The mark for a bioassay, anywhere one is counted or opened.
//
// One export rather than an inline `<MdBiotech />` per call site: assay
// buttons live in five files across three tabs, and three of them had
// drifted onto MdDescription — the document icon, which belongs to
// publications and composition data points. Reading the same glyph for
// "assays" and "papers" makes two different evidence kinds look alike,
// which is the exact confusion the merged evidence tab exists to undo.
//
// Import this; don't reach for MdBiotech directly. `assay-icon-convention`
// fails the build if a new assay button does.

import { MdBiotech } from "react-icons/md";

// size-3 is the chip-icon size shared by every Chip in the app.
const AssayIcon = ({ className = "size-3" }: { className?: string }) => (
  <MdBiotech className={className} />
);

AssayIcon.displayName = "AssayIcon";
export default AssayIcon;
