"use client";

import { twMerge } from "tailwind-merge";

import { useReportRows } from "@/context/reportModeContext";
import type { ReportContext } from "@/types/Report";

interface Props {
  context: ReportContext;
  children: React.ReactNode;
  className?: string;
  // Render as <span> (default), <div>, or <li>. Choose based on the
  // parent's layout — <span> is safe inside inline flow, <div> if the
  // wrapper needs to be a block/flex item.
  as?: "span" | "div" | "li";
  // Skip the affordance for values that don't have enough anchoring
  // data to file a useful report.
  disabled?: boolean;
}

// Client-side wrapper that lets a server-rendered metadata surface
// participate in the global report-select flow. When the FAB isn't in
// select mode, this is transparent (just renders children). When it is,
// the wrapper picks up the amber-outline row-props from useReportRows
// and captures the click to open the report modal with `context`.
const ReportableField = ({
  context,
  children,
  className,
  as = "span",
  disabled,
}: Props) => {
  const { getRowProps } = useReportRows();
  const props = getRowProps(context, { disabled });
  const Tag = as;
  return (
    <Tag {...props} className={twMerge(className, props.className)}>
      {children}
    </Tag>
  );
};

ReportableField.displayName = "ReportableField";
export default ReportableField;
