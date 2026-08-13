"use client";

import {
  MdKeyboardDoubleArrowLeft,
  MdKeyboardArrowLeft,
  MdKeyboardArrowRight,
  MdKeyboardDoubleArrowRight,
} from "react-icons/md";

import Button from "@/components/basic/Button";
import Skeleton from "@/components/basic/Skeleton";
import { usePaginations } from "@/context/paginationsContext";

interface PagintationProps {
  tableId: string;
  numberOfPages: number | undefined;
  // No page count yet — show the placeholder in place of the label.
  isLoading: boolean;
  // A refetch with rows already on screen. The label is still valid, so
  // it stays; only the controls lock, to keep a second click from racing
  // the response already in flight.
  isBusy?: boolean;
}

const Pagination = ({
  tableId,
  numberOfPages,
  isLoading,
  isBusy = false,
}: PagintationProps) => {
  const isLocked = isLoading || isBusy;
  const { setTablePaginations, getTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);

  const handleFirstPageClick = () => {
    const newPage = 1;
    setTablePaginations(tableId, newPage, 20);
  };

  const handleNextPageClick = () => {
    const newPage = Math.min(numberOfPages!, currentPage + 1);
    setTablePaginations(tableId, newPage, 20);
  };

  const handlePreviousPageClick = () => {
    const newPage = Math.max(1, currentPage - 1);
    setTablePaginations(tableId, newPage, 20);
  };

  const handleLastPageClick = () => {
    const newPage = numberOfPages!;
    setTablePaginations(tableId, newPage, 20);
  };

  return (
    <div className="my-3 flex items-center justify-between">
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLocked || currentPage === 1}
        isSquared
        onClick={handleFirstPageClick}
        aria-label="First page"
      >
        <MdKeyboardDoubleArrowLeft />
      </Button>
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLocked || currentPage === 1}
        isSquared
        onClick={handlePreviousPageClick}
        aria-label="Previous page"
      >
        <MdKeyboardArrowLeft />
      </Button>
      {isLoading ? (
        // Same w-40 as the real label: at w-32 the whole row re-centred
        // when the text landed.
        <Skeleton className="h-6 w-40" />
      ) : (
        // tabular-nums so "Page 9 of 10" -> "Page 10 of 10" doesn't nudge
        // the arrows either.
        <span className="w-40 text-center tabular-nums">
          Page {numberOfPages! > 0 ? currentPage : 0} of {numberOfPages}
        </span>
      )}
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLocked || currentPage === numberOfPages}
        onClick={handleNextPageClick}
        aria-label="Next page"
      >
        <MdKeyboardArrowRight />
      </Button>
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLocked || currentPage === numberOfPages}
        onClick={handleLastPageClick}
        aria-label="Last page"
      >
        <MdKeyboardDoubleArrowRight />
      </Button>
    </div>
  );
};

Pagination.displayName = "Pagination";

export default Pagination;
