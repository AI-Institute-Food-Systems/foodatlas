"use client";

import {
  MdKeyboardDoubleArrowLeft,
  MdKeyboardArrowLeft,
  MdKeyboardArrowRight,
  MdKeyboardDoubleArrowRight,
} from "react-icons/md";

import Button from "@/components/basic/Button";
import LoadingCard from "@/components/basic/LoadingCard";
import { usePaginations } from "@/context/paginationsContext";

interface PagintationProps {
  tableId: string;
  numberOfPages: number | undefined;
  isLoading: boolean;
}

const Pagination = ({
  tableId,
  numberOfPages,
  isLoading,
}: PagintationProps) => {
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
        isDisabled={isLoading || currentPage === 1}
        isSquared
        onClick={handleFirstPageClick}
      >
        <MdKeyboardDoubleArrowLeft />
      </Button>
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLoading || currentPage === 1}
        isSquared
        onClick={handlePreviousPageClick}
      >
        <MdKeyboardArrowLeft />
      </Button>
      {isLoading ? (
        <LoadingCard className="h-6 w-32" />
      ) : (
        <span className="w-40 text-center">
          Page {numberOfPages! > 0 ? currentPage : 0} of {numberOfPages}
        </span>
      )}
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLoading || currentPage === numberOfPages}
        onClick={handleNextPageClick}
      >
        <MdKeyboardArrowRight />
      </Button>
      <Button
        className="hover:bg-zinc-700/80 transition duration-300 ease-in-out"
        isIconOnly
        isDisabled={isLoading || currentPage === numberOfPages}
        onClick={handleLastPageClick}
      >
        <MdKeyboardDoubleArrowRight />
      </Button>
    </div>
  );
};

Pagination.displayName = "Pagination";

export default Pagination;
