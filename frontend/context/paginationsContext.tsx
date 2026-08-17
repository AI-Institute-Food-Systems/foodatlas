"use client";

import React, {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  ReactNode,
} from "react";

type PaginationssSettings = {
  currentPage: number;
  rowsPerPage: number;
};

interface PaginationssContextProps {
  setTablePaginations: (
    tableId: string,
    currentPage: number,
    rowsPerPage?: number
  ) => void;
  getTablePaginations: (tableId: string) => PaginationssSettings;
}

const PaginationsContext = createContext<PaginationssContextProps | undefined>(
  undefined
);

interface PaginationsProviderProps {
  children: ReactNode;
}

export const PaginationsProvider: React.FC<PaginationsProviderProps> = ({
  children,
}) => {
  const [paginations, setPaginations] = useState<
    Record<string, PaginationssSettings>
  >({});

  // Referential stability matters here, not just for render count: several
  // tables list `setTablePaginations` in the dep array of their data-fetching
  // effect (e.g. FoodCompositionSection). Entity tabs keep their panels
  // mounted, so an unstable identity would make paging one table refire the
  // fetches of every other table on the page, including hidden ones.
  const setTablePaginations = useCallback(
    (tableId: string, currentPage: number, rowsPerPage: number = 10) => {
      setPaginations((prevState) => ({
        ...prevState,
        [tableId]: { currentPage, rowsPerPage },
      }));
    },
    []
  );

  const getTablePaginations = useCallback(
    (tableId: string): PaginationssSettings =>
      paginations[tableId] || { currentPage: 1, rowsPerPage: 10 },
    [paginations]
  );

  const value = useMemo(
    () => ({ setTablePaginations, getTablePaginations }),
    [setTablePaginations, getTablePaginations]
  );

  return (
    <PaginationsContext.Provider value={value}>
      {children}
    </PaginationsContext.Provider>
  );
};

export const usePaginations = (): PaginationssContextProps => {
  const context = useContext(PaginationsContext);
  if (!context) {
    throw new Error("usePaginations must be used within a PaginationsProvider");
  }
  return context;
};
