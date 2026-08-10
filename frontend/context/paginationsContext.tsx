"use client";

import React, { createContext, useContext, useState, ReactNode } from "react";

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
  // Drop every table's page state. Entries are keyed by tableId and are
  // otherwise never removed, so a table that unmounts and remounts (e.g.
  // switching entity tabs) would come back on whatever page it was left
  // on — with its filters freshly reset, which reads as a bug.
  resetAllPaginations: () => void;
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

  const setTablePaginations = (
    tableId: string,
    currentPage: number,
    rowsPerPage: number = 10
  ) => {
    setPaginations((prevState) => ({
      ...prevState,
      [tableId]: { currentPage, rowsPerPage },
    }));
  };

  const getTablePaginations = (tableId: string): PaginationssSettings => {
    return paginations[tableId] || { currentPage: 1, rowsPerPage: 10 };
  };

  const resetAllPaginations = () => setPaginations({});

  return (
    <PaginationsContext.Provider
      value={{ setTablePaginations, getTablePaginations, resetAllPaginations }}
    >
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
