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

  return (
    <PaginationsContext.Provider
      value={{ setTablePaginations, getTablePaginations }}
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
