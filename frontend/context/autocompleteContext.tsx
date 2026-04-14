import { createContext, useState } from "react";

type AutocompleteContextValue = {
  autocompleteTerm: string;
  setAutocompleteTerm: (autocompleteTerm: string) => void;
};

export const AutocompleteContext = createContext<AutocompleteContextValue>({
  autocompleteTerm: "",
  setAutocompleteTerm: () => {},
});

interface AutocompleteProviderProps {
  children: React.ReactNode;
}

export const AutocompleteProvider = ({
  children,
}: AutocompleteProviderProps) => {
  const [autocompleteTerm, setAutocompleteTerm] = useState("");

  const value: AutocompleteContextValue = {
    autocompleteTerm,
    setAutocompleteTerm,
  };

  return (
    <AutocompleteContext.Provider value={value}>
      {children}
    </AutocompleteContext.Provider>
  );
};
