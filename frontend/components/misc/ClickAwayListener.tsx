import React, { useEffect, useRef, ReactNode } from "react";

interface ClickAwayListenerProps {
  onClickAway: () => void;
  children: ReactNode;
}

const ClickAwayListener: React.FC<ClickAwayListenerProps> = ({
  onClickAway,
  children,
}) => {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent | TouchEvent) => {
      if (ref.current && !ref.current.contains(event.target as Node)) {
        onClickAway();
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    document.addEventListener("touchstart", handleClickOutside);

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("touchstart", handleClickOutside);
    };
  }, [onClickAway]);

  return <div ref={ref}>{children}</div>;
};

ClickAwayListener.displayName = "ClickAwayListener";

export default ClickAwayListener;
