import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

interface PortalProps {
  children: React.ReactNode;
}

const Portal = ({ children }: PortalProps) => {
  const [mounted, setMounted] = useState(false);
  const portalContainerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    // create a div element for the portal container
    const portalContainer = document.createElement("div");
    // append the container to the body element
    document.body.appendChild(portalContainer);
    // assign the container reference
    portalContainerRef.current = portalContainer;
    // set mounted to true
    setMounted(true);

    // clean up function to remove the portal container from the DOM
    return () => {
      document.body.removeChild(portalContainer);
    };
  }, []);
  // render the children into the portal container
  return mounted && portalContainerRef.current
    ? createPortal(children, portalContainerRef.current)
    : null;
};

Portal.displayName = "Portal";

export default Portal;
