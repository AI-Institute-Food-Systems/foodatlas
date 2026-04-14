import { MdErrorOutline } from "react-icons/md";

import Button from "@/components/basic/Button";

const NotFound = () => {
  return (
    <div className="h-screen px-3 md:px-12 mt-64">
      <div className="max-w-6xl mx-auto flex flex-col gap-6">
        <div className="text-6xl flex gap-5">
          <MdErrorOutline />
          <h1 className="text-6xl font-mono italic">Not Found</h1>
        </div>
        <p className="text-xl text-light-300">
          Could not find the requested resource.
        </p>
        <Button variant="filled" href="/">
          Return Home
        </Button>
      </div>
    </div>
  );
};

export default NotFound;

NotFound.displayName = "NotFound";
