"use client";

import { MdRefresh } from "react-icons/md";

import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";

interface ErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

const Error = ({ error, reset }: ErrorProps) => {
  console.error("error :>> ", error);
  return (
    <div className="px-3 md:px-12 h-screen justify-center flex flex-col text-light-100">
      <div className="w-full max-w-6xl mx-auto">
        <h2 className="text-4xl font-semibold">Runtime Error</h2>
        <p className="mt-4 mb-8 text-lg">
          Something went wrong. If this issue persists, we appreciate you{" "}
          <Link
            href={`mailto:lmasopust@ucdavis.edu?subject=[FoodAtlas] Runtime Error&body=FoodAtlas ecnountered an unexpected runtime error with error message: ${error.message}`}
          >
            reporting this issue
          </Link>{" "}
          .
        </p>
        <code className="bg-light-800 px-2 py-2 rounded-sm">
          <b className="mr-2">Error:</b>
          {error.message}
        </code>
        <Button
          className="mt-10"
          variant="filled"
          size="md"
          onClick={() => {
            reset();
          }}
        >
          <MdRefresh />
          Reload
        </Button>
      </div>
    </div>
  );
};

Error.displayName = "Error";

export default Error;
