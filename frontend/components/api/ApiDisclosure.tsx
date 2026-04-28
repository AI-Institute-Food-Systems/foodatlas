"use client";

import {
  Disclosure,
  DisclosureButton,
  DisclosurePanel,
} from "@headlessui/react";
import { MdExpandMore } from "react-icons/md";

import Card from "@/components/basic/Card";

interface ApiDisclosureProps {
  requestType: "GET" | "POST";
  endpoint: string;
  description: React.ReactNode;
  panelContent: React.ReactNode;
}

const ApiDisclosure = ({
  requestType,
  endpoint,
  description,
  panelContent,
}: ApiDisclosureProps) => {
  return (
    <Card className="p-0 md:p-0">
      <Disclosure as="div">
        {({ open }) => (
          <>
            <DisclosureButton className="flex min-w-full justify-between rounded-lg text-left items-center text-light-100">
              <div className="flex gap-7 items-center justify-between w-full p-4 md:p-6">
                <div className="flex gap-7">
                  <div
                    className={`font-bold h-fit w-16 my-auto px-2 py-1 rounded min-w-16 text-center ${
                      requestType === "GET"
                        ? "bg-amber-500 text-amber-50"
                        : "bg-cyan-500 text-cyan-50"
                    } `}
                  >
                    {requestType}
                  </div>
                  <div className="">
                    <code className="text-lg">{endpoint}</code>
                    <div className="mt-0.5 text-light-300">{description}</div>
                  </div>
                </div>
                <MdExpandMore
                  className={`flex-shrink-0 ${
                    open ? "rotate-180 transform" : ""
                  } h-5 w-5 text-light-300`}
                />
              </div>
            </DisclosureButton>
            <DisclosurePanel className="text-lg text-light-300 rounded-lg p-4 md:p-6">
              {panelContent}
            </DisclosurePanel>
          </>
        )}
      </Disclosure>
    </Card>
  );
};

ApiDisclosure.displayName = "ApiDisclosure";

export default ApiDisclosure;
