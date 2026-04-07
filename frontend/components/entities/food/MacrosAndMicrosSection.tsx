// macros and micros section for food entity

"use client";

import { useEffect, useState } from "react";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { getFoodMacroAndMicroData } from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import { MacroAndMicroData } from "@/types";

const VALUES_CUTOFF = 5;

interface MacrosAndMicrosSectionProps {
  commonName: string;
}

const MacrosAndMicrosSection = ({
  commonName,
}: MacrosAndMicrosSectionProps) => {
  const [data, setData] = useState<MacroAndMicroData | undefined>(undefined);
  const [isShowAllOpen, setIsShowAllOpen] = useState(false);
  const [selectedMacroOrMicro, setSelectedMacroOrMicro] = useState<
    string | undefined
  >(undefined);

  useEffect(() => {
    const fetchData = async () => {
      const result = await getFoodMacroAndMicroData(commonName);
      setData(result);
    };
    fetchData();
  }, [commonName]);

  // handle show all click
  const handleShowAllClick = (key: string) => {
    setSelectedMacroOrMicro(key);
    setIsShowAllOpen(true);
  };

  // handle modal close
  const handleModalClose = () => {
    setSelectedMacroOrMicro(undefined);
    setIsShowAllOpen(false);
  };

  return (
    <div className="flex flex-col gap-7">
      {/* macros and micros section header */}
      <Heading type="h2" variant="boxed">
        Macro- & Micronutrients
      </Heading>
      {/* macros and micros container */}
      <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-3">
        {data &&
          Object.entries(data).map(
            ([key, values]) =>
              values.length > 0 && (
                <Card key={key}>
                  <div className="flex items-center justify-between">
                    <Heading
                      type="h3"
                      className="capitalize font-mono italic text-light-400 text-xs"
                    >
                      {key}
                    </Heading>
                    {values.length >= VALUES_CUTOFF && (
                      <Button
                        className="rounded-full border-light-500 text-light-500"
                        variant="outlined"
                        size="xs"
                        onClick={() => handleShowAllClick(key)}
                      >
                        + Show all {values.length}
                      </Button>
                    )}
                  </div>
                  <div className="mt-3 flex flex-col gap-3">
                    {values
                      .slice(0, VALUES_CUTOFF)
                      .map((concentration, index) => (
                        <p
                          key={concentration.name + "_" + index}
                          className="flex items-baseline w-full gap-2.5"
                        >
                          <Link
                            href={`/chemical/${encodeURIComponent(
                              encodeSpace(concentration.name)
                            )}`}
                            className="capitalize"
                            isExternal={false}
                          >
                            {concentration.name}
                          </Link>
                          <span className="flex-grow border-b-2 border-dotted border-light-700" />
                          <span className="text-right">
                            {concentration.median_concentration?.value
                              ? `${formatConcentrationValueAlt(
                                  concentration.median_concentration.value
                                )} ${concentration.median_concentration.unit}`
                              : "—"}
                          </span>
                        </p>
                      ))}
                  </div>
                </Card>
              )
          )}
      </div>
      {/* macros and micros detail modal */}
      {selectedMacroOrMicro && data && (
        <Modal
          title={`All ${selectedMacroOrMicro}`}
          isOpen={isShowAllOpen}
          onClose={handleModalClose}
        >
          <div className="flex flex-col gap-3">
            {data[selectedMacroOrMicro].map((concentration, index) => (
              <p
                key={concentration.name + "_" + index}
                className="flex items-baseline w-full gap-2.5"
              >
                <Link
                  href={`/chemical/${encodeURIComponent(
                    encodeSpace(concentration.name)
                  )}`}
                  className="capitalize"
                  isExternal={false}
                >
                  {concentration.name}
                </Link>
                <span className="flex-grow border-b-2 border-dotted border-light-700" />
                <span className="text-right">
                  {concentration.median_concentration?.value
                    ? `${formatConcentrationValueAlt(
                        concentration.median_concentration.value
                      )} ${concentration.median_concentration.unit}`
                    : "—"}
                </span>
              </p>
            ))}
          </div>
        </Modal>
      )}
    </div>
  );
};

export default MacrosAndMicrosSection;

MacrosAndMicrosSection.displayName = "MacrosAndMicrosSection";
