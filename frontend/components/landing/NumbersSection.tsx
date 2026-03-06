"use client";

import { useEffect, useRef, useState } from "react";
import { Transition } from "@headlessui/react";

import NumberCard from "@/components/landing/NumberCard";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import PublicationIcon from "@/components/icons/PublicationIcon";
import ConnectionIcon from "@/components/icons/ConnectionIcon";
import Heading from "@/components/basic/Heading";

const NumbersSection = () => {
  const [isShowing, setIsShowing] = useState(false);
  const [stats, setStats] = useState({
    associations: 0,
    foods: 0,
    chemicals: 0,
    diseases: 0,
    publications: 0,
  });
  const [isLoading, setIsLoading] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setIsShowing(true);
          }
        });
      },
      { threshold: 0.3 },
    );

    const currentContainer = containerRef.current;
    if (currentContainer) {
      observer.observe(currentContainer);
    }

    return () => {
      if (currentContainer) {
        observer.unobserve(currentContainer);
      }
    };
  }, []);

  useEffect(() => {
    async function fetchNumbers() {
      setIsLoading(true);
      try {
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL}/metadata/statistics`,
          {
            headers: {
              Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
            },
          },
        );
        if (!response.ok) {
          console.error(
            "Failed to fetch statistics:",
            response.status,
            response.statusText,
          );
          setIsLoading(false);
          return;
        }

        const responseData = await response.json();
        console.log("Full API Response:", responseData);
        console.log("Statistics object:", responseData.data?.statistics);

        // Check if statistics is empty
        const stats = responseData.data?.statistics || {};
        if (Object.keys(stats).length === 0) {
          console.warn(
            "⚠️ API returned empty statistics object. Backend may not be populating data.",
          );
        }

        setStats({
          associations: stats.connections || 0,
          foods: stats.foods || 0,
          chemicals: stats.chemicals || 0,
          diseases: stats.diseases || 0,
          publications: stats.publications || 0,
        });
        setIsLoading(false);
      } catch (error) {
        console.error("Error fetching statistics:", error);
        setIsLoading(false);
      }
    }
    fetchNumbers();
  }, []);

  const delays = [
    "delay-0",
    "delay-[250ms]",
    "delay-[500ms]",
    "delay-[750ms]",
    "delay-[1000ms]",
  ];

  return (
    <div className="bg-light-950 w-full">
      {/* padding container */}
      <div className="px-3 md:px-12">
        <div className="max-w-6xl py-28 mx-auto delay-150">
          <div>
            <Heading
              type="h2"
              className="text-2xl sm:text-3xl md:text-4xl lg:text-5xl max-w-2xl text-light-200"
            >
              Building the largest evidence-based food knowledge graph in the
              world
            </Heading>
          </div>
          <div
            className="grid grid-cols-2 md:grid-cols-2 lg:grid-cols-5 gap-4 lg:gap-4 mt-20 min-h-60 w-full"
            ref={containerRef}
          >
            {/* replace with # of associations */}
            {[
              {
                icon: <ConnectionIcon width={40} height={40} color="#F4511E" />,
                number: stats.associations,
                label: "Associations",
              },
              {
                icon: <FoodIcon width={40} height={40} color="#F4511E" />,
                number: stats.foods,
                label: "Foods",
              },
              {
                icon: <ChemicalIcon width={40} height={40} color="#F4511E" />,
                number: stats.chemicals,
                label: "Chemicals",
              },
              {
                icon: <DiseaseIcon width={40} height={40} color="#F4511E" />,
                number: stats.diseases,
                label: "Diseases",
              },
              {
                icon: (
                  <PublicationIcon width={40} height={40} color="#F4511E" />
                ),
                number: stats.publications,
                label: "Publications",
              },
            ].map((card, index) => (
              <Transition
                key={card.label}
                show={isShowing}
                enter={`transition-opacity ease-in-out duration-300 ${delays[index]}`}
                enterFrom="opacity-0"
                enterTo="opacity-100"
              >
                <div>
                  <NumberCard
                    isLoading={isLoading}
                    icon={card.icon}
                    number={card.number}
                    label={card.label}
                  />
                </div>
              </Transition>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

NumbersSection.displayName = "NumbersSection";

export default NumbersSection;
