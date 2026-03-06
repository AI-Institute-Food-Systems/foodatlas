"use client";

import React, { useState, useEffect, useMemo } from "react";
import { useRouter } from "next/navigation";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import {
  MdArrowDownward,
  MdArrowUpward,
  MdInfo,
  MdInfoOutline,
  MdKeyboardArrowDown,
} from "react-icons/md";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LabelList,
  TooltipProps,
  BarProps,
} from "recharts";
import {
  ValueType,
  NameType,
} from "recharts/types/component/DefaultTooltipContent";

import Card from "@/components/basic/Card";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";

const BAR_HEIGHT = 28;

interface DotPlotProps {
  data:
    | {
        id: string;
        name: string;
        median_concentration: { value: number; unit: string };
      }[]
    | undefined
    | null;
}

type SortedData = { food: string; value: any; id: string };

const ConcentrationCompositionPlot = ({ data }: DotPlotProps) => {
  const router = useRouter();
  const [sortedData, setSortedData] = useState<SortedData[]>([]);
  const [sortOrder, setSortOrder] = useState("desc");

  // sort data
  useEffect(() => {
    if (!data) return;

    const d = Array.from(
      data.map((row) => {
        return {
          food: row.name,
          value: row.median_concentration?.value ?? 0,
          id: row.id,
        };
      })
    );

    const sortedData = [...d].sort((a, b) => {
      if (a.value === undefined) return 1;
      if (b.value === undefined) return -1;
      if (a.value === b.value) return 0;
      return sortOrder === "asc" ? a.value - b.value : b.value - a.value;
    });

    setSortedData(sortedData);
  }, [data, sortOrder]);

  // custom bar
  const CustomBar = (props: BarProps) => {
    const { x, y, width, height } = props;

    return (
      <rect height={height} width={width} x={x} y={y} rx={2.5} fill="#0891b2" />
    );
  };

  // custom floating tooltip
  const CustomTooltip = ({
    active,
    payload,
    label,
  }: TooltipProps<ValueType, NameType>) => {
    if (active) {
      return (
        <div className="border border-light-50/5 bg-light-800 p-3 rounded-lg text-xs flex flex-col text-light-50">
          <span className="capitalize font-bold">{label}</span>
          <span>
            {formatConcentrationValueAlt(payload?.[0]?.value as number)}{" "}
            {payload?.[0]?.unit}
          </span>
        </div>
      );
    }
  };

  // custom label on bars
  const CustomLabel = (props: any) => {
    const { x, y, width, height, value } = props;

    const fontSize = 16;
    const offset = 4;
    // empirical correction so the thing looks like it's in the middle (not caring about the baseline)
    const baselineCorrection = 2.1;

    return (
      <svg>
        <defs>
          <filter id="shadow" x="-50%" y="-50%" width="200%" height="200%">
            <feDropShadow
              dx="0"
              dy="0"
              stdDeviation="1.5"
              floodColor="black"
              floodOpacity="0.3"
            />
          </filter>
        </defs>
        <g>
          <text
            x={x + offset}
            y={y + height / 2 + fontSize / 2 - baselineCorrection}
            width={width}
            height={height}
            fill="#fff"
            fontSize={fontSize}
            filter="url(#shadow)"
            className="capitalize"
          >
            {value}
          </text>
        </g>
      </svg>
    );
  };

  // calculate chart height from data length
  const chartHeight = (sortedData.length + 1.5) * BAR_HEIGHT;

  // memoize graph to optimize performance
  const graph = useMemo(() => {
    // handle bar click
    const handleClick = (commonName: string) => {
      router.push(`/food/${encodeURIComponent(encodeSpace(commonName))}`);
    };

    return (
      <ResponsiveContainer width="100%" height={chartHeight}>
        <BarChart
          data={sortedData}
          layout="vertical"
          margin={{ top: 0, right: 6, bottom: 10, left: 6 }}
          style={{ cursor: "pointer" }}
          maxBarSize={1000}
          onClick={(state) =>
            handleClick(state?.activePayload?.[0]?.payload.food)
          }
        >
          <XAxis
            type="number"
            dataKey="value"
            name="concentration (mg/100g)"
            orientation="top"
            fontSize={10}
            tickCount={10}
            axisLine={false}
            label={{ angle: -90, offset: 10 }}
          />
          <YAxis
            type="category"
            dataKey="food"
            width={0}
            tickCount={1}
            tickLine
          />
          <Bar
            dataKey="value"
            shape={(props: BarProps) => <CustomBar {...props} />}
          >
            <LabelList dataKey="food" content={<CustomLabel />} />
          </Bar>
          <CartesianGrid
            stroke="#595653"
            strokeDasharray="5 5"
            horizontal={false}
          />
          <Tooltip
            animationDuration={0}
            content={<CustomTooltip />}
            cursor={false}
          />
        </BarChart>
      </ResponsiveContainer>
    );
  }, [chartHeight, sortedData, router]);

  return (
    <Card>
      {data && data.length > 0 ? (
        <div className="flex flex-col gap-5">
          {/* info & sort container */}
          {/* search */}
          {/* <div className="relative flex items-center">
          <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
          <input
            className="pl-9 w-60 h-9 text-sm rounded-lg border border-light-50/5 bg-light-800 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
            type="text"
            placeholder="Search foods"
            onChange={handleSearchChange}
          />
        </ */}
          {/* label & sort container */}
          <div className="flex justify-between items-center">
            <span className="text-light-400">Found {data.length} foods</span>
            {/* sort */}
            <div className="self-end flex items-center gap-10">
              <div className="flex items-center gap-2 text-light-300 text-sm/6">
                <span>sort names</span>
                <Menu>
                  <MenuButton className="w-32 inline-flex justify-between items-center gap-1.5 rounded-full bg-light-800 py-1.5 px-3 text-light-100 focus:outline-none data-[hover]:bg-light-700 data-[open]:bg-light-700 data-[focus]:outline-1 data-[focus]:outline-light-50">
                    {
                      {
                        asc: "ascending",
                        desc: "descending",
                      }[sortOrder]
                    }
                    <MdKeyboardArrowDown className="w-4 h-4" />
                  </MenuButton>
                  <MenuItems
                    transition
                    anchor="bottom end"
                    className="mt-2 w-36 origin-top-right rounded-xl border border-light-50/5 bg-white/5 backdrop-blur-2xl p-1 text-sm/6 text-white transition duration-100 ease-out [--anchor-gap:var(--spacing-1)] focus:outline-none data-[closed]:scale-95 data-[closed]:opacity-0"
                  >
                    <MenuItem>
                      <button
                        className="group flex w-full items-center gap-2 rounded-lg py-1.5 px-3 data-[focus]:bg-white/10"
                        onClick={() => setSortOrder("asc")}
                      >
                        <MdArrowUpward className="text-light-400" />
                        Ascending
                      </button>
                    </MenuItem>
                    <MenuItem>
                      <button
                        className="group flex w-full items-center gap-2 rounded-lg py-1.5 px-3 data-[focus]:bg-white/10"
                        onClick={() => setSortOrder("desc")}
                      >
                        <MdArrowDownward className="text-light-400" />
                        Descending
                      </button>
                    </MenuItem>
                  </MenuItems>
                </Menu>
              </div>
            </div>
          </div>
          {/* graph container */}
          <div className="mt-2 min-h-16 max-h-80 overflow-y-auto">{graph}</div>
          {/* concentration unit info */}
          <div className="flex items-center gap-1.5 text-sm text-light-400">
            <MdInfo />
            All concentrations shown are measured in mg / 100g
          </div>
        </div>
      ) : (
        <div className="h-16 flex items-center justify-center text-light-300 gap-2">
          <MdInfoOutline /> No foods found
        </div>
      )}
    </Card>
  );
};

ConcentrationCompositionPlot.displayName = "ConcentrationCompositionPlot";

export default ConcentrationCompositionPlot;
