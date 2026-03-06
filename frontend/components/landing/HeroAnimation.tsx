"use client";

import React, { useEffect } from "react";

import Graph from "@/components/landing/Graph";
import "@/styles/heroAnimation.css";

const generateRandomKeyframes = () => {
  const keyframes = [];
  for (let i = 0; i < 4; i++) {
    const x = Math.random() * 20 - 10;
    const y = Math.random() * 20 - 10;
    keyframes.push({ x, y });
  }
  return keyframes;
};

const HeroAnimation = () => {
  useEffect(() => {
    const keyframes = generateRandomKeyframes();

    const nodes = Array.from(document.querySelectorAll("circle[id^=node_]"));
    const lines = Array.from(document.querySelectorAll("line[id^=line_]"));

    nodes.forEach((node) => {
      // node.style.setProperty("--kf20-x", `${keyframes[0].x}px`);
    });
  }, []);

  // useEffect(() => {
  //   return;
  //   const nodes = ["node_1", "node_2", "node_3"];
  //   const lines = [
  //     { line: "line_1", startNode: "node_1", endNode: "node_2" },
  //     { line: "line_2", startNode: "node_2", endNode: "node_3" },
  //   ];

  //   nodes.forEach((nodeId) => {
  //     const node = document.getElementById(nodeId);

  //     const setRandomKeyframes = () => {
  //       const keyframes = generateRandomKeyframes();
  //       node.style.setProperty("--kf20-x", `${keyframes[0].x}px`);
  //       node.style.setProperty("--kf20-y", `${keyframes[0].y}px`);
  //       node.style.setProperty("--kf40-x", `${keyframes[1].x}px`);
  //       node.style.setProperty("--kf40-y", `${keyframes[1].y}px`);
  //       node.style.setProperty("--kf60-x", `${keyframes[2].x}px`);
  //       node.style.setProperty("--kf60-y", `${keyframes[2].y}px`);
  //       node.style.setProperty("--kf80-x", `${keyframes[3].x}px`);
  //       node.style.setProperty("--kf80-y", `${keyframes[3].y}px`);
  //       lineElement.style.setProperty("--kf20-x", `${keyframes[0].x}px`);
  //       lineElement.style.setProperty("--kf20-y", `${keyframes[0].y}px`);
  //       lineElement.style.setProperty("--kf40-x", `${keyframes[1].x}px`);
  //       lineElement.style.setProperty("--kf40-y", `${keyframes[1].y}px`);
  //       lineElement.style.setProperty("--kf60-x", `${keyframes[2].x}px`);
  //       lineElement.style.setProperty("--kf60-y", `${keyframes[2].y}px`);
  //       lineElement.style.setProperty("--kf80-x", `${keyframes[3].x}px`);
  //       lineElement.style.setProperty("--kf80-y", `${keyframes[3].y}px`);
  //     };

  //     node.addEventListener("animationiteration", setRandomKeyframes);
  //     lineElement.addEventListener("animationiteration", updateLinePosition);
  //     setRandomKeyframes();
  //   });

  //   const updateLines = () => {
  //     lines.forEach(({ line, startNode, endNode }) => {
  //       const lineElement = document.getElementById(line);
  //       // const startElement = document.getElementById(startNode);
  //       // const endElement = document.getElementById(endNode);

  //       // const startX = parseFloat(startElement.getAttribute("cx"));
  //       // const startY = parseFloat(startElement.getAttribute("cy"));
  //       // const endX = parseFloat(endElement.getAttribute("cx"));
  //       // const endY = parseFloat(endElement.getAttribute("cy"));

  //       // console.log("startX :>> ", startX);

  //       const updateLinePosition = () => {};

  //       // startElement.addEventListener("animationiteration", updateLinePosition);
  //       // endElement.addEventListener("animationiteration", updateLinePosition);
  //       updateLinePosition();
  //     });
  //   };

  //   updateLines();
  // }, []);

  return <Graph />;
};

HeroAnimation.displayName = "HeroAnimation";

export default HeroAnimation;
