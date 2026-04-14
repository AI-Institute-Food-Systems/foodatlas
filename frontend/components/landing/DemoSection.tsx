"use client";

import { useRef, useState, useEffect } from "react";
import { MdPause, MdPlayArrow, MdRefresh } from "react-icons/md";

import Button from "@/components/basic/Button";

const DemoSection = () => {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [isVideoVisible, setIsVideoVisible] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          setIsVideoVisible(entry.isIntersecting);
        });
      },
      { threshold: 0.5 }
    );

    const currentVideo = videoRef.current;
    if (currentVideo) {
      observer.observe(currentVideo);
    }

    return () => {
      if (currentVideo) {
        observer.unobserve(currentVideo);
      }
    };
  }, []);

  useEffect(() => {
    if (isVideoVisible && videoRef.current) {
      videoRef.current.play();
      setIsPlaying(true);
    } else if (videoRef.current) {
      videoRef.current.pause();
      setIsPlaying(false);
    }
  }, [isVideoVisible]);

  const togglePlayback = () => {
    if (videoRef.current) {
      if (videoRef.current.paused) {
        videoRef.current.play();
        setIsPlaying(true);
      } else {
        videoRef.current.pause();
        setIsPlaying(false);
      }
    }
  };

  const restartVideo = () => {
    if (videoRef.current) {
      videoRef.current.currentTime = 0;
      videoRef.current.play();
      setIsPlaying(true);
    }
  };

  return (
    <div className="w-full max-w-7xl px-3md:px-12 mx-auto">
      <div className="relative py-32 w-full flex flex-col-reverse lg:flex-row gap-16">
        <div className="lg:w-3/5 h-auto rounded overflow-hidden relative">
          <video
            ref={videoRef}
            muted
            loop
            playsInline
            className="w-full h-auto"
          >
            <source src="/video/demo.mp4" type="video/mp4" />
            Your browser does not support the video tag.
          </video>
          <div className="absolute bottom-0 left-0 right-0 flex justify-end pr-4 gap-4 py-3 bg-light-900/20 backdrop-blur-lg">
            <Button
              className="from-transparent to-transparent hover:from-light-600/50 hover:to-light-600/50 shadow-transparent hover:shadow-transparent"
              variant="filled"
              onClick={togglePlayback}
            >
              {isPlaying ? <MdPause /> : <MdPlayArrow />}
            </Button>
            <Button
              className="from-transparent to-transparent hover:from-light-600/50 hover:to-light-600/50 shadow-transparent hover:shadow-transparent"
              variant="filled"
              onClick={restartVideo}
            >
              <MdRefresh />
            </Button>
          </div>
        </div>
        <div className="lg:w-2/5">
          <h2 className="relative text-lg md:text-xl lg:text-2xl font-semibold">
            <div className="absolute inset-y-0 -left-2 md:-left-3 h-full w-[0.2rem] bg-accent-600 flex items-center"></div>
            Searchable Food Composition Table
          </h2>
          {/* TODO: update this text */}
          <p className="mt-4 sm:text-sm md:text-base lg:text-lg leading-loose text-light-300">
            Use our easy search and filtering options to find evidence-based
            facts about foods and their components.
          </p>
        </div>
      </div>
    </div>
  );
};

DemoSection.displayName = "DemoSection";

export default DemoSection;
