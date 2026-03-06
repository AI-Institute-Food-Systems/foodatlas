import { FaInstagram, FaLinkedin, FaThreads, FaYoutube } from "react-icons/fa6";
import { MdOutlineMailOutline } from "react-icons/md";

import FoodAtlasIcon from "@/components/icons/FoodAltasIcon";
import AIFSIcon from "@/components/icons/AIFSIcon";
import UCDIcon from "@/components/icons/UCDIcon";
import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";

const Footer = () => {
  return (
    <div className="w-full py-16 bg-[#090909] text-lg px-3 md:px-12">
      <div className="max-w-6xl mx-auto">
        {/* upper content */}
        <div className="flex gap-8 md:gap-12 flex-col md:flex-row">
          <div className="md:w-1/3">
            {/* <img className="w-[50%] md:w-[70%]" src="/icons/foodatlas.svg" /> */}
            <div className="flex items-start">
              <FoodAtlasIcon
                height={"50"}
                width={"100%"}
                color={"#FFFBF7"}
                preserveAspectRatio={"xMinYMin meet"}
              />
            </div>
            <p className="mt-3 md:mt-5 leading-6 text-sm lg:text-md text-light-300">
              FoodAtlas was created and is maintained by AIFS at the University
              of California, Davis.
            </p>
            <div className="mt-4 md:mt-6 h-16 md:h-20 flex md:justify-between gap-4 sm:gap-10 md:gap-4 text-light-100">
              <a
                className="w-32 md:w-1/2 my-auto"
                href="https://aifs.ucdavis.edu"
                tabIndex={0}
              >
                <AIFSIcon width="100%" height="58" color="#FFFBF7" />
              </a>
              <div className="w-[0.05rem] h-full bg-light-600" />
              <a
                className="w-32 md:w-1/2 my-auto"
                href="https://ucdavis.edu"
                tabIndex={0}
              >
                <UCDIcon width="100%" height="58" color="#FFFBF7" />
              </a>
            </div>
          </div>
          <div className="md:w-1/3">
            <h3 className="text-sm lg:text-md italic font-semibold font-mono text-light-50">
              About AIFS
            </h3>
            <p className="mt-3 md:mt-5 leading-6 text-sm lg:text-md text-light-300">
              The{" "}
              <Link href={"https://aifs.ucdavis.edu"}>
                AI Institute for Next Generation Food Systems, or{" "}
                <span className="whitespace-nowrap">AIFS</span>
              </Link>{" "}
              aims to meet growing demands in our food supply by increasing
              efficiencies using Al and bioinformatics spanning the entire
              system&ndash;from growing crops through consumption. We are
              dedicated to creating AI applications for a healthier, more
              sustainable planet from farm to fork.
            </p>
          </div>
          <div className="md:w-1/3">
            <h3 className="text-sm lg:text-md italic font-semibold font-mono text-light-50">
              Connect with us
            </h3>
            <p className="mt-3 md:mt-5 leading-6 text-sm lg:text-md text-light-300">
              Subscribe to our newsletter to stay up-to-date on AIFS events,
              industry news, and AI research.
            </p>
            <Button
              className="mt-4"
              variant="outlined"
              href="http://eepurl.com/hEVLcP"
              size="sm"
            >
              <MdOutlineMailOutline />
              Newsletter
            </Button>
          </div>
        </div>
        {/* separator */}
        <div className="h-[0.05rem] w-full bg-light-600 my-10" />
        {/* lower content */}
        <div className="w-full">
          {/* logos */}
          <div className="flex gap-8 justify-center text-light-100">
            <a
              className="cursor-pointer hover:text-light-50 transition duration-300 ease-in-out"
              href="https://www.threads.net/@aifoodsystems"
              tabIndex={0}
            >
              <FaThreads className="h-8 w-8 md:h-9 md:w-9" />
            </a>
            <a
              className="cursor-pointer hover:text-light-50 transition duration-300 ease-in-out"
              href="https://www.instagram.com/aifoodsystems"
              tabIndex={0}
            >
              <FaInstagram className="h-8 w-8 md:h-9 md:w-9" />
            </a>
            <a
              className="cursor-pointer hover:text-light-50 transition duration-300 ease-in-out"
              href="https://www.linkedin.com/company/aifoodsystems/"
              tabIndex={0}
            >
              <FaLinkedin className="h-8 w-8 md:h-9 md:w-9" />
            </a>
            <a
              className="cursor-pointer hover:text-light-50 transition duration-300 ease-in-out"
              href="https://www.youtube.com/channel/UCyvVBZ6Qx34ElPB0UmoEF2A"
              tabIndex={0}
            >
              <FaYoutube className="h-8 w-8 md:h-9 md:w-9" />
            </a>
          </div>
          {/* copyright */}
          <p className="text-center mt-10 text-xs text-light-500">
            This work is supported by AFRI Competitive Grant no.
            2020-67021-32855/project accession no. 1024262 from the USDA
            National Institute of Food and Agriculture. <br />
            <br />Ⓒ {new Date().getFullYear()} AIFS. All rights reserved.
          </p>
        </div>
      </div>
    </div>
  );
};

Footer.displayName = "Footer";

export default Footer;
