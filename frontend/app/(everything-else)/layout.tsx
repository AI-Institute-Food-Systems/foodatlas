import Breadcrumb from "@/components/navigation/Breadcrumb";
import Footer from "@/components/navigation/Footer";
import Navbar from "@/components/navigation/Navbar";

interface Props {
  children: React.ReactNode;
}

const Layout = ({ children }: Props) => {
  return (
    <div>
      <Navbar />
      {/* Top and bottom split apart: the navbar is `fixed top-0` at
       * h-12/h-14, so this margin is the only thing clearing it, while the
       * bottom is just breathing room before the footer. Trimmed 16px off
       * the top at each breakpoint — clearance below the bar goes 48/56/72
       * to 32/40/56px — and left the bottom where it was. */}
      <div className="mt-20 md:mt-24 lg:mt-28 mb-24 md:mb-28 lg:mb-32 px-4 md:px-24 ">
        <div className="max-w-5xl mx-auto min-h-screen">
          <Breadcrumb />
          {children}
        </div>
      </div>
      <Footer />
    </div>
  );
};

export default Layout;

Layout.displayName = "Layout";
