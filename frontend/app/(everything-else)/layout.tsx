import Footer from "@/components/navigation/Footer";
import Navbar from "@/components/navigation/Navbar";

interface Props {
  children: React.ReactNode;
}

const Layout = ({ children }: Props) => {
  return (
    <div>
      <Navbar />
      <div className="my-24 md:my-28 lg:my-32 px-3 md:px-12 ">
        <div className="max-w-6xl mx-auto min-h-screen">{children}</div>
      </div>
      <Footer />
    </div>
  );
};

export default Layout;

Layout.displayName = "Layout";
