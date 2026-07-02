import Footer from "@/components/navigation/Footer";
import Navbar from "@/components/navigation/Navbar";

interface Props {
  children: React.ReactNode;
}

const Layout = ({ children }: Props) => {
  return (
    <div>
      <Navbar />
      <div className="my-16 md:my-20 lg:my-24 px-3 md:px-12 ">
        <div className="max-w-5xl mx-auto min-h-screen">{children}</div>
      </div>
      <Footer />
    </div>
  );
};

export default Layout;

Layout.displayName = "Layout";
