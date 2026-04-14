import Footer from "@/components/navigation/Footer";
import Navbar from "@/components/navigation/Navbar";

interface LayoutProps {
  children: React.ReactNode;
}

const Layout = ({ children }: LayoutProps) => {
  return (
    <>
      <Navbar />
      <div className="mt-10 sm:mt-14 md:mt-16 lg:mt-20">{children}</div>
      <Footer />
    </>
  );
};

export default Layout;

Layout.displayName = "Layout";
