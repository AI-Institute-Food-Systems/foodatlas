import Link from "@/components/basic/Link";

const DataLicenseNote = () => (
  <p className="mt-4 text-base font-light text-light-300">
    The data is licensed under{" "}
    <Link href="https://creativecommons.org/licenses/by-nc/4.0/">
      CC BY-NC 4.0
    </Link>
    : academic and non-commercial use only. Please cite <i>FoodAtlas</i> in
    any published work.
  </p>
);

export default DataLicenseNote;
