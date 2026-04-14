/**
 * @author Lukas Masopust
 * @email lmasopust@ucdavis.edu
 * @create date 2025-05-22 14:43:06
 * @modify date 2025-05-22 14:43:06
 * @desc Actual validation content for validation page
 */

import { useEffect, useState } from "react";
import Heading from "../basic/Heading";
import SubHeading from "../basic/SubHeading";
import Card from "../basic/Card";

export default function SignedInContent() {
  const [data, setData] = useState<any>(null);

  // fetch data from API
  useEffect(() => {
    // TODO: move to env
    fetch(
      "https://zn6m0hn4rl.execute-api.us-west-1.amazonaws.com/Prod/list-unverified"
    )
      .then((res) => res.json())
      .then((data) => {
        setData(data);
      })
      .catch((err) => console.error(err));
  }, []);

  return (
    <div className="">
      <Heading type="h1">Data Validation</Heading>
      <SubHeading>New data pending validation since last login</SubHeading>
      <p className="mt-10 text-lg leading-loose text-light-200">
        Lorem ipsum dolor sit amet consectetur adipisicing elit. Quisquam, quos.
      </p>
      {data && (
        <div className="mt-10 flex flex-col gap-4">
          {data.map((item: any) => (
            <Card key={item.paper_id}>{item.title}</Card>
          ))}
        </div>
      )}
    </div>
  );
}
