/**
 * @author Lukas Masopust
 * @email lmasopust@ucdavis.edu
 * @create date 2025-05-22 13:36:19
 * @modify date 2025-05-22 13:41:54
 * @desc This file is the component for the password input.
 */
"use client";

import { useState } from "react";
import { signIn } from "next-auth/react";
import { useRouter } from "next/navigation";
import Card from "../basic/Card";
import Button from "../basic/Button";
import Heading from "../basic/Heading";
import { Input } from "@headlessui/react";
import { Label } from "@headlessui/react";
import { Field } from "@headlessui/react";
import { twMerge } from "tailwind-merge";

const Password = () => {
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const router = useRouter();
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    try {
      const result = await signIn("credentials", {
        password,
        redirect: false,
      });

      if (result?.error) {
        setError("incorrect password");
      } else {
        router.refresh();
      }
    } catch (err) {
      setError("an error occurred");
    }
  };

  return (
    <Card className="p-4 flex flex-col gap-6 max-w-sm">
      <Heading type="h2">Login</Heading>
      <p className="text-sm/6 text-light-400">
        This page is for internal use only. Please enter your password to
        continue.
      </p>
      <form className="space-y-6" onSubmit={handleSubmit}>
        <Field>
          <Label className="text-sm/6 font-medium text-white">Password</Label>
          <Input
            className={twMerge(
              "mt-3 block w-full rounded-lg bg-light-800 border-light-700/50 border py-1.5 px-3 text-sm/6 text-light-50",
              "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
            )}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </Field>
        {error && <p className="text-center text-sm text-red-600">{error}</p>}
        <div>
          <Button type="submit" variant="filled">
            Login
          </Button>
        </div>
      </form>
    </Card>
  );
};

export default Password;
