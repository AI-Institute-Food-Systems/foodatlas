/**
 * @author Lukas Masopust
 * @email lmasopust@ucdavis.edu
 * @create date 2025-05-22 13:36:19
 * @modify date 2025-05-22 13:36:19
 * @desc This file is the route for the authentication process.
 */

import NextAuth from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";

const handler = NextAuth({
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        password: { label: "Password", type: "password" },
      },
      async authorize(credentials) {
        // check if password matches
        if (credentials?.password === process.env.VALIDATION_PAGE_PASSWORD) {
          return {
            id: "1",
            name: "Validation User",
            email: "validation@example.com",
          };
        }
        return null;
      },
    }),
  ],
  pages: {
    signIn: "/validation",
  },
  callbacks: {
    async jwt({ token }) {
      return token;
    },
    async session({ session }) {
      return session;
    },
  },
});

export { handler as GET, handler as POST };
