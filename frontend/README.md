# FoodAtlas Frontend

Next.js 14 web application with React 18, TypeScript, and Tailwind CSS using the App Router.

## Getting Started

```bash
npm ci
npm run dev
```

Open [http://localhost:3001](http://localhost:3001) to view the app. The dev server is pinned to port 3001 in `package.json` (see the `dev` script).

> The local API's default `API_CORS_ORIGINS` is still `http://localhost:3000`. To call the API from `localhost:3001` you need to set `API_CORS_ORIGINS=http://localhost:3001` on the API process.

## Commands

| Command         | Description                      |
|-----------------|----------------------------------|
| `npm run dev`   | Start development server         |
| `npm run build` | Production build                 |
| `npm run lint`  | ESLint + TypeScript type check   |
| `npm test`      | Run tests (Vitest)               |

## Project Structure

```
frontend/
├── app/                    # Next.js App Router pages and layouts
│   ├── (home)/             # Landing page route group
│   ├── (everything-else)/  # All other pages (about, contact, entities, etc.)
│   └── api/                # API routes (auth, contact form)
├── components/             # Reusable React components
│   ├── basic/              # UI primitives (Button, Card, Modal, etc.)
│   ├── entities/           # Entity detail page components (food, chemical, disease)
│   ├── icons/              # SVG icon components
│   ├── landing/            # Landing page sections
│   ├── navigation/         # Navbar and Footer
│   └── search/             # Search bar and results
├── context/                # React context providers
├── hooks/                  # Custom React hooks
├── styles/                 # Global CSS and font config
├── types/                  # TypeScript type definitions
└── utils/                  # Utility functions (fetching, downloads)
```

## Environment Variables

Create a `.env.local` file (or set env vars) before running locally:

| Variable | Local default | Production source | Description |
|---|---|---|---|
| `NEXT_PUBLIC_API_URL` | `http://localhost:8000` | Vercel project env var → ALB DNS or domain | Backend API URL |
| `NEXT_PUBLIC_API_KEY` | — | Vercel project env var | Backend API key (not needed when API runs in debug mode) |
| `VALIDATION_PAGE_PASSWORD` | — | Vercel project env var | Password for the validation page (NextAuth) |
| `NEXTAUTH_SECRET` | — | Vercel project env var | NextAuth.js secret |
| `RESEND_API_KEY` | — | Vercel project env var | Resend API key (contact form) |
| `EMAIL_FROM` | — | Vercel project env var | Sender email address |
| `EMAIL_TO` | — | Vercel project env var | Recipient email address(es) |

## Production

The frontend is deployed via **Vercel**, which watches this repo and auto-builds on push (preview builds for branches, production builds for `main`). The only setup needed beyond pushing code:

1. Set `NEXT_PUBLIC_API_URL` in the Vercel project to the production API endpoint. Today that's the raw ALB DNS; long-term it should be a custom domain with HTTPS via ACM. See [`infra/README.md`](../infra/README.md) for the API deployment.
2. Set `NEXT_PUBLIC_API_KEY` to match the API's bearer token (the `API_KEY` env var injected into the ECS task).
3. Set the contact-form and auth-related env vars as needed.

> **Mixed content note.** As of writing, the ALB serves HTTP only. Browsers block HTTPS pages from making HTTP requests, so until HTTPS is wired up on the ALB, end-to-end calls from a Vercel-hosted Next.js page to the API will fail with a mixed-content error. Tracked as a deferred infrastructure task.
