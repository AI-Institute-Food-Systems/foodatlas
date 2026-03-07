# FoodAtlas Frontend

Next.js 14 web application with React 18, TypeScript, and Tailwind CSS using the App Router.

## Getting Started

```bash
npm ci
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the app.

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

| Variable               | Description                        |
|------------------------|------------------------------------|
| `NEXT_PUBLIC_API_URL`  | Backend API base URL               |
| `NEXT_PUBLIC_API_KEY`  | Backend API key                    |
| `RESEND_API_KEY`       | Resend API key (contact form)      |
| `EMAIL_FROM`           | Sender email address               |
| `EMAIL_TO`             | Recipient email address(es)        |
| `NEXTAUTH_SECRET`      | NextAuth.js secret                 |
