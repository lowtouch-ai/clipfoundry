# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Shape

This repo has two distinct concerns that share the same tree:

1. **The ClipFoundry product itself** — an agentic AI video-production appliance (email + chat workflows, Airflow DAGs, Gemini scripting, Nano Banana / Veo video synthesis). Described in `README.md`. The DAG/agent source is deployed out-of-band via `repos.json`, which maps this repo to `/appz/home/airflow/dags/clipfoundry` on the Airflow host. That DAG code is **not** in this working tree.
2. **The marketing website** at `src/website/` — a Next.js 15 / React 19 app served at clipfoundry.ai. This is the only code actually present in the repo and is where almost all work happens.

When a task references "the site", "the landing page", or UI changes, it's `src/website/`. When it references the product, agent, workflows, or DAGs, that code lives elsewhere.

## Common Commands

All website commands run from `src/website/`:

```bash
npm run dev      # Next.js dev server on :3000
npm run build    # Production build (standalone output — see next.config.mjs)
npm run start    # Serve the production build
npm run lint     # next lint
```

Full stack (website + nginx reverse proxy on :3070) from the repo root:

```bash
# First: cp src/website/.env.local.example src/website/.env.local and fill in SMTP creds
docker compose up --build
```

There is **no test suite** — do not invent test commands or claim tests pass.

## Website Architecture

- **App Router** (`src/website/app/`): single-page marketing site. `page.tsx` composes the section components (`Hero`, `Demo`, `HowItWorks`, `Features`, `EarlyAccess`, `Footer`) in order. `layout.tsx` wraps everything in the `Navbar` and sets site-wide metadata driven by `NEXT_PUBLIC_SITE_URL`.
- **Contact form API** (`app/api/contact/route.ts`): the only server route. Accepts `{ name, email, company, usecase, notes }`, validates server-side, and sends via `nodemailer`. Required env vars: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `MAIL_TO`. Optional: `FROMMAIL` (falls back to `SMTP_USER`). Port 465 triggers `secure: true`; anything else is STARTTLS. The route returns 503 when SMTP env is missing — do not remove that guard.
- **Components** (`src/website/components/`): all client-side; heavy use of `framer-motion`, `three`, and `simplex-noise` for the animated hero/background (`AuroraBackground`, `ConstellationNetwork`, `FloatingParticles`, `SvgBeams`). These are GPU-hungry — keep that in mind when adding more.
- **Styling**: Tailwind 3 with a custom `lt-*` brand palette defined in `tailwind.config.ts` (`lt-dark` navy, `lt-green`, `lt-accent` magenta, etc.). A plugin exposes every color as a CSS variable on `:root`. Prefer these tokens over raw hex.
- **Path alias**: `@/*` resolves from `src/website/` (see `tsconfig.json`) — e.g. `@/components/Hero`.

## Production Serving

`next.config.mjs` sets `output: 'standalone'`, `trailingSlash: true`, and ships a strict CSP plus long-lived immutable caching for `/_next/static/`. The Dockerfile copies the standalone bundle plus `nodemailer` (kept out of the bundle via `serverExternalPackages`) into a second-stage `node:22.11` image. In production, nginx (`src/website/conf/nginx.conf`) proxies to the Next server on port 3000.

If you change CSP, be aware `form-action` includes `mailto:` and `frame-src` allows YouTube — both are load-bearing for existing sections.

## Conventions Worth Knowing

- **Any new env var referenced in code must also be added to `src/website/.env.local.example`** with a short comment. `.env.local` is gitignored; the example file is the single source of truth for what needs to be configured in a new deployment.
- **Commit style**: short imperative subject, optional body explaining the "why". Recent history shows PR-review follow-ups labelled as `Address PR review: <what>` — match that when responding to reviews.
- **Licence**: GPLv3 (see `LICENSE`). Don't add code under an incompatible licence.
