import type { Config } from 'tailwindcss'
// eslint-disable-next-line @typescript-eslint/no-require-imports
const flattenColorPalette = require('tailwindcss/lib/util/flattenColorPalette').default

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function addVariablesForColors({ addBase, theme }: any) {
  const allColors = flattenColorPalette(theme('colors'))
  const newVars = Object.fromEntries(
    Object.entries(allColors).map(([key, val]) => [`--${key}`, val])
  )
  addBase({ ':root': newVars })
}

const config: Config = {
  content: [
    './app/**/*.{ts,tsx}',
    './components/**/*.{ts,tsx}',
  ],
  theme: {
    extend: {
      colors: {
        // ── Brand navy ──
        'lt-dark':    '#041250',
        'lt-surface': '#0f1e66',
        'lt-purple':  '#1a2878',

        // ── Brand green ──
        'lt-green':   '#7FFF00',

        // ── Brand hot pink / magenta ──
        'lt-accent':  '#f031d0',
        'lt-pink':    '#f580e8',
        'lt-lavender':'#c060f8',

        // ── Light sections ──
        'lt-light':   '#f4f2ff',
        'lt-light-2': '#ffffff',

        // ── Borders ──
        'lt-border-dark':  '#1c2b85',
        'lt-border-light': '#e5e0f8',

        // ── Text ──
        'lt-text':    '#f0e8ff',
        'lt-ink':     '#0a0d30',
        'lt-ink-dim': '#4a4870',
      },
      fontFamily: {
        sans: ['-apple-system', 'BlinkMacSystemFont', '"Segoe UI"', 'Roboto', 'Oxygen-Sans', 'Ubuntu', 'Cantarell', '"Helvetica Neue"', 'sans-serif'],
        mono: ['JetBrains Mono', 'Menlo', 'monospace'],
      },
      animation: {
        aurora: 'aurora 60s linear infinite',
      },
      keyframes: {
        aurora: {
          from: { backgroundPosition: '50% 50%, 50% 50%' },
          to:   { backgroundPosition: '350% 50%, 350% 50%' },
        },
      },
    },
  },
  plugins: [addVariablesForColors],
}

export default config
