'use client'

import { motion } from 'framer-motion'

// Deterministic node positions using golden ratio spiral — no Math.random, no hydration issues
const NODE_COUNT = 20
const NODES = Array.from({ length: NODE_COUNT }, (_, i) => {
  const t     = i / (NODE_COUNT - 1)
  const angle = t * Math.PI * 2 * 2.618 // golden ratio
  const r     = 12 + t * 38
  return {
    cx:    50 + Math.cos(angle) * r,
    cy:    50 + Math.sin(angle) * r * 0.65,
    size:  1.2 + Math.abs(Math.sin(i * 1.3)) * 1.4,
    delay: i * 0.25,
  }
})

// Edges between nodes closer than threshold
const EDGES = NODES.flatMap((a, i) =>
  NODES.slice(i + 1)
    .filter(b => {
      const dx = a.cx - b.cx, dy = a.cy - b.cy
      return Math.sqrt(dx * dx + dy * dy) < 20
    })
    .map(b => ({ x1: a.cx, y1: a.cy, x2: b.cx, y2: b.cy, delay: (i * 0.15) % 3 }))
)

export default function ConstellationNetwork() {
  return (
    <div className="absolute inset-0 pointer-events-none overflow-hidden">
      <svg
        viewBox="0 0 100 100"
        preserveAspectRatio="xMidYMid slice"
        className="absolute inset-0 w-full h-full"
      >
        {/* Edges */}
        {EDGES.map((e, i) => (
          <motion.line
            key={`e-${i}`}
            x1={e.x1} y1={e.y1} x2={e.x2} y2={e.y2}
            stroke="rgba(192,96,248,0.2)"
            strokeWidth="0.15"
            initial={{ pathLength: 0, opacity: 0 }}
            animate={{ pathLength: [0, 1, 1, 0], opacity: [0, 0.8, 0.8, 0] }}
            transition={{ duration: 4, delay: e.delay, repeat: Infinity, repeatDelay: 2, ease: 'easeInOut', times: [0, 0.3, 0.7, 1] }}
          />
        ))}

        {/* Nodes */}
        {NODES.map((n, i) => (
          <motion.circle
            key={`n-${i}`}
            cx={n.cx} cy={n.cy} r={n.size}
            fill="rgba(192,96,248,0.5)"
            animate={{ cy: [n.cy, n.cy - 1.5, n.cy], opacity: [0.4, 1, 0.4] }}
            transition={{ duration: 3 + (i % 4) * 0.5, delay: n.delay, repeat: Infinity, ease: 'easeInOut' }}
          />
        ))}

        {/* Travelling signal dots along edges */}
        {EDGES.slice(0, 8).map((e, i) => (
          <motion.circle
            key={`s-${i}`}
            r="0.6"
            fill="rgba(240,49,208,0.9)"
            animate={{
              cx: [e.x1, e.x2, e.x1],
              cy: [e.y1, e.y2, e.y1],
              opacity: [0, 1, 1, 0],
            }}
            transition={{ duration: 3, delay: i * 0.6, repeat: Infinity, repeatDelay: 1.5, ease: 'easeInOut', times: [0, 0.45, 0.55, 1] }}
          />
        ))}
      </svg>
    </div>
  )
}
