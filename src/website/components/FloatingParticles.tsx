'use client'

import { motion } from 'framer-motion'

interface Particle {
  id: number
  x: number
  y: number
  size: number
  duration: number
  delay: number
  color: string
}

const COLORS = [
  'rgba(192,96,248,0.5)',  // lt-lavender
  'rgba(240,49,208,0.4)',  // lt-accent
  'rgba(127,255,0,0.3)',   // lt-green
  'rgba(192,96,248,0.3)',  // lt-lavender dim
]

// Simple seeded LCG for deterministic particles (avoids hydration mismatch)
function seededRand(seed: number) {
  let s = seed
  return () => {
    s = (s * 1664525 + 1013904223) & 0xffffffff
    return (s >>> 0) / 0xffffffff
  }
}

function generateParticles(count: number): Particle[] {
  const rand = seededRand(42)
  return Array.from({ length: count }, (_, i) => ({
    id: i,
    x: rand() * 100,
    y: rand() * 100,
    size: rand() * 2.5 + 1,
    duration: rand() * 12 + 8,
    delay: rand() * 6,
    color: COLORS[Math.floor(rand() * COLORS.length)],
  }))
}

export default function FloatingParticles({ count = 30 }: { count?: number }) {
  const particles = generateParticles(count)

  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none">
      {particles.map((p) => (
        <motion.div
          key={p.id}
          className="absolute rounded-full"
          style={{
            left: `${p.x}%`,
            top: `${p.y}%`,
            width: p.size,
            height: p.size,
            backgroundColor: p.color,
          }}
          animate={{
            y: [0, -40, 0],
            opacity: [0, 0.8, 0],
            scale: [0.8, 1.4, 0.8],
          }}
          transition={{
            duration: p.duration,
            delay: p.delay,
            repeat: Infinity,
            ease: 'easeInOut',
          }}
        />
      ))}
    </div>
  )
}
