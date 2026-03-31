'use client'

import { motion } from 'framer-motion'

const BEAM_COUNT = 24
const BEAMS = Array.from({ length: BEAM_COUNT }, (_, i) => {
  const t = i / (BEAM_COUNT - 1)
  const startY = 20 + t * 560
  const endY  = 50 + t * 500
  const cp1x  = 200 + Math.sin(t * Math.PI) * 160
  const cp1y  = startY - 70 + Math.cos(t * Math.PI * 2) * 55
  const cp2x  = 960 + Math.cos(t * Math.PI) * 200
  const cp2y  = endY  + 50 + Math.sin(t * Math.PI * 1.5) * 70

  // Color gradient: lt-accent (#f031d0) → lt-lavender (#c060f8)
  const r = Math.round(240 - t * 48)
  const g = Math.round(49  + t * 47)
  const b = Math.round(208 + t * 40)
  const a = (0.45 + Math.sin(t * Math.PI) * 0.30).toFixed(2)

  return {
    d: `M-10,${startY.toFixed(0)} C${cp1x.toFixed(0)},${cp1y.toFixed(0)} ${cp2x.toFixed(0)},${cp2y.toFixed(0)} 1450,${endY.toFixed(0)}`,
    color: `rgba(${r},${g},${b},${a})`,
    strokeWidth: +(0.5 + Math.sin(t * Math.PI) * 0.25).toFixed(2),
    delay: i * 0.18,
    duration: 3.5 + (i % 6) * 0.3,
  }
})

export default function SvgBeams() {
  return (
    <div className="absolute inset-0 pointer-events-none overflow-hidden">
      <svg
        viewBox="0 0 1440 600"
        preserveAspectRatio="xMidYMid slice"
        className="absolute inset-0 w-full h-full"
        fill="none"
      >
        {BEAMS.map((beam, i) => (
          <motion.path
            key={i}
            d={beam.d}
            stroke={beam.color}
            strokeWidth={beam.strokeWidth}
            fill="none"
            initial={{ pathLength: 0, opacity: 0 }}
            animate={{
              pathLength: [0, 1, 1, 0],
              opacity:    [0, 0.9, 0.9, 0],
            }}
            transition={{
              duration:    beam.duration,
              delay:       beam.delay,
              repeat:      Infinity,
              repeatDelay: 0.4,
              ease:        'easeInOut',
              times:       [0, 0.4, 0.7, 1],
            }}
          />
        ))}
      </svg>
    </div>
  )
}
