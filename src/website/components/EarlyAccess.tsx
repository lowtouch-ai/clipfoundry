'use client'

import { motion } from 'framer-motion'

const RING_COUNT = 5
const WARP_COUNT = 24

// Deterministic warp line angles
const WARP_LINES = Array.from({ length: WARP_COUNT }, (_, i) => ({
  angle: (i / WARP_COUNT) * 360,
  length: 35 + Math.abs(Math.sin(i * 1.1)) * 25,
  opacity: 0.15 + Math.abs(Math.sin(i * 0.9)) * 0.2,
  delay: (i / WARP_COUNT) * 2,
  duration: 1.2 + (i % 5) * 0.2,
}))

export default function EarlyAccess() {
  return (
    <section id="early-access" className="relative bg-lt-dark py-16 px-4 overflow-hidden">

      {/* Base radial glow */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{ background: 'radial-gradient(ellipse 60% 55% at 50% 50%, rgba(240,49,208,0.14), transparent)' }}
      />

      {/* Rotating radar sweep */}
      <motion.div
        className="absolute inset-0 pointer-events-none"
        style={{
          background: 'conic-gradient(from 0deg at 50% 50%, rgba(240,49,208,0.18) 0deg, rgba(240,49,208,0.06) 55deg, transparent 85deg, transparent 360deg)',
          maskImage: 'radial-gradient(ellipse 75% 65% at 50% 50%, black 20%, transparent 80%)',
          WebkitMaskImage: 'radial-gradient(ellipse 75% 65% at 50% 50%, black 20%, transparent 80%)',
        }}
        animate={{ rotate: 360 }}
        transition={{ duration: 9, repeat: Infinity, ease: 'linear' }}
      />

      {/* Pulsing sonar rings */}
      <div className="absolute inset-0 pointer-events-none flex items-center justify-center overflow-hidden">
        {Array.from({ length: RING_COUNT }, (_, i) => (
          <motion.div
            key={i}
            className="absolute rounded-full"
            style={{ border: '1px solid rgba(240,49,208,0.5)' }}
            animate={{
              width:   [0, 900],
              height:  [0, 900],
              x:       [0, -450],
              y:       [0, -450],
              opacity: [0.7, 0],
            }}
            transition={{
              duration:    4.5,
              delay:       i * 0.9,
              repeat:      Infinity,
              ease:        'easeOut',
            }}
          />
        ))}
      </div>

      {/* Warp speed lines radiating from center */}
      <div className="absolute inset-0 pointer-events-none flex items-center justify-center overflow-hidden">
        <svg viewBox="0 0 200 200" className="absolute w-full h-full" style={{ maxWidth: 700 }}>
          {WARP_LINES.map((line, i) => {
            const rad = (line.angle * Math.PI) / 180
            const x1  = 100 + Math.cos(rad) * 8
            const y1  = 100 + Math.sin(rad) * 8
            const x2  = 100 + Math.cos(rad) * (8 + line.length)
            const y2  = 100 + Math.sin(rad) * (8 + line.length)
            return (
              <motion.line
                key={i}
                x1={x1} y1={y1} x2={x2} y2={y2}
                stroke={`rgba(240,49,208,${line.opacity.toFixed(2)})`}
                strokeWidth="0.4"
                initial={{ pathLength: 0, opacity: 0 }}
                animate={{ pathLength: [0, 1, 0], opacity: [0, line.opacity * 2, 0] }}
                transition={{
                  duration:    line.duration,
                  delay:       line.delay,
                  repeat:      Infinity,
                  repeatDelay: 1.5 + (i % 4) * 0.3,
                  ease:        'easeOut',
                }}
              />
            )
          })}
        </svg>
      </div>

      <div className="relative max-w-2xl mx-auto">
        <div className="flex justify-center mb-6">
          <span className="text-sm font-semibold px-3 py-1 rounded-full border border-lt-accent/30 text-lt-accent/80">
            Early Access
          </span>
        </div>

        {/* Form card with animated glowing border */}
        <motion.div
          className="rounded-3xl p-[1px] relative"
          animate={{
            boxShadow: [
              '0 0 20px rgba(240,49,208,0.2), 0 0 60px rgba(240,49,208,0.05)',
              '0 0 40px rgba(240,49,208,0.4), 0 0 80px rgba(192,96,248,0.15)',
              '0 0 20px rgba(240,49,208,0.2), 0 0 60px rgba(240,49,208,0.05)',
            ],
          }}
          transition={{ duration: 3, repeat: Infinity, ease: 'easeInOut' }}
        >
          <div className="bg-lt-surface border border-white/10 rounded-3xl p-8 sm:p-10">
            <h2 className="text-3xl sm:text-4xl font-bold text-lt-text mb-8 leading-tight">
              Join the early access list and help us shape the future of agentic video creation.
            </h2>

            <form action="mailto:info@lowtouch.ai" method="post" encType="text/plain" className="space-y-4">
              <div>
                <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="name">Full name</label>
                <input
                  id="name"
                  name="name"
                  type="text"
                  required
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="email">Work email</label>
                <input
                  id="email"
                  name="email"
                  type="email"
                  required
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="company">Company</label>
                <input
                  id="company"
                  name="company"
                  type="text"
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="usecase">What would you use ClipFoundry for?</label>
                <select
                  id="usecase"
                  name="usecase"
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
                >
                  <option value="marketing">Marketing &amp; Advertising</option>
                  <option value="product">Product videos</option>
                  <option value="training">Internal training</option>
                  <option value="other">Other</option>
                </select>
              </div>

              <div>
                <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="notes">Anything we should know (optional)</label>
                <textarea
                  id="notes"
                  name="notes"
                  rows={3}
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 resize-none placeholder:text-lt-text/30"
                />
              </div>

              <button
                type="submit"
                className="w-full bg-lt-accent hover:bg-lt-pink text-white text-lg font-semibold py-3 rounded-xl transition-all duration-300 mt-2 shadow-[0_0_20px_rgba(240,49,208,0.3)] hover:shadow-[0_0_36px_rgba(240,49,208,0.55)]"
              >
                Request early access
              </button>

              <p className="text-xs text-lt-text/40 text-center">We will only contact you with relevant updates.</p>
            </form>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
