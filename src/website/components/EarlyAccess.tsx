'use client'

import { useState } from 'react'
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
  const [status, setStatus] = useState<'idle' | 'sending' | 'success' | 'error'>('idle')

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setStatus('sending')
    const form = e.currentTarget
    const data = new FormData(form)
    const usecase = (form.querySelector('#usecase') as HTMLSelectElement)?.selectedOptions[0]?.text ?? data.get('usecase')

    try {
      const res = await fetch('/api/contact', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name:    data.get('name'),
          email:   data.get('email'),
          company: data.get('company'),
          usecase,
          notes:   data.get('notes'),
        }),
      })
      if (!res.ok) throw new Error()
      setStatus('success')
      form.reset()
    } catch {
      setStatus('error')
    }
  }

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
            <h2 className="text-xl sm:text-2xl font-bold text-lt-text mb-6 leading-tight">
              Join the early access list and help us shape the future of agentic video creation.
            </h2>

            <form onSubmit={handleSubmit} className="space-y-3">
              <div>
                <label className="block text-sm font-medium text-lt-text/70 mb-1" htmlFor="name">Full name</label>
                <input
                  id="name"
                  name="name"
                  type="text"
                  required
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-3 py-2 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-lt-text/70 mb-1" htmlFor="email">Work email</label>
                <input
                  id="email"
                  name="email"
                  type="email"
                  required
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-3 py-2 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-lt-text/70 mb-1" htmlFor="company">Company</label>
                <input
                  id="company"
                  name="company"
                  type="text"
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-3 py-2 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-lt-text/70 mb-1" htmlFor="usecase">What would you use ClipFoundry for?</label>
                <select
                  id="usecase"
                  name="usecase"
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-3 py-2 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
                >
                  <option value="marketing">Marketing &amp; Advertising</option>
                  <option value="product">Product videos</option>
                  <option value="training">Internal training</option>
                  <option value="other">Other</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-lt-text/70 mb-1" htmlFor="notes">Anything we should know (optional)</label>
                <textarea
                  id="notes"
                  name="notes"
                  rows={3}
                  className="w-full bg-lt-dark border border-white/15 rounded-xl px-3 py-2 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40 resize-none placeholder:text-lt-text/30"
                />
              </div>

              <button
                type="submit"
                disabled={status === 'sending' || status === 'success'}
                className="w-full bg-lt-accent hover:bg-lt-pink disabled:opacity-60 disabled:cursor-not-allowed text-white text-base font-semibold py-2.5 rounded-xl transition-all duration-300 mt-2 shadow-[0_0_20px_rgba(240,49,208,0.3)] hover:shadow-[0_0_36px_rgba(240,49,208,0.55)]"
              >
                {status === 'sending' ? 'Sending…' : status === 'success' ? 'Request sent!' : 'Request early access'}
              </button>

              {status === 'success' && (
                <p className="text-sm text-green-400 text-center">We&apos;ll be in touch soon.</p>
              )}
              {status === 'error' && (
                <p className="text-sm text-red-400 text-center">Something went wrong. Please try again.</p>
              )}

              <p className="text-xs text-lt-text/40 text-center">We will only contact you with relevant updates.</p>
            </form>
          </div>
        </motion.div>
      </div>
    </section>
  )
}
