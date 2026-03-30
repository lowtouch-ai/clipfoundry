'use client'

import { motion } from 'framer-motion'

const STEPS = [
  {
    number: '01',
    title: 'Share your idea',
    description:
      'Email or chat via OpenWebUI. Attach a few images and optionally a script. No script? The agent drafts one for you.',
  },
  {
    number: '02',
    title: 'Review & approve',
    description:
      'ClipFoundry generates your script and waits for your sign-off before anything starts. You stay in control.',
  },
  {
    number: '03',
    title: 'Receive your video',
    description:
      'Your finished video arrives in 5–10 minutes. Aspect ratio (9:16 or 16:9) is auto-detected from your images.',
  },
]

export default function HowItWorks() {
  return (
    <section className="relative bg-lt-dark py-16 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col items-center mb-8">
          <div className="w-8 h-px bg-lt-accent mb-4" />
          <p className="text-base font-mono font-semibold uppercase tracking-[0.15em] text-lt-accent mb-3">
            How It Works
          </p>
          <h2 className="text-4xl lg:text-[3.25rem] font-bold text-lt-text text-center leading-tight">
            Three steps to a finished video
          </h2>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
          {STEPS.map((step, i) => (
            <motion.div
              key={step.number}
              initial={{ opacity: 0, y: 24 }}
              whileInView={{ opacity: 1, y: 0 }}
              whileHover={{ y: -6, transition: { duration: 0.2 } }}
              viewport={{ once: true }}
              transition={{ duration: 0.5, delay: i * 0.12 }}
              className="relative bg-lt-surface border border-white/10 rounded-2xl p-8 cursor-default"
            >
              <span className="text-5xl font-bold text-lt-accent/40 leading-none block mb-5">
                {step.number}
              </span>
              <h3 className="text-2xl font-semibold text-lt-text mb-2">{step.title}</h3>
              <p className="text-xl text-lt-text/65 leading-relaxed">{step.description}</p>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Section transition to lt-surface */}
      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-b from-transparent to-lt-surface pointer-events-none" />
    </section>
  )
}
