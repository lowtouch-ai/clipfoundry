'use client'

import { motion } from 'framer-motion'
import { Inbox, PenLine, Ratio, Zap } from 'lucide-react'

const FEATURES = [
  {
    icon: Inbox,
    title: 'Works in your inbox',
    description:
      'Email-native workflow — no dashboard to babysit. Send your request and walk away. The agent handles the rest.',
  },
  {
    icon: PenLine,
    title: 'AI writes the script',
    description:
      "Don't have a script? Describe what you want and ClipFoundry drafts it. You review and approve before anything is generated.",
  },
  {
    icon: Ratio,
    title: 'Auto aspect ratio',
    description:
      'Portrait or landscape — ClipFoundry detects 9:16 or 16:9 automatically from the images you provide. No manual config.',
  },
  {
    icon: Zap,
    title: '5 free videos, fast',
    description:
      'Start with 5 freemium generations, no credit card required. Most videos are ready in under 10 minutes.',
  },
]

export default function Features() {
  return (
    <section className="relative bg-lt-surface py-16 px-4">
      <div className="max-w-6xl mx-auto">
        <div className="flex flex-col items-center mb-8">
          <p className="text-base font-mono font-semibold uppercase tracking-[0.15em] text-lt-accent mb-3">
            Why ClipFoundry
          </p>
          <h2 className="text-4xl lg:text-[3.25rem] font-bold text-lt-text text-center leading-tight">
            Built to stay out of your way
          </h2>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
          {FEATURES.map((feature, i) => {
            const Icon = feature.icon
            return (
              <motion.div
                key={feature.title}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                whileHover={{ y: -6, transition: { duration: 0.2 } }}
                viewport={{ once: true }}
                transition={{ duration: 0.5, delay: i * 0.1 }}
                className="bg-lt-dark border border-white/10 rounded-2xl p-7 flex gap-5 cursor-default"
              >
                <div className="shrink-0 w-10 h-10 rounded-xl bg-lt-accent/15 flex items-center justify-center">
                  <Icon size={18} className="text-lt-accent" />
                </div>
                <div>
                  <h3 className="text-2xl font-semibold text-lt-text mb-2">{feature.title}</h3>
                  <p className="text-xl text-lt-text/65 leading-relaxed">{feature.description}</p>
                </div>
              </motion.div>
            )
          })}
        </div>
      </div>

      {/* Section transition to lt-dark */}
      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-b from-transparent to-lt-dark pointer-events-none" />
    </section>
  )
}
