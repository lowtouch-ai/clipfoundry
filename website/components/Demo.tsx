'use client'

import { useState } from 'react'

const SLIDES = [
  {
    videoId: '3gBI_p6tAgc',
    script: `Your AI agent didn't suddenly break. It just slowly got worse. That's the problem no one talks about. It's called agent decay.

Agent decay is what happens when an AI agent in production gradually stops doing the right thing, even though nothing obviously changed. Same model. Same code. Same prompts. But decisions start slipping. Edge cases increase. Humans step in more often, quietly fixing things.

Why does this happen? Because the world changes. Inputs drift. Data shifts. Business rules evolve. And most agents are never built to notice that they're degrading.

The fix is not a smarter model. The fix is discipline. You need observability into decisions, not just outputs. Guardrails that catch bad actions. Human-in-the-loop checks where it matters. And feedback loops that continuously correct agents in production.

If your agents aren't monitored like real systems, they're already decaying. Agentic AI only works when reliability is engineered, not assumed.`,
  },
  {
    videoId: 'b8V1nY5cd3k',
    script: `ClipFoundry turns scripts into fully produced videos automatically. Upload a script, a model, and a background, and ClipFoundry generates a polished multiclip video for you. No editing, no production overhead, just ideas brought to life.`,
  },
]

export default function Demo() {
  const [idx, setIdx] = useState(0)
  const slide = SLIDES[idx]

  return (
    <section className="bg-lt-dark py-24 px-4">
      <div className="max-w-6xl mx-auto">
        <p className="text-sm font-mono font-semibold uppercase tracking-[0.15em] text-lt-accent mb-12 text-center">
          See It In Action
        </p>

        {/* Carousel wrapper with arrows */}
        <div className="flex items-center gap-4">
          {/* Left arrow */}
          <button
            onClick={() => setIdx((i) => i - 1)}
            disabled={idx === 0}
            aria-label="Previous"
            className="shrink-0 w-10 h-10 rounded-full border border-white/20 flex items-center justify-center text-lt-text/60 hover:text-lt-text hover:border-white/50 transition-colors disabled:opacity-20 disabled:cursor-not-allowed"
          >
            ←
          </button>

          {/* Side-by-side panels */}
          <div className="flex-1 grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Script panel */}
            <div className="bg-lt-surface rounded-2xl p-8 overflow-y-auto max-h-72 lg:max-h-none">
              <p className="text-xs font-mono uppercase tracking-widest text-lt-accent/60 mb-4">The Script</p>
              <p className="font-mono text-sm text-lt-text/70 leading-relaxed whitespace-pre-wrap">{slide.script}</p>
            </div>

            {/* Video panel */}
            <div className="flex flex-col">
              <p className="text-xs font-mono uppercase tracking-widest text-lt-accent/60 mb-4">The Result</p>
              <div className="aspect-video w-full rounded-2xl overflow-hidden">
                <iframe
                  src={`https://www.youtube.com/embed/${slide.videoId}`}
                  title="ClipFoundry demo video"
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                  allowFullScreen
                  className="w-full h-full"
                />
              </div>
            </div>
          </div>

          {/* Right arrow */}
          <button
            onClick={() => setIdx((i) => i + 1)}
            disabled={idx === SLIDES.length - 1}
            aria-label="Next"
            className="shrink-0 w-10 h-10 rounded-full border border-white/20 flex items-center justify-center text-lt-text/60 hover:text-lt-text hover:border-white/50 transition-colors disabled:opacity-20 disabled:cursor-not-allowed"
          >
            →
          </button>
        </div>

        {/* Dot indicators */}
        <div className="flex justify-center gap-2 mt-6">
          {SLIDES.map((_, i) => (
            <button
              key={i}
              onClick={() => setIdx(i)}
              aria-label={`Slide ${i + 1}`}
              className={`w-2 h-2 rounded-full transition-colors ${i === idx ? 'bg-lt-accent' : 'bg-white/20'}`}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
