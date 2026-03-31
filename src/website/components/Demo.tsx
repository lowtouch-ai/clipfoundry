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

// Aurora background - blue/indigo tones (distinct from Hero's purple/pink)
const AURORA_MASK = 'radial-gradient(ellipse at 80% 0%, black 20%, transparent 90%)'

export default function Demo() {
  const [idx, setIdx] = useState(0)
  const slide = SLIDES[idx]

  return (
    <section className="relative bg-lt-surface py-16 px-4 overflow-hidden">

      {/* Aurora - blue/indigo tone, different from hero */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div
          className="absolute inset-[-10%] animate-aurora will-change-transform"
          style={{
            backgroundImage: [
              'repeating-linear-gradient(100deg, rgba(255,255,255,0.9) 0%, rgba(255,255,255,0.9) 7%, transparent 10%, transparent 12%, rgba(255,255,255,0.9) 16%)',
              'repeating-linear-gradient(100deg, #3b82f6 10%, #a5b4fc 15%, #93c5fd 20%, #ddd6fe 25%, #60a5fa 30%)',
            ].join(', '),
            backgroundSize: '300%, 200%',
            filter: 'blur(10px) invert(1)',
            opacity: 0.3,
            mixBlendMode: 'difference',
            maskImage: AURORA_MASK,
            WebkitMaskImage: AURORA_MASK,
          }}
        />
      </div>

      <div className="relative max-w-6xl mx-auto">
        <div className="flex flex-col items-center mb-8">
          <p className="text-base font-mono font-semibold uppercase tracking-[0.15em] text-lt-accent">
            See It In Action
          </p>
        </div>

        <div className="flex items-center gap-4">
          <button
            onClick={() => setIdx((i) => i - 1)}
            disabled={idx === 0}
            aria-label="Previous"
            className="shrink-0 w-10 h-10 rounded-full border border-white/20 flex items-center justify-center text-lt-text/60 hover:text-lt-text hover:border-white/50 transition-colors disabled:opacity-20 disabled:cursor-not-allowed"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M10 12L6 8l4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>

          <div className="flex-1 grid grid-cols-1 lg:grid-cols-2 gap-5 lg:h-[480px]">
            <div className="bg-lt-dark border border-white/10 rounded-3xl p-8 overflow-y-auto max-h-72 lg:max-h-full">
              <p className="text-sm font-mono uppercase tracking-widest text-lt-accent/70 mb-4">The Script</p>
              <p className="font-mono text-lg text-lt-text/70 leading-relaxed whitespace-pre-wrap">{slide.script}</p>
            </div>

            <div className="bg-lt-dark border border-white/10 rounded-3xl p-8 flex flex-col h-full min-h-0">
              <p className="text-sm font-mono uppercase tracking-widest text-lt-accent/70 mb-4">The Result</p>
              <div className="flex-1 rounded-2xl overflow-hidden min-h-0">
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

          <button
            onClick={() => setIdx((i) => i + 1)}
            disabled={idx === SLIDES.length - 1}
            aria-label="Next"
            className="shrink-0 w-10 h-10 rounded-full border border-white/20 flex items-center justify-center text-lt-text/60 hover:text-lt-text hover:border-white/50 transition-colors disabled:opacity-20 disabled:cursor-not-allowed"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
          </button>
        </div>

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
      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-b from-transparent to-lt-dark pointer-events-none" />
    </section>
  )
}
