export default function Hero() {
  return (
    <section id="top" className="relative bg-lt-dark pt-28 pb-24 px-6 text-center overflow-hidden">

      {/* Animated gradient blobs */}
      <div className="absolute inset-0 pointer-events-none overflow-hidden">
        <div
          className="absolute -top-32 -left-32 w-[600px] h-[600px] rounded-full bg-lt-accent/10 blur-[120px]"
          style={{ animation: 'blob 14s ease-in-out infinite' }}
        />
        <div
          className="absolute top-10 -right-32 w-[500px] h-[500px] rounded-full bg-lt-lavender/10 blur-[100px]"
          style={{ animation: 'blob 18s ease-in-out infinite 3s' }}
        />
        <div
          className="absolute -bottom-10 left-1/3 w-[400px] h-[400px] rounded-full bg-lt-surface/60 blur-[80px]"
          style={{ animation: 'blob 11s ease-in-out infinite 6s' }}
        />
      </div>

      {/* Dot grid overlay */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{
          backgroundImage: 'radial-gradient(circle, rgba(255,255,255,0.18) 1px, transparent 1px)',
          backgroundSize: '32px 32px',
          maskImage: 'radial-gradient(ellipse 80% 80% at 50% 50%, black, transparent)',
          WebkitMaskImage: 'radial-gradient(ellipse 80% 80% at 50% 50%, black, transparent)',
        }}
      />

      <div className="relative max-w-5xl mx-auto">
        {/* Badge */}
        <span className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1 rounded-full border border-lt-accent/30 text-lt-accent/80 mb-6">
          <span className="w-1.5 h-1.5 rounded-full bg-lt-accent animate-pulse" />
          Now in early access
        </span>

        {/* Gradient headline */}
        <h1
          className="font-bold tracking-tight mb-5 text-white"
          style={{ fontSize: 'clamp(2.5rem, 6vw, 4.75rem)', lineHeight: 1.05 }}
        >
          From script to screen.<br />
          Completely automated.
        </h1>

        <p className="text-white/55 mb-10 max-w-2xl mx-auto leading-relaxed" style={{ fontSize: 'clamp(18px, 1.6vw, 24px)' }}>
          Send an email or chat message. ClipFoundry writes the script, gets your approval, and delivers a finished video — in minutes.
        </p>

        {/* Glowing CTA */}
        <a
          href="#early-access"
          className="inline-flex items-center gap-2 bg-lt-accent hover:bg-lt-pink text-white text-lg font-semibold px-8 py-3.5 rounded-full transition-all duration-300 shadow-[0_0_20px_rgba(240,49,208,0.35)] hover:shadow-[0_0_40px_rgba(240,49,208,0.60)]"
        >
          Get early access <span aria-hidden>→</span>
        </a>
      </div>

      {/* Section transition to lt-surface */}
      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-b from-transparent to-lt-surface pointer-events-none" />
    </section>
  )
}
