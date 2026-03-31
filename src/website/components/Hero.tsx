import SvgBeams from './SvgBeams'
import FloatingParticles from './FloatingParticles'

const DOT_GRID_MASKS = [
  'radial-gradient(ellipse 72% 78% at 50% 50%, black 30%, transparent 70%)',
  'radial-gradient(ellipse 38% 55% at 15% 25%, black 20%, transparent 65%)',
  'radial-gradient(ellipse 55% 35% at 85% 15%, black 20%, transparent 60%)',
  'radial-gradient(ellipse 25% 30% at 70% 80%, black 15%, transparent 55%)',
  'radial-gradient(ellipse 30% 25% at 25% 75%, black 15%, transparent 55%)',
].join(', ')

export default function Hero() {
  return (
    <section id="top" className="relative bg-lt-dark pt-28 pb-24 px-6 text-center overflow-hidden">

      {/* Animated SVG beams */}
      <SvgBeams />

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

      {/* Floating particles */}
      <FloatingParticles count={35} />

      {/* Dot grid overlay */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{
          backgroundImage: 'radial-gradient(circle, rgba(255,255,255,0.15) 1px, transparent 1px)',
          backgroundSize: '24px 24px',
          maskImage: DOT_GRID_MASKS,
          WebkitMaskImage: DOT_GRID_MASKS,
          maskComposite: 'add',
          WebkitMaskComposite: 'source-over',
        }}
      />

      <div className="relative max-w-5xl mx-auto">
        <span className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1 rounded-full border border-lt-accent/30 text-lt-accent/80 mb-6">
          <span className="w-1.5 h-1.5 rounded-full bg-lt-accent animate-pulse" />
          Now in early access
        </span>

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

        <a
          href="#early-access"
          className="inline-flex items-center gap-2 bg-lt-accent hover:bg-lt-pink text-white text-lg font-semibold px-8 py-3.5 rounded-full transition-all duration-300 shadow-[0_0_20px_rgba(240,49,208,0.35)] hover:shadow-[0_0_40px_rgba(240,49,208,0.60)]"
        >
          Get early access <span aria-hidden>→</span>
        </a>
      </div>

      <div className="absolute bottom-0 left-0 right-0 h-20 bg-gradient-to-b from-transparent to-lt-surface pointer-events-none" />
    </section>
  )
}
