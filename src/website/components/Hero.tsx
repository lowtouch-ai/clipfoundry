export default function Hero() {
  return (
    <section className="bg-lt-dark pt-8 pb-20 px-6 text-center">
      <a href="/" className="inline-block text-3xl font-bold text-white mb-2">
        ClipFoundry<span className="text-lt-accent">.ai</span>
      </a>
      <p className="text-sm text-white/50 mb-8">
        Open source agentic AI by lowtouch.ai
      </p>
      <h1 className="font-bold leading-snug tracking-tight text-white"
          style={{ fontSize: 'clamp(1.75rem, 3vw, 2.75rem)' }}>
        From script to screen.<br />
        Completely automated.
      </h1>
    </section>
  )
}
