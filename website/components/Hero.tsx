export default function Hero() {
  return (
    <section className="bg-lt-dark py-20 px-6 text-center">
      <a href="/" className="inline-block text-lg font-bold text-lt-text mb-2">
        ClipFoundry<span className="text-lt-accent">.ai</span>
      </a>
      <p className="text-xs text-lt-text/50 mb-10">
        Open source agentic AI by lowtouch.ai
      </p>
      <h1 className="font-bold leading-snug tracking-tight text-lt-text"
          style={{ fontSize: 'clamp(1.75rem, 3.5vw, 3rem)' }}>
        From script to screen.<br />
        Completely automated.
      </h1>
    </section>
  )
}
