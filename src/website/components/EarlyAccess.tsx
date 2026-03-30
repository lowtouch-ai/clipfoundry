export default function EarlyAccess() {
  return (
    <section id="early-access" className="relative bg-lt-dark py-16 px-4 overflow-hidden">
      {/* Background glow */}
      <div
        className="absolute inset-0 pointer-events-none"
        style={{ background: 'radial-gradient(ellipse 60% 50% at 50% 50%, rgba(240,49,208,0.08), transparent)' }}
      />

      <div className="relative max-w-2xl mx-auto">
        {/* Badge */}
        <div className="flex justify-center mb-6">
          <span className="text-sm font-semibold px-3 py-1 rounded-full border border-lt-accent/30 text-lt-accent/80">
            Early Access
          </span>
        </div>

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
                className="w-full bg-lt-dark border border-white/15 rounded-lg px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
              />
            </div>

            <div>
              <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="email">Work email</label>
              <input
                id="email"
                name="email"
                type="email"
                required
                className="w-full bg-lt-dark border border-white/15 rounded-lg px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
              />
            </div>

            <div>
              <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="company">Company</label>
              <input
                id="company"
                name="company"
                type="text"
                className="w-full bg-lt-dark border border-white/15 rounded-lg px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 placeholder:text-lt-text/30"
              />
            </div>

            <div>
              <label className="block text-base font-medium text-lt-text/70 mb-1.5" htmlFor="usecase">What would you use ClipFoundry for?</label>
              <select
                id="usecase"
                name="usecase"
                className="w-full bg-lt-dark border border-white/15 rounded-lg px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
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
                className="w-full bg-lt-dark border border-white/15 rounded-lg px-4 py-3 text-lt-text text-base focus:outline-none focus:ring-2 focus:ring-lt-accent/40 resize-none placeholder:text-lt-text/30"
              />
            </div>

            <button
              type="submit"
              className="w-full bg-lt-accent hover:bg-lt-pink text-white text-lg font-semibold py-3 rounded-lg transition-all duration-300 mt-2 shadow-[0_0_20px_rgba(240,49,208,0.3)] hover:shadow-[0_0_36px_rgba(240,49,208,0.55)]"
            >
              Request early access
            </button>

            <p className="text-xs text-lt-text/40 text-center">We will only contact you with relevant updates.</p>
          </form>
        </div>
      </div>
    </section>
  )
}
