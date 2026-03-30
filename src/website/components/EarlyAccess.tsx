export default function EarlyAccess() {
  return (
    <section className="bg-lt-dark py-24 px-4">
      <div className="max-w-2xl mx-auto">
        <h2 className="text-3xl sm:text-4xl font-bold text-lt-text mb-10 leading-tight">
          Join the early access list and help us shape the future of agentic video creation.
        </h2>

        <form action="mailto:info@lowtouch.ai" method="post" encType="text/plain" className="space-y-5">
          <div>
            <label className="block text-sm font-medium text-lt-text/70 mb-1.5" htmlFor="name">Full name</label>
            <input
              id="name"
              name="name"
              type="text"
              required
              className="w-full bg-lt-surface border border-white/10 rounded-lg px-4 py-3 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-lt-text/70 mb-1.5" htmlFor="email">Work email</label>
            <input
              id="email"
              name="email"
              type="email"
              required
              className="w-full bg-lt-surface border border-white/10 rounded-lg px-4 py-3 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-lt-text/70 mb-1.5" htmlFor="company">Company</label>
            <input
              id="company"
              name="company"
              type="text"
              className="w-full bg-lt-surface border border-white/10 rounded-lg px-4 py-3 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-lt-text/70 mb-1.5" htmlFor="usecase">What would you use ClipFoundry for?</label>
            <select
              id="usecase"
              name="usecase"
              className="w-full bg-lt-surface border border-white/10 rounded-lg px-4 py-3 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40"
            >
              <option value="marketing">Marketing &amp; Advertising</option>
              <option value="product">Product videos</option>
              <option value="training">Internal training</option>
              <option value="other">Other</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-lt-text/70 mb-1.5" htmlFor="notes">Anything we should know (optional)</label>
            <textarea
              id="notes"
              name="notes"
              rows={4}
              className="w-full bg-lt-surface border border-white/10 rounded-lg px-4 py-3 text-lt-text text-sm focus:outline-none focus:ring-2 focus:ring-lt-accent/40 resize-none"
            />
          </div>

          <button
            type="submit"
            className="w-full bg-violet-500 hover:bg-violet-400 text-white font-semibold py-3 rounded-lg transition-colors"
          >
            Request early access
          </button>

          <p className="text-xs text-lt-text/40 text-center">We will only contact you with relevant updates.</p>
        </form>
      </div>
    </section>
  )
}
