export default function NotFound() {
  return (
    <div className="min-h-screen bg-lt-dark flex items-center justify-center text-center px-4">
      <div>
        <p className="text-lt-green font-mono text-sm font-semibold uppercase tracking-widest mb-4">404</p>
        <h1 className="text-5xl font-bold text-lt-text mb-4">Page Not Found</h1>
        <p className="text-lt-text/50 mb-8">The page you&apos;re looking for doesn&apos;t exist.</p>
        <a href="/" className="bg-lt-accent hover:bg-lt-accent/85 text-white font-semibold px-6 py-3 rounded transition-colors">
          Go Home
        </a>
      </div>
    </div>
  )
}
