export default function Navbar() {
  return (
    <header className="fixed top-0 left-0 right-0 z-50 bg-lt-dark/80 backdrop-blur-md border-b border-white/[0.06]">
      <nav className="h-24 flex items-center justify-center px-8 max-w-6xl mx-auto">
        <a href="/" className="opacity-100 hover:opacity-80 transition-opacity">
          <span className="text-4xl font-bold text-white">
            ClipFoundry<span className="text-lt-accent">.ai</span>
          </span>
        </a>
      </nav>
    </header>
  )
}
