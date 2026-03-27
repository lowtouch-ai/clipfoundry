export default function Navbar() {
  return (
    <header className="fixed top-0 left-0 right-0 z-50 bg-lt-dark border-b border-white/[0.06]">
      <nav className="h-14 flex items-center justify-center">
        <a href="/" className="opacity-100 hover:opacity-80 transition-opacity">
          <span className="text-lg font-bold text-lt-text">
            ClipFoundry<span className="text-lt-accent">.ai</span>
          </span>
        </a>
      </nav>
    </header>
  )
}
