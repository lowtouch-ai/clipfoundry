'use client'

export default function AuroraBackground({ className = '' }: { className?: string }) {
  return (
    <div className={`absolute inset-0 overflow-hidden pointer-events-none ${className}`}>
      <div
        className="absolute inset-[-10%] animate-aurora will-change-transform"
        style={{
          backgroundImage: [
            'repeating-linear-gradient(100deg, rgba(192,96,248,0.45) 0%, rgba(240,49,208,0.3) 6%, transparent 9%, transparent 13%, rgba(192,96,248,0.2) 17%)',
            'repeating-linear-gradient(100deg, rgba(4,18,80,0) 0%, rgba(26,40,120,0.55) 9%, rgba(15,30,102,0.4) 14%, rgba(127,255,0,0.08) 18%, rgba(4,18,80,0) 22%)',
          ].join(', '),
          backgroundSize: '300%, 200%',
          filter: 'blur(12px)',
          opacity: 0.55,
          maskImage: 'radial-gradient(ellipse 90% 80% at 50% 40%, black 30%, transparent 85%)',
          WebkitMaskImage: 'radial-gradient(ellipse 90% 80% at 50% 40%, black 30%, transparent 85%)',
        }}
      />
    </div>
  )
}
