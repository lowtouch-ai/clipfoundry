import { ImageResponse } from 'next/og'

export const runtime = 'edge'
export const alt = 'ClipFoundry.ai — Autonomous Video Production Agent'
export const size = { width: 1200, height: 630 }
export const contentType = 'image/png'

export default function Image() {
  return new ImageResponse(
    (
      <div
        style={{
          background: '#0d0d14',
          width: '100%',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          fontFamily: 'sans-serif',
          padding: '60px',
        }}
      >
        <div style={{ fontSize: 72, fontWeight: 700, color: '#ffffff', marginBottom: 20, letterSpacing: '-2px' }}>
          ClipFoundry.ai
        </div>
        <div style={{ fontSize: 30, color: 'rgba(255,255,255,0.55)', textAlign: 'center', maxWidth: 800, lineHeight: 1.4 }}>
          Autonomous Video Production Agent
        </div>
        <div style={{ fontSize: 22, color: '#f031d0', marginTop: 40, letterSpacing: '0.05em' }}>
          From script to screen. Completely automated.
        </div>
      </div>
    ),
    { ...size }
  )
}
