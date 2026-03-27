import type { Metadata } from 'next'
import Hero from '@/components/Hero'
import Demo from '@/components/Demo'
import EarlyAccess from '@/components/EarlyAccess'
import Footer from '@/components/Footer'

export const metadata: Metadata = {
  title: 'ClipFoundry.ai — Autonomous Video Production Agent',
  description:
    'From script to screen, completely automated. ClipFoundry is the open source agentic AI video production platform by lowtouch.ai.',
  openGraph: {
    type: 'website',
    siteName: 'ClipFoundry.ai',
    locale: 'en_US',
    title: 'ClipFoundry.ai — Autonomous Video Production Agent',
    description: 'From script to screen, completely automated.',
    url: '/',
  },
  alternates: { canonical: '/' },
}

export default function HomePage() {
  return (
    <>
      <main>
        <Hero />
        <Demo />
        <EarlyAccess />
      </main>
      <Footer />
    </>
  )
}
