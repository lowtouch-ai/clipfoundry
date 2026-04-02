import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import Navbar from '@/components/Navbar'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  metadataBase: new URL(process.env.NEXT_PUBLIC_SITE_URL || 'https://clipfoundry.ai'),
  title: {
    default: 'ClipFoundry.ai — Autonomous Video Production Agent',
    template: '%s | ClipFoundry.ai',
  },
  description:
    'ClipFoundry is the autonomous video production agent for creative studios. From script to screen, completely automated. Send an idea, approve a script, receive a finished reel.',
  openGraph: {
    siteName: 'ClipFoundry.ai',
    type: 'website',
    locale: 'en_US',
  },
  twitter: {
    card: 'summary_large_image',
  },
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <Navbar />
        {children}
      </body>
    </html>
  )
}
