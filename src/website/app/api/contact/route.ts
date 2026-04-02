import { NextRequest, NextResponse } from 'next/server'
import nodemailer from 'nodemailer'

export async function POST(req: NextRequest) {
  const { name, email, company, usecase, notes } = await req.json()

  if (!name || !email) {
    return NextResponse.json({ error: 'Name and email are required.' }, { status: 400 })
  }

  const emailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))
  if (!emailOk) return NextResponse.json({ error: 'Invalid email address.' }, { status: 400 })

  if (!process.env.SMTP_HOST || !process.env.SMTP_USER || !process.env.SMTP_PASS || !process.env.MAIL_TO) {
    console.error('Mail service not configured: missing SMTP_HOST, SMTP_USER, or SMTP_PASS')
    return NextResponse.json({ error: 'Service temporarily unavailable. Please try again later.' }, { status: 503 })
  }

  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: Number(process.env.SMTP_PORT) || 587,
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  })

  try {
    await transporter.sendMail({
      from: `"ClipFoundry Early Access" <${process.env.SMTP_USER}>`,
      to: process.env.MAIL_TO,
      subject: `ClipFoundry Early Access Request — ${name}`,
      text:
        `Name:      ${name}\n` +
        `Email:     ${email}\n` +
        `Company:   ${company || '—'}\n` +
        `Use case:  ${usecase}\n` +
        `Notes:     ${notes || '—'}`,
    })
    return NextResponse.json({ success: true })
  } catch (err) {
    console.error('sendMail failed:', err)
    return NextResponse.json({ error: 'Failed to send. Please try again later.' }, { status: 500 })
  }
}
