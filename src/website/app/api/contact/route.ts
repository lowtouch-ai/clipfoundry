import { NextRequest, NextResponse } from 'next/server'
import nodemailer from 'nodemailer'

export async function POST(req: NextRequest) {
  const { name, email, company, usecase, notes } = await req.json()

  if (!name || !email) {
    return NextResponse.json({ error: 'Name and email are required.' }, { status: 400 })
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
}
