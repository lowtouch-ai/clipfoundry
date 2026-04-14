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

  const port = Number(process.env.SMTP_PORT) || 587
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port,
    secure: port === 465,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  })

  const text =
    `Name:      ${name}\n` +
    `Email:     ${email}\n` +
    `Company:   ${company || '—'}\n` +
    `Use case:  ${usecase}\n` +
    `Notes:     ${notes || '—'}`

  const html = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8" /></head>
<body style="margin:0;padding:0;background:#ffffff;font-family:Arial,Helvetica,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;">

    <!-- Header -->
    <tr>
      <td style="background:#041250;padding:28px 32px;">
        <h1 style="margin:0;color:#ffffff;font-size:20px;font-weight:700;">New Early Access Request</h1>
        <p style="margin:6px 0 0;font-size:13px;"><a href="https://clipfoundry.ai" style="color:#4ade80;text-decoration:none;">via clipfoundry.ai</a></p>
      </td>
    </tr>

    <!-- Body -->
    <tr>
      <td style="padding:28px 32px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
          <tr>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#6b7280;font-size:13px;width:100px;vertical-align:top;">Name</td>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#0a0d30;font-size:14px;font-weight:600;">${name}</td>
          </tr>
          <tr>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#6b7280;font-size:13px;vertical-align:top;">Email</td>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#0a0d30;font-size:14px;"><a href="mailto:${email}" style="color:#f031d0;text-decoration:none;">${email}</a></td>
          </tr>
          <tr>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#6b7280;font-size:13px;vertical-align:top;">Company</td>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#0a0d30;font-size:14px;">${company || '—'}</td>
          </tr>
          <tr>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#6b7280;font-size:13px;vertical-align:top;">Use case</td>
            <td style="padding:14px 0;border-bottom:1px solid #eee;color:#0a0d30;font-size:14px;">
              <span style="display:inline-block;background:#f0e8ff;color:#041250;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600;">${usecase || '—'}</span>
            </td>
          </tr>
          <tr>
            <td style="padding:14px 0;color:#6b7280;font-size:13px;vertical-align:top;">Notes</td>
            <td style="padding:14px 0;color:#0a0d30;font-size:14px;line-height:1.6;white-space:pre-wrap;">${notes || '—'}</td>
          </tr>
        </table>

        <!-- Reply button -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:24px;">
          <tr><td align="center">
            <a href="mailto:${email}?subject=Re: ClipFoundry Early Access" style="display:inline-block;background:#f031d0;color:#ffffff;padding:12px 32px;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none;">Reply to ${name.split(' ')[0]}</a>
          </td></tr>
        </table>
      </td>
    </tr>

    <!-- Footer -->
    <tr>
      <td style="background:#f9fafb;padding:16px 32px;border-top:1px solid #eee;">
        <p style="margin:0;color:#9ca3af;font-size:11px;text-align:center;">This email was sent automatically from the ClipFoundry early access form.</p>
      </td>
    </tr>

  </table>
</body>
</html>`

  try {
    await transporter.sendMail({
      from: `"ClipFoundry Early Access" <${process.env.FROMMAIL || process.env.SMTP_USER}>`,
      to: process.env.MAIL_TO,
      subject: `ClipFoundry Early Access Request — ${name}`,
      text,
      html,
    })
    return NextResponse.json({ success: true })
  } catch (err) {
    console.error('sendMail failed:', err)
    return NextResponse.json({ error: 'Failed to send. Please try again later.' }, { status: 500 })
  }
}
