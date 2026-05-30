import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'ACN Command Center',
  description: 'Firestore → Google Sheets sync pipeline',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
