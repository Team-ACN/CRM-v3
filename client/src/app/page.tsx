'use client'

import dynamic from 'next/dynamic'

const Dashboard = dynamic(() => import('@/components/Dashboard'), { ssr: false })

export default function Home() {
  return (
    <main
      style={{
        minHeight: '100vh',
        padding: '1.25rem 1.5rem',
        background: 'var(--bg)',
      }}
    >
      <div style={{ maxWidth: '1100px', margin: '0 auto' }}>
        <Dashboard />
      </div>
    </main>
  )
}
