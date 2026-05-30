'use client'

import { useState } from 'react'
import { Header } from './Header'
import { SyncSection } from './SyncSection'
import { SECTIONS, type DbMode } from '@/lib/scripts'

export default function Dashboard() {
  const [db, setDb] = useState<DbMode>('new')
  const [runningScript, setRunningScript] = useState<string | null>(null)

  const handleToggle = () => setDb((prev) => (prev === 'new' ? 'old' : 'new'))

  return (
    <>
      <Header db={db} onToggle={handleToggle} />

      {SECTIONS.map((section) => (
        <SyncSection
          key={section.title}
          section={section}
          db={db}
          runningScript={runningScript}
          onStart={setRunningScript}
          onFinish={() => setRunningScript(null)}
        />
      ))}

      <footer
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '1rem',
          marginTop: '1rem',
          paddingTop: '1rem',
          borderTop: '1px solid rgba(255,255,255,0.04)',
          fontSize: '0.68rem',
          color: 'var(--text-3)',
          letterSpacing: '0.03em',
        }}
      >
        <span>ACN Command Center</span>
        <span style={{ opacity: 0.3 }}>·</span>
        <span>Next.js</span>
        <span style={{ opacity: 0.3 }}>·</span>
        <span
          style={{
            padding: '2px 8px',
            borderRadius: '99px',
            background: db === 'new' ? 'rgba(16,185,129,0.12)' : 'rgba(245,158,11,0.12)',
            border: `1px solid ${db === 'new' ? 'rgba(16,185,129,0.25)' : 'rgba(245,158,11,0.25)'}`,
            color: db === 'new' ? '#6ee7b7' : '#fcd34d',
          }}
        >
          {db === 'new' ? '● NEW DB' : '◆ OLD DB'}
        </span>
      </footer>
    </>
  )
}
