'use client'

import type { DbMode } from '@/lib/scripts'

interface Props {
  db: DbMode
  onToggle: () => void
}

export function Header({ db, onToggle }: Props) {
  return (
    <header
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0.9rem 1.25rem',
        marginBottom: '1rem',
        background: 'rgba(10,13,24,0.85)',
        border: '1px solid rgba(255,255,255,0.07)',
        borderRadius: '12px',
        backdropFilter: 'blur(16px)',
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      {/* top gradient line */}
      <div
        style={{
          position: 'absolute', top: 0, left: 0, right: 0, height: '2px',
          background: 'linear-gradient(90deg, #3b82f6 0%, #8b5cf6 40%, #06b6d4 70%, transparent 100%)',
        }}
      />

      {/* left: title */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <span style={{ fontSize: '1.1rem' }}>⚡</span>
          <h1
            style={{
              fontSize: '1.15rem',
              fontWeight: 700,
              letterSpacing: '-0.02em',
              color: '#e2e8f0',
            }}
          >
            ACN Command Center
          </h1>
        </div>
        <p style={{ fontSize: '0.72rem', color: 'var(--text-3)', marginTop: '1px', paddingLeft: '1.6rem' }}>
          Firestore → Google Sheets sync pipeline
        </p>
      </div>

      {/* right: DB picker */}
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '4px' }}>
        <span style={{ fontSize: '0.6rem', color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '1.2px' }}>
          Data Source
        </span>
        <div
          style={{
            display: 'flex',
            background: 'rgba(0,0,0,0.35)',
            border: '1px solid rgba(255,255,255,0.07)',
            borderRadius: '7px',
            padding: '2px',
            gap: '2px',
          }}
        >
          {(['new', 'old'] as DbMode[]).map((mode) => {
            const active = db === mode
            const isNew = mode === 'new'
            return (
              <button
                key={mode}
                onClick={() => { if (!active) onToggle() }}
                style={{
                  padding: '0.28rem 0.8rem',
                  borderRadius: '5px',
                  fontSize: '0.72rem',
                  fontWeight: active ? 600 : 400,
                  border: 'none',
                  cursor: active ? 'default' : 'pointer',
                  transition: 'all 0.2s',
                  background: active
                    ? isNew ? 'linear-gradient(135deg,#059669,#10b981)' : 'linear-gradient(135deg,#b45309,#d97706)'
                    : 'transparent',
                  color: active ? '#fff' : 'var(--text-3)',
                  boxShadow: active
                    ? isNew ? '0 0 10px rgba(16,185,129,0.35)' : '0 0 10px rgba(245,158,11,0.35)'
                    : 'none',
                }}
              >
                {isNew ? '● NEW' : '◆ OLD'}
              </button>
            )
          })}
        </div>
      </div>
    </header>
  )
}
