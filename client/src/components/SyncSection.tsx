'use client'

import type { SectionDef, DbMode } from '@/lib/scripts'
import { ScriptButton } from './ScriptButton'

interface Props {
  section: SectionDef
  db: DbMode
  runningScript: string | null
  onStart: (id: string) => void
  onFinish: () => void
}

const SECTION_ACCENT: Record<string, { color: string; glow: string; bg: string }> = {
  'Leads & Growth':       { color: '#60a5fa', glow: 'rgba(59,130,246,0.15)',  bg: 'rgba(59,130,246,0.06)'  },
  'Inventory Management': { color: '#a78bfa', glow: 'rgba(139,92,246,0.15)', bg: 'rgba(139,92,246,0.06)' },
  'System & Data':        { color: '#22d3ee', glow: 'rgba(6,182,212,0.15)',  bg: 'rgba(6,182,212,0.06)'  },
}

export function SyncSection({ section, db, runningScript, onStart, onFinish }: Props) {
  const accent = SECTION_ACCENT[section.title] ?? SECTION_ACCENT['Leads & Growth']

  return (
    <div
      style={{
        background: 'rgba(10,13,23,0.7)',
        border: `1px solid ${accent.color}22`,
        borderTop: `1px solid ${accent.color}44`,
        borderRadius: '12px',
        padding: '1rem 1rem 1rem',
        marginBottom: '0.75rem',
        backdropFilter: 'blur(8px)',
      }}
    >
      {/* section header */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          marginBottom: '0.75rem',
        }}
      >
        <span
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: '22px', height: '22px',
            borderRadius: '5px',
            background: accent.bg,
            border: `1px solid ${accent.color}33`,
            fontSize: '0.75rem',
          }}
        >
          {section.icon}
        </span>
        <span
          style={{
            fontSize: '0.65rem',
            fontWeight: 700,
            letterSpacing: '1.8px',
            textTransform: 'uppercase',
            color: accent.color,
          }}
        >
          {section.title}
        </span>
        <div
          style={{
            flex: 1,
            height: '1px',
            background: `linear-gradient(90deg, ${accent.color}33, transparent)`,
          }}
        />
      </div>

      {/* grid */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${section.cols}, 1fr)`,
          gap: '0.5rem',
        }}
      >
        {section.scripts.map((script) => (
          <ScriptButton
            key={script.id}
            script={script}
            db={db}
            isGloballyLocked={runningScript !== null && runningScript !== script.id}
            onStart={onStart}
            onFinish={onFinish}
          />
        ))}
      </div>
    </div>
  )
}
