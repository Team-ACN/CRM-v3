'use client'

import { useState } from 'react'
import type { ScriptDef, DbMode } from '@/lib/scripts'
import { useSyncScript } from '@/lib/useSyncScript'
import { OutputPanel } from './OutputPanel'

interface Props {
  script: ScriptDef
  db: DbMode
  isGloballyLocked: boolean
  onStart: (id: string) => void
  onFinish: () => void
}

function splitLabel(label: string): { emoji: string; name: string } {
  const match = label.match(/^(\p{Emoji_Presentation}|\p{Extended_Pictographic})\s+(.+)$/u)
  if (match) return { emoji: match[1], name: match[2] }
  return { emoji: '▶', name: label }
}

export function ScriptButton({ script, db, isGloballyLocked, onStart, onFinish }: Props) {
  const [showOutput, setShowOutput] = useState(false)
  const { status, lines, duration, run } = useSyncScript(script.filename, db)

  const isRunning = status === 'running'
  const isDone    = status === 'done'
  const isError   = status === 'error'
  const disabled  = isGloballyLocked || isRunning
  const { emoji, name } = splitLabel(script.label)

  const handleClick = () => {
    setShowOutput(true)
    run(() => onStart(script.id), () => onFinish())
  }

  const borderColor = isRunning
    ? 'rgba(99,155,255,0.5)'
    : isDone   ? 'rgba(16,185,129,0.3)'
    : isError  ? 'rgba(239,68,68,0.3)'
    : 'rgba(255,255,255,0.07)'

  return (
    <div>
      <button
        onClick={handleClick}
        disabled={disabled}
        style={{
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          gap: '0.6rem',
          padding: '0.55rem 0.65rem',
          background: isRunning ? 'rgba(59,130,246,0.07)' : 'rgba(255,255,255,0.025)',
          border: `1px solid ${borderColor}`,
          borderRadius: '8px',
          cursor: disabled ? 'not-allowed' : 'pointer',
          opacity: isGloballyLocked && !isRunning ? 0.3 : 1,
          transition: 'background 0.15s, border-color 0.15s, transform 0.12s, box-shadow 0.15s',
          boxShadow: isRunning ? '0 0 12px rgba(59,130,246,0.12)' : 'none',
          textAlign: 'left',
          position: 'relative',
        }}
        onMouseEnter={(e) => {
          if (!disabled) {
            const el = e.currentTarget
            el.style.background = 'rgba(255,255,255,0.055)'
            el.style.borderColor = 'rgba(255,255,255,0.15)'
            el.style.transform = 'translateY(-1px)'
            el.style.boxShadow = '0 6px 20px rgba(0,0,0,0.35)'
          }
        }}
        onMouseLeave={(e) => {
          if (!disabled) {
            const el = e.currentTarget
            el.style.background = isRunning ? 'rgba(59,130,246,0.07)' : 'rgba(255,255,255,0.025)'
            el.style.borderColor = borderColor
            el.style.transform = 'none'
            el.style.boxShadow = isRunning ? '0 0 12px rgba(59,130,246,0.12)' : 'none'
          }
        }}
      >
        {/* emoji badge */}
        <span
          style={{
            fontSize: '1.15rem',
            lineHeight: 1,
            width: '28px',
            height: '28px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'rgba(255,255,255,0.05)',
            borderRadius: '6px',
            flexShrink: 0,
          }}
        >
          {emoji}
        </span>

        {/* text */}
        <span style={{ flex: 1, minWidth: 0 }}>
          <span
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '5px',
              fontSize: '0.8rem',
              fontWeight: 600,
              color: isRunning ? '#93c5fd' : 'var(--text)',
              letterSpacing: '-0.01em',
              lineHeight: 1.2,
              marginBottom: '2px',
            }}
          >
            {isRunning && (
              <span
                style={{
                  width: '7px', height: '7px', borderRadius: '50%',
                  border: '1.5px solid #93c5fd', borderTopColor: 'transparent',
                  display: 'inline-block', flexShrink: 0,
                  animation: 'btn-spin 0.7s linear infinite',
                }}
              />
            )}
            {name}
          </span>
          <span
            style={{
              fontSize: '0.67rem',
              color: 'var(--text-3)',
              lineHeight: 1.2,
              display: 'block',
              overflow: 'hidden',
              whiteSpace: 'nowrap',
              textOverflow: 'ellipsis',
            }}
          >
            {script.caption}
          </span>
        </span>

        {/* status dot */}
        {(isDone || isError) && (
          <span
            style={{
              width: '6px', height: '6px', borderRadius: '50%', flexShrink: 0,
              background: isDone ? '#10b981' : '#ef4444',
              boxShadow: `0 0 6px ${isDone ? '#10b981' : '#ef4444'}`,
            }}
          />
        )}
      </button>

      {showOutput && (
        <OutputPanel
          lines={lines}
          status={status}
          duration={duration}
          onClose={() => setShowOutput(false)}
        />
      )}

      <style>{`
        @keyframes btn-spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  )
}
