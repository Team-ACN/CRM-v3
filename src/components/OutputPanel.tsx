'use client'

import { useEffect, useRef } from 'react'
import type { SyncStatus } from '@/lib/useSyncScript'

interface Props {
  lines: string[]
  status: SyncStatus
  duration?: number
  onClose: () => void
}

function colorLine(line: string): string {
  const l = line.toLowerCase()
  if (l.includes('[err]') || l.includes('error') || l.includes('failed') || l.includes('exception') || l.includes('traceback'))
    return 'log-error'
  if (l.includes('✅') || l.includes('success') || l.includes('complete') || l.includes('written') || l.includes('done'))
    return 'log-success'
  if (l.includes('⚠') || l.includes('warn') || l.includes('missing'))
    return 'log-warn'
  if (l.includes('🚀') || l.includes('📦') || l.includes('⚡') || l.includes('📥') || l.includes('📤') || l.includes('phase') || l.includes('fetching') || l.includes('processing') || l.includes('writing'))
    return 'log-info'
  return 'log-dim'
}

export function OutputPanel({ lines, status, duration, onClose }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lines])

  const durationStr = duration !== undefined ? `${(duration / 1000).toFixed(1)}s` : ''

  const statusInfo =
    status === 'running'
      ? { label: 'Running', color: '#60a5fa', pulse: true }
      : status === 'done'
      ? { label: `Done · ${durationStr}`, color: '#34d399', pulse: false }
      : { label: `Error · ${durationStr}`, color: '#f87171', pulse: false }

  return (
    <div
      style={{
        marginTop: '0.5rem',
        borderRadius: '10px',
        border: '1px solid rgba(255,255,255,0.07)',
        background: 'rgba(4,6,15,0.95)',
        overflow: 'hidden',
      }}
    >
      {/* toolbar */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '0.45rem 0.75rem',
          borderBottom: '1px solid rgba(255,255,255,0.05)',
          background: 'rgba(255,255,255,0.02)',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          {/* traffic-light dots */}
          <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#ef4444', display: 'inline-block', opacity: 0.6 }} />
          <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#f59e0b', display: 'inline-block', opacity: 0.6 }} />
          <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#22c55e', display: 'inline-block', opacity: 0.6 }} />
          <span
            style={{
              marginLeft: '0.4rem',
              fontSize: '0.65rem',
              fontFamily: 'monospace',
              color: 'rgba(255,255,255,0.2)',
            }}
          >
            output
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '5px',
              fontSize: '0.68rem',
              color: statusInfo.color,
            }}
          >
            <span
              style={{
                width: '6px',
                height: '6px',
                borderRadius: '50%',
                background: statusInfo.color,
                boxShadow: `0 0 6px ${statusInfo.color}`,
                display: 'inline-block',
                animation: statusInfo.pulse ? 'statusPulse 1.2s ease-in-out infinite' : 'none',
              }}
            />
            {statusInfo.label}
          </span>

          <button
            onClick={onClose}
            style={{
              background: 'rgba(255,255,255,0.06)',
              border: '1px solid rgba(255,255,255,0.08)',
              borderRadius: '4px',
              color: 'rgba(255,255,255,0.3)',
              fontSize: '0.65rem',
              padding: '2px 7px',
              cursor: 'pointer',
            }}
          >
            ✕
          </button>
        </div>
      </div>

      {/* log body */}
      <div
        style={{
          maxHeight: '280px',
          overflowY: 'auto',
          padding: '0.6rem 0.75rem',
        }}
      >
        {lines.length === 0 && status === 'running' ? (
          <p
            className="mono"
            style={{ fontSize: '0.75rem', color: 'rgba(255,255,255,0.2)' }}
          >
            Initializing…
          </p>
        ) : (
          <div className="mono" style={{ fontSize: '0.75rem' }}>
            {lines.map((line, i) => (
              <div
                key={i}
                className={colorLine(line)}
                style={{ lineHeight: '1.65', wordBreak: 'break-all' }}
              >
                <span style={{ color: 'rgba(255,255,255,0.12)', userSelect: 'none', marginRight: '0.6rem' }}>
                  {String(i + 1).padStart(3, ' ')}
                </span>
                {line}
              </div>
            ))}
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <style>{`
        @keyframes statusPulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.3; }
        }
      `}</style>
    </div>
  )
}
