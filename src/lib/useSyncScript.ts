'use client'

import { useCallback, useRef, useState } from 'react'
import type { DbMode } from './scripts'

export type SyncStatus = 'idle' | 'running' | 'done' | 'error'

export function useSyncScript(filename: string, db: DbMode) {
  const [status, setStatus] = useState<SyncStatus>('idle')
  const [lines, setLines] = useState<string[]>([])
  const [duration, setDuration] = useState<number | undefined>()
  const esRef = useRef<EventSource | null>(null)

  const run = useCallback(
    (onStart: () => void, onFinish: () => void) => {
      esRef.current?.close()
      setLines([])
      setDuration(undefined)
      setStatus('running')
      onStart()

      const t0 = Date.now()
      const url = `/api/run?script=${encodeURIComponent(filename)}&db=${db}`
      const es = new EventSource(url)
      esRef.current = es

      es.addEventListener('log', (e) => {
        setLines((prev) => [...prev, JSON.parse(e.data) as string])
      })

      const finish = (s: SyncStatus) => {
        setStatus(s)
        setDuration(Date.now() - t0)
        es.close()
        onFinish()
      }

      es.addEventListener('done', () => finish('done'))
      es.addEventListener('error-event', () => finish('error'))
      es.onerror = () => finish('error')
    },
    [filename, db]
  )

  const cancel = useCallback(() => {
    esRef.current?.close()
    setStatus('idle')
  }, [])

  return { status, lines, duration, run, cancel }
}
